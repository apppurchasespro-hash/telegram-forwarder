"""Index the storage channel's history into the catalog.

Runs on the **Telethon user account** (same session/credentials as the
forwarder) because the Bot API cannot read channel history.

Usage:
    python -m searchbot.indexer                 # incremental (resume from high-water)
    python -m searchbot.indexer --full          # re-scan everything
    python -m searchbot.indexer --channel -100…  # override storage channel

Resumable: the largest indexed message_id is stored in ``meta`` and used as
``min_id`` on the next incremental run.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from telethon import TelegramClient
from telethon.sessions import StringSession

# Reuse the forwarder's credential loading so config stays in one place.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from downloader import load_config  # noqa: E402

from .db import Catalog  # noqa: E402
from .parse import parse  # noqa: E402

BASE_DIR = Path(__file__).resolve().parent.parent
HIGH_WATER_KEY = "indexer_high_water"


def load_bot_config() -> dict:
    path = BASE_DIR / "searchbot" / "config.json"
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found — copy searchbot/config.example.json to it and fill it in."
        )
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _build_client(api_id: int, api_hash: str) -> TelegramClient:
    session_string = os.environ.get("TELETHON_SESSION_STRING")
    if session_string:
        session = StringSession(session_string)
    else:
        session_name = os.environ.get("TELETHON_SESSION_FILE", "tg_session")
        session = str(BASE_DIR / session_name)
    return TelegramClient(session, api_id, api_hash)


def _extract(msg) -> dict | None:
    """Return catalog fields for a media message, or None to skip."""
    doc = getattr(msg, "document", None)
    if doc is None and not getattr(msg, "video", None):
        return None  # text-only / photo-only posts aren't servable files here

    file_name = None
    size = None
    mime = None
    if msg.file:
        file_name = msg.file.name
        size = msg.file.size
        mime = msg.file.mime_type

    meta = parse(file_name, msg.message)
    return {
        "message_id": msg.id,
        "title": meta.title,
        "year": meta.year,
        "quality": meta.quality,
        "language": meta.language,
        "size": size,
        "file_name": file_name,
        "caption": msg.message or None,
        "mime": mime,
    }


async def run(full: bool, channel_override: int | None) -> None:
    cfg = load_config()
    bot_cfg = load_bot_config()
    channel = channel_override or bot_cfg["storage_channel_id"]
    catalog = Catalog(bot_cfg.get("db_path", str(BASE_DIR / "searchbot" / "catalog.db")))

    min_id = 0
    if not full:
        hw = catalog.get_meta(HIGH_WATER_KEY)
        min_id = int(hw) if hw else 0

    client = _build_client(int(cfg["api_id"]), cfg["api_hash"])
    await client.connect()
    if not await client.is_user_authorized():
        raise RuntimeError(
            "Telethon session is not authorized. Set TELETHON_SESSION_FILE/"
            "TELETHON_SESSION_STRING or log in the session used by the forwarder."
        )
    me = await client.get_me()
    print(f"[indexer] signed in as {me.username or me.id}; channel={channel} min_id={min_id}")

    entity = await client.get_entity(channel)
    indexed = 0
    skipped = 0
    high_water = min_id
    # reverse=True → oldest→newest, so high_water grows monotonically and a
    # crash mid-run resumes cleanly.
    async for msg in client.iter_messages(entity, reverse=True, min_id=min_id):
        high_water = max(high_water, msg.id)
        fields = _extract(msg)
        if fields is None:
            skipped += 1
            continue
        catalog.upsert_file(**fields)
        indexed += 1
        if indexed % 200 == 0:
            catalog.set_meta(HIGH_WATER_KEY, high_water)
            print(f"[indexer] indexed={indexed} skipped={skipped} high_water={high_water}")

    catalog.set_meta(HIGH_WATER_KEY, high_water)
    print(f"[indexer] done. indexed={indexed} skipped={skipped} "
          f"total_in_catalog={catalog.file_count()} high_water={high_water}")
    catalog.close()
    await client.disconnect()


def main() -> None:
    ap = argparse.ArgumentParser(description="Index the storage channel into the catalog.")
    ap.add_argument("--full", action="store_true", help="re-scan from the beginning")
    ap.add_argument("--channel", type=int, default=None, help="override storage channel id")
    args = ap.parse_args()
    asyncio.run(run(full=args.full, channel_override=args.channel))


if __name__ == "__main__":
    main()

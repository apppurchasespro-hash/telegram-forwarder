"""Backfill a Telegram supergroup's full history into the Lovable bot's index.

Reuses the @dhoarder StringSession (secrets/acct2_session_string.txt) so no
interactive OTP is needed. That account is already an admin in the source
supergroup, so it can read full history.

POSTs Bot-API-shaped messages in batches to the bot's /api/public/telegram/ingest
endpoint, which upserts into Supabase `movies` table (idempotent on
chat_id+message_id).

Usage:
    py -3.11 scripts/bot_backfill.py --source -1003776591963 --token <ingest_token>
    py -3.11 scripts/bot_backfill.py --source -1003776591963 --token <t> --min-id 50000
    py -3.11 scripts/bot_backfill.py --source -1003776591963 --token <t> --batch 50 --dry-run

Resumable via --min-id; just re-run with the last reported scanned message id.
"""
from __future__ import annotations
import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)

ROOT = Path(__file__).resolve().parent.parent

import httpx
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import (
    Message, MessageMediaDocument, MessageMediaPhoto,
    DocumentAttributeFilename, DocumentAttributeVideo, DocumentAttributeAudio,
)

DEFAULT_INGEST_URL = "https://project--c8d6f36b-9341-4949-9bee-a1561128a316-dev.lovable.app/api/public/telegram/ingest"


def _photo_payload(msg: Message) -> dict | None:
    if not isinstance(msg.media, MessageMediaPhoto) or not msg.photo:
        return None
    p = msg.photo
    return {
        "file_id": f"photo:{p.id}",
        "file_unique_id": str(p.id),
        "file_size": None,
        "width": 0,
        "height": 0,
    }


def _coerce_int(v) -> int | None:
    """Telethon returns float duration on some new-schema video/audio docs;
    Supabase movies.duration is INT, so coerce. Returns None on garbage."""
    if v is None:
        return None
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


def _doc_payload(msg: Message) -> tuple[str | None, dict | None]:
    if not isinstance(msg.media, MessageMediaDocument) or not msg.document:
        return None, None
    d = msg.document
    file_name = None
    duration = None
    file_type = "document"
    for a in d.attributes or []:
        if isinstance(a, DocumentAttributeFilename):
            file_name = a.file_name
        elif isinstance(a, DocumentAttributeVideo):
            file_type = "video"
            duration = a.duration
        elif isinstance(a, DocumentAttributeAudio):
            file_type = "audio"
            duration = a.duration
    payload = {
        "file_id": f"doc:{d.id}",
        "file_unique_id": str(d.id),
        "file_size": d.size,
        "file_name": file_name,
        "mime_type": d.mime_type,
        "duration": _coerce_int(duration),
    }
    return file_type, payload


def to_bot_api(msg: Message, chat_id: int) -> dict[str, Any] | None:
    if not isinstance(msg, Message):
        return None
    caption = msg.message or None
    out: dict[str, Any] = {
        "message_id": msg.id,
        "date": int(msg.date.timestamp()) if msg.date else 0,
        "chat": {"id": chat_id, "type": "supergroup"},
        "caption": caption,
        "text": caption,
    }
    photo = _photo_payload(msg)
    if photo:
        out["photo"] = [photo]
        return out
    file_type, doc = _doc_payload(msg)
    if file_type and doc:
        out[file_type] = doc
        return out
    return None


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, help="chat id, e.g. -1003776591963")
    ap.add_argument("--token", required=True, help="ingest token from bot's /ingesttoken")
    ap.add_argument("--ingest-url", default=DEFAULT_INGEST_URL)
    ap.add_argument("--batch", type=int, default=100)
    ap.add_argument("--min-id", type=int, default=0, help="resume from this message id (exclusive)")
    ap.add_argument("--dry-run", action="store_true", help="parse + count, don't POST")
    ap.add_argument("--concurrency", type=int, default=6, help="in-flight POSTs to ingest endpoint")
    ap.add_argument("--session-string-file", default=str(ROOT / "secrets" / "acct2_session_string.txt"))
    ap.add_argument("--config", default=str(ROOT / "config.json"))
    args = ap.parse_args()

    cfg = json.loads(Path(args.config).read_text())
    sess = Path(args.session_string_file).read_text().strip()
    source_id = int(args.source)

    client = TelegramClient(StringSession(sess), cfg["api_id"], cfg["api_hash"])
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: session string isn't authorized", file=sys.stderr)
        return 1

    me = await client.get_me()
    entity = await client.get_entity(source_id)
    title = getattr(entity, "title", str(entity))
    # Bot API chat_id format for supergroups is -100<internal_id>
    chat_id = int(f"-100{entity.id}") if not str(source_id).startswith("-100") else source_id
    print(f"As: @{me.username} ({me.first_name})  id={me.id}")
    print(f"Source: {title}  (resolved chat_id={chat_id})")
    print(f"Ingest: {args.ingest_url}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print()

    batch: list[dict[str, Any]] = []
    scanned = 0
    posted = 0
    failed = 0
    start = time.time()
    last_report_id = args.min_id
    sem = asyncio.Semaphore(args.concurrency)
    inflight: set[asyncio.Task] = set()
    last_log_at = 0

    async with httpx.AsyncClient(timeout=180, limits=httpx.Limits(max_connections=args.concurrency * 2)) as http:
        async def post_batch(payload_list: list[dict[str, Any]]) -> None:
            nonlocal posted, failed
            if args.dry_run:
                posted += len(payload_list)
                return
            async with sem:
                for attempt in range(5):
                    try:
                        r = await http.post(
                            args.ingest_url,
                            json={"messages": payload_list},
                            headers={"X-Ingest-Token": args.token, "Content-Type": "application/json"},
                        )
                        if r.status_code == 200:
                            data = r.json()
                            posted += data.get("indexed", 0)
                            failed += data.get("failed", 0)
                            if data.get("failed"):
                                print(f"  batch had {data['failed']} failures: {data.get('errors')}")
                            return
                        print(f"  HTTP {r.status_code}: {r.text[:300]}")
                    except Exception as e:
                        print(f"  network error: {e}")
                    wait = 2 ** attempt
                    print(f"  retrying in {wait}s...")
                    await asyncio.sleep(wait)
                print(f"  GAVE UP on batch of {len(payload_list)}")

        def schedule_flush():
            nonlocal batch
            if not batch:
                return
            payload_list = batch
            batch = []
            t = asyncio.create_task(post_batch(payload_list))
            inflight.add(t)
            t.add_done_callback(inflight.discard)

        async for msg in client.iter_messages(entity, reverse=True, min_id=args.min_id):
            scanned += 1
            last_report_id = msg.id
            payload = to_bot_api(msg, chat_id)
            if payload is None:
                continue
            batch.append(payload)
            if len(batch) >= args.batch:
                # Wait for a slot before scheduling — keeps memory bounded
                # and avoids dispatching thousands of tasks at once.
                while len(inflight) >= args.concurrency * 2:
                    done, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                schedule_flush()
                # Log every ~1000 scanned msgs OR every 30s, whichever first
                now = time.time()
                if scanned - last_log_at >= 1000 or now - start - last_log_at / 1000 > 30:
                    elapsed = max(now - start, 0.001)
                    rate = scanned / elapsed
                    print(f"  scanned={scanned:,}  posted={posted:,}  failed={failed:,}  "
                          f"last_id={last_report_id}  inflight={len(inflight)}  "
                          f"rate={rate:.0f}msg/s  elapsed={elapsed/60:.1f}m")
                    last_log_at = scanned
        schedule_flush()
        # Drain
        if inflight:
            print(f"  draining {len(inflight)} in-flight batches...")
            await asyncio.gather(*inflight, return_exceptions=True)

    elapsed = time.time() - start
    print()
    print("=" * 60)
    print(f"DONE in {elapsed/60:.1f} min")
    print(f"  scanned:  {scanned:,}")
    print(f"  posted:   {posted:,}  (indexed in Supabase)")
    print(f"  failed:   {failed:,}")
    print(f"  last_id:  {last_report_id}  (use --min-id {last_report_id} to resume)")
    await client.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

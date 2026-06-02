"""Re-fire 8 actions through @mayur_gdrive_bot via @dhoarder to populate
bot_events + deliveries after the Tier 1 migration.

Actions:
  1. /start
  2. /start campaign_smoke_test   (deep-link param attribution)
  3. /help
  4. /whoami
  5. /notacommand                  (command_unknown)
  6. "inception"                   (search with results)
  7. "zzz_no_such_movie_xyz"       (search zero hits)
  8. "ladies first" -> tap first 📥 button (delivery)
"""
from __future__ import annotations
import asyncio
import json
import sys
from pathlib import Path

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)

ROOT = Path(__file__).resolve().parent.parent

from telethon import TelegramClient
from telethon.sessions import StringSession

BOT_USERNAME = "mayur_gdrive_bot"


async def wait_reply(client, peer, after_id: int, timeout: float = 15):
    end = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < end:
        msgs = await client.get_messages(peer, limit=3)
        for m in msgs:
            if m.id > after_id and not m.out:
                return m
        await asyncio.sleep(0.5)
    return None


async def send_and_wait(client, bot, payload: str, label: str, timeout: float = 12):
    prior = await client.get_messages(bot, limit=1)
    last_id = prior[0].id if prior else 0
    sent = await client.send_message(bot, payload)
    reply = await wait_reply(client, bot, last_id, timeout=timeout)
    if reply is None:
        print(f"  [{label}] sent={payload!r}  TIMEOUT")
        return None
    snippet = (reply.message or "(no text)").splitlines()[0][:120]
    print(f"  [{label}] sent={payload!r}  reply={snippet!r}")
    return reply


async def main() -> int:
    cfg = json.loads((ROOT / "config.json").read_text())
    sess = (ROOT / "secrets" / "acct2_session_string.txt").read_text().strip()
    client = TelegramClient(StringSession(sess), cfg["api_id"], cfg["api_hash"])
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: session not authorized")
        return 1

    me = await client.get_me()
    bot = await client.get_entity(BOT_USERNAME)
    print(f"As: @{me.username} id={me.id}")
    print(f"Bot: @{bot.username} id={bot.id}")
    print()

    print("=" * 60)
    print("8-action smoke")
    print("=" * 60)
    await send_and_wait(client, bot, "/start", "1 /start")
    await asyncio.sleep(0.7)
    await send_and_wait(client, bot, "/start campaign_smoke_test", "2 /start <param>")
    await asyncio.sleep(0.7)
    await send_and_wait(client, bot, "/help", "3 /help")
    await asyncio.sleep(0.7)
    await send_and_wait(client, bot, "/whoami", "4 /whoami")
    await asyncio.sleep(0.7)
    await send_and_wait(client, bot, "/notacommand", "5 /notacommand")
    await asyncio.sleep(0.7)
    await send_and_wait(client, bot, "inception", "6 search hits")
    await asyncio.sleep(0.7)
    await send_and_wait(client, bot, "zzz_no_such_movie_xyz", "7 search miss")
    await asyncio.sleep(0.7)

    # Delivery: search for ladies first, tap 📥
    print()
    print("8 delivery test ('ladies first')")
    prior = await client.get_messages(bot, limit=1)
    last_id = prior[0].id if prior else 0
    await client.send_message(bot, "ladies first")
    search_reply = await wait_reply(client, bot, last_id, timeout=15)
    if not search_reply or not search_reply.buttons:
        print("  no buttons in search reply — delivery test skipped")
    else:
        btn_to_click = None
        for row in search_reply.buttons:
            for btn in row:
                if "📥" in getattr(btn, "text", ""):
                    btn_to_click = btn
                    break
            if btn_to_click:
                break
        if not btn_to_click:
            print("  no 📥 button found")
        else:
            prior = await client.get_messages(bot, limit=1)
            last_id = prior[0].id if prior else 0
            result = await btn_to_click.click()
            print(f"  callback ack: {result}")
            delivered = await wait_reply(client, bot, last_id, timeout=25)
            if delivered is None:
                print("  TIMEOUT waiting for file")
            else:
                kind = "text"
                if delivered.video: kind = f"video ({(delivered.video.size or 0)//1024} KB)"
                elif delivered.document: kind = f"document ({(delivered.document.size or 0)//1024} KB)"
                elif delivered.audio: kind = f"audio ({(delivered.audio.size or 0)//1024} KB)"
                elif delivered.photo: kind = "photo"
                print(f"  DELIVERED msg #{delivered.id}: {kind}")

    print()
    print("done.")
    await client.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

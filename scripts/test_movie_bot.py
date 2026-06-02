"""End-to-end test of the @mayur_gdrive_bot search + delivery flow.

Uses the @dhoarder StringSession (already in secrets/) so no OTP needed.
Sends a few search queries, captures replies, then taps the first 📥 button
on the most useful result and waits for the file delivery to verify the
copyMessage path works.
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

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.custom import Button

BOT_USERNAME = "mayur_gdrive_bot"
QUERIES = [
    "ladies first",
    "2026",
    "1080p",
    "hindi",
]


async def wait_reply(client, peer, after_id: int, timeout: float = 15) -> object | None:
    """Wait until a new message from peer appears (id > after_id)."""
    end = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < end:
        msgs = await client.get_messages(peer, limit=3)
        for m in msgs:
            if m.id > after_id and not m.out:
                return m
        await asyncio.sleep(0.5)
    return None


async def main() -> int:
    cfg = json.loads((ROOT / "config.json").read_text())
    sess = (ROOT / "secrets" / "acct2_session_string.txt").read_text().strip()
    client = TelegramClient(StringSession(sess), cfg["api_id"], cfg["api_hash"])
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: session not authorized")
        return 1

    me = await client.get_me()
    print(f"Acting as: @{me.username} ({me.first_name}) id={me.id}")

    bot = await client.get_entity(BOT_USERNAME)
    print(f"Bot:       @{bot.username} ({bot.first_name}) id={bot.id}")
    print()

    saved_for_delivery: object | None = None

    for q in QUERIES:
        print("=" * 60)
        print(f"QUERY: {q!r}")
        # Note last seen msg id so we can detect the reply
        prior = await client.get_messages(bot, limit=1)
        last_id = prior[0].id if prior else 0
        sent = await client.send_message(bot, q)
        print(f"  sent  (msg #{sent.id})")
        reply = await wait_reply(client, bot, last_id, timeout=12)
        if reply is None:
            print("  TIMEOUT (no reply in 12s)")
            continue
        text = reply.message or "(no text)"
        print(f"  reply (msg #{reply.id}):")
        for line in text.splitlines():
            print(f"    | {line}")
        # Extract inline keyboard if any
        if reply.buttons:
            print(f"  buttons: {sum(len(row) for row in reply.buttons)} total")
            for ri, row in enumerate(reply.buttons):
                for ci, btn in enumerate(row):
                    label = getattr(btn, "text", "?")
                    data = getattr(btn, "data", b"")
                    print(f"    [{ri}][{ci}] '{label}' data={data!r}")
            if saved_for_delivery is None:
                # First time we see download buttons, save the message for delivery test
                # Look for a 📥 button
                for row in reply.buttons:
                    for btn in row:
                        if "📥" in getattr(btn, "text", ""):
                            saved_for_delivery = reply
                            break
                    if saved_for_delivery:
                        break
        await asyncio.sleep(0.5)

    # Delivery test: tap the first 📥 button on saved result
    print()
    print("=" * 60)
    if saved_for_delivery is None:
        print("DELIVERY TEST SKIPPED — no 📥 button seen in any reply.")
    else:
        print(f"DELIVERY TEST — tapping first 📥 button on msg #{saved_for_delivery.id}")
        # Find the button and click it
        btn_to_click = None
        for row in saved_for_delivery.buttons:
            for btn in row:
                if "📥" in getattr(btn, "text", ""):
                    btn_to_click = btn
                    break
            if btn_to_click:
                break
        prior = await client.get_messages(bot, limit=1)
        last_id = prior[0].id if prior else 0
        result = await btn_to_click.click()
        # CallbackQuery answer is in result; we also wait for the delivered file
        print(f"  callback ack: {result}")
        delivered = await wait_reply(client, bot, last_id, timeout=20)
        if delivered is None:
            print("  TIMEOUT waiting for delivered file (20s)")
        else:
            kind = "text"
            if delivered.video: kind = f"video ({(delivered.video.size or 0)//1024} KB)"
            elif delivered.document: kind = f"document ({(delivered.document.size or 0)//1024} KB)"
            elif delivered.audio: kind = f"audio ({(delivered.audio.size or 0)//1024} KB)"
            elif delivered.photo: kind = "photo"
            print(f"  DELIVERED msg #{delivered.id}: {kind}")
            if delivered.message:
                print(f"  caption: {delivered.message[:200]}")

    print()
    print("done.")
    await client.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

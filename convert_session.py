"""
One-shot helper: convert a local SQLite session file into a Telethon
StringSession string suitable for the TELETHON_SESSION_STRING env var.

Usage: python convert_session.py [path/to/tg_session]
(default: ./tg_session)

Reads api_id/api_hash from config.json (or env vars). Prints the string to
stdout — paste it into your Railway service's TELETHON_SESSION_STRING var.

Treat the output like a password: anyone with this string can act as your
Telegram account.
"""

import asyncio
import sys
from pathlib import Path

from telethon import TelegramClient
from telethon.sessions import StringSession

from downloader import load_config


async def main():
    config = load_config()
    session_path = sys.argv[1] if len(sys.argv) > 1 else "tg_session"
    if session_path.endswith(".session"):
        session_path = session_path[: -len(".session")]
    if not Path(session_path + ".session").exists():
        print(f"Session file not found: {session_path}.session", file=sys.stderr)
        sys.exit(1)

    client = TelegramClient(session_path, config["api_id"], config["api_hash"])
    await client.connect()
    if not await client.is_user_authorized():
        print("Session is not authorised. Run a CLI command first to log in.", file=sys.stderr)
        sys.exit(1)

    string = StringSession.save(client.session)
    me = await client.get_me()
    await client.disconnect()

    print(f"# Logged in as: {me.first_name} (@{me.username})", file=sys.stderr)
    print(f"# Paste the line below as TELETHON_SESSION_STRING on Railway.", file=sys.stderr)
    print(f"# Keep it secret — it grants full account access.", file=sys.stderr)
    print(string)


if __name__ == "__main__":
    asyncio.run(main())

# searchbot

A search-and-serve bot that sits on top of a Telegram **storage channel**.
Users type a title; the bot finds matching files in a local catalog and delivers
them with `copy_message` (no re-upload). Built on **aiogram v3** + **SQLite/FTS5**,
and reuses the forwarder's Telethon session for indexing.

## Why two processes

The Bot API **cannot read a channel's message history** — a bot only sees *new*
posts. So:

| Process | Identity | Job |
|---|---|---|
| `indexer.py` | your Telethon **user account** | walk the storage channel's history → build the searchable catalog |
| `bot.py` | a BotFather **bot token** | search the catalog, serve files via `copy_message` |

MTProto `file_id`s aren't reusable by a Bot-API bot, so we store each file's
**`message_id`** and the bot serves by `copy_message(from_chat_id=storage, message_id=…)`.
**The bot must be an admin of the storage channel** for this to work.

## Setup

1. Create a bot with [@BotFather](https://t.me/BotFather), copy the token.
   Run `/setinline` on BotFather to enable inline mode (optional but recommended).
2. Add the bot as an **admin** of your storage channel (`-1003930887639`).
   (Optional) create a public "force-sub" channel and add the bot as admin there too.
3. `cp searchbot/config.example.json searchbot/config.json` and fill in:
   - `bot_token` — from BotFather
   - `storage_channel_id` — where the files live
   - `force_sub_channel` — `@yourchannel` to gate downloads, or `null` to disable
   - `admins` — your numeric user id(s) (for `/stats`, `/broadcast`)
   - `auto_delete_seconds` — `0` to keep files, e.g. `600` to delete after 10 min
4. `pip install -r requirements.txt`
5. The indexer reuses the forwarder's `config.json` (api_id/api_hash) and
   `tg_session` session — no extra Telethon setup needed.

## Run

```bash
# 1) build / refresh the catalog (user account)
python -m searchbot.indexer            # incremental; resumes from last run
python -m searchbot.indexer --full     # full re-scan

# 2) start the bot (bot token)
python -m searchbot.bot
```

Re-run the indexer periodically (cron / scheduled task) to pick up new uploads,
or extend it later to listen for new posts live.

## Commands

- `/start` — greet + register the user.
- *(any text)* — search; results come back as tappable buttons.
- inline: `@yourbot query` from any chat → results deep-link back to the bot.
- `/stats` *(admin)* — file and user counts.
- `/broadcast <msg>` *(admin)* — message every known user.

## Notes / next steps

- **Storage engine is swappable.** Everything goes through `db.Catalog`; port
  that one class to MongoDB when you outgrow SQLite.
- **Legal:** serving copyrighted content violates Telegram's ToS and copyright
  law — bots/tokens in that space get taken down. The code is content-agnostic;
  point it at content you have the rights to.

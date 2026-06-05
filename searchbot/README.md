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

## Deploy (Docker, always-on)

The container runs the **bot only**. Config + catalog are mounted from a
`/data` volume so secrets and your account session are never baked into the
image (`.dockerignore` enforces this). Re-index locally and re-upload
`catalog.db` when you add content.

```bash
# on the server, from the repo root:
mkdir -p searchbot/data
# copy in your two runtime files:
#   searchbot/data/config.json   (use config.docker.example.json -> db_path "/data/catalog.db")
#   searchbot/data/catalog.db    (scp the file from your local machine)

docker compose -f searchbot/docker-compose.yml up -d --build
docker logs -f searchbot          # expect: "bot @… online; catalog has N files"
```

To refresh the catalog after adding files (run locally, where the Telethon
session lives), then push the DB up:

```bash
TELETHON_SESSION_FILE=tg_session python -m searchbot.indexer
scp searchbot/catalog.db  user@server:/path/repo/searchbot/data/catalog.db
ssh user@server 'docker restart searchbot'
```

### On Tencent Cloud

- **Use an international region** (Hong Kong / Singapore / Silicon Valley).
  Mainland-China regions cannot reach `api.telegram.org` — Telegram is blocked.
- A **Lighthouse** instance (or a small **CVM**, 1 vCPU / 1–2 GB) with the
  Docker application image is plenty. Open no inbound ports — the bot uses
  outbound long-polling only (no webhook).
- Install Docker, clone the repo, place the two files in `searchbot/data/`,
  then `docker compose … up -d --build` as above.

## Notes / next steps

- **Storage engine is swappable.** Everything goes through `db.Catalog`; port
  that one class to MongoDB when you outgrow SQLite.
- **Legal:** serving copyrighted content violates Telegram's ToS and copyright
  law — bots/tokens in that space get taken down. The code is content-agnostic;
  point it at content you have the rights to.

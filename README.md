# telegram-forwarder

A small Python CLI that copies messages between Telegram chats, channels, and forum topics — including from channels with "Restrict saving content" enabled (a.k.a. protected chats).

Built on [Telethon](https://github.com/LonamiWebs/Telethon). Runs on your own Telegram account.

## Why

Telegram's built-in forward fails on protected channels with `ChatForwardsRestricted`. This tool detects that automatically and switches to **copy mode**: it downloads each message's media to a temp folder and re-uploads it to the destination, preserving captions, hyperlinks, and other inline formatting (bold/italic/code/links).

## Features

- Forward an entire channel/chat or just the latest N messages
- Forward a single forum **topic** (uses `iter_messages(reply_to=topic_id)` — no full-chat scan)
- Automatic copy-mode fallback for protected sources
- Preserves text formatting and inline hyperlinks
- Download media (documents, photos, videos) to local disk
- Export message history to JSON
- FloodWait-aware: sleeps when Telegram tells it to
- **Long-running automation** (`automate.py`): config-driven (source, dest, type) pairs polled on an interval, watermark per pair so only new messages get copied.
- **Web UI** (`server.py`): browse chats, add/edit/delete recurring pairs, trigger one-shot forwards, view run history — all in one page behind HTTP Basic Auth. Includes Dockerfile + Railway config.
- **Forum cloning end-to-end**: one click creates a new private supergroup, mirrors every source topic, and registers one recurring pair per topic.
- **Bulk backfill**: kick off all (or a filtered subset of) pairs serially with `POST /api/run-all`. Each pair appears as a separate job in the live tracker.
- **Live job tracker** with per-pair progress bars, cancel buttons that work mid-scan (not just between message copies), and inline edit on every pair row.
- **Pause / Resume** kill switch (`POST /api/pause` / `POST /api/resume`): cancels every in-flight job, halts the scheduler between cycles, and blocks bulk/manual/one-shot triggers until resumed. One-click in the header.

## Install

Python 3.11+ recommended.

```bash
git clone https://github.com/apppurchasespro-hash/telegram-forwarder.git
cd telegram-forwarder
pip install -r requirements.txt
```

`cryptg` speeds up media transfer significantly. If it fails to build on your platform, you can drop it from `requirements.txt` — the tool will still work, just slower for large files.

## Setup

1. Get your `api_id` and `api_hash` from <https://my.telegram.org> → API development tools.
2. Copy the example config and fill it in:

   ```bash
   cp config.example.json config.json
   ```

   ```json
   {
     "api_id": 12345678,
     "api_hash": "your_api_hash_here",
     "download_path": "./downloads",
     "max_concurrent_downloads": 3
   }
   ```

3. First run will prompt for your phone number and Telegram login code, then save a `tg_session.session` file so you don't have to log in again.

`config.json` and `*.session` are gitignored — never commit them.

## Usage

### List your chats (to find IDs)

```bash
python cli.py list-chats --limit 200
```

Output includes the chat ID you'll pass to other commands (the negative `-100…` value for channels/supergroups).

### Forward a whole channel

```bash
python cli.py forward \
  --source -1001234567890 \
  --dest   -1009876543210 \
  --type all \
  --limit 100 \
  --delay 1
```

- `--type` is one of `all`, `media`, `documents`, `messages`
- `--limit 0` (default) means no limit — the entire chat history
- `--delay` is seconds between sends; raise it if you hit FloodWait

If the source has "Restrict saving content" enabled, you'll see:

```
⚠ Source is a protected chat — using copy-mode (download + re-upload)
```

This is normal and slower than native forwarding, since every message is downloaded and re-uploaded.

### Forward a single forum topic

```bash
python cli.py list-topics --chat -1001234567890
python cli.py forward-topic \
  --source -1001234567890 \
  --topic 1234 \
  --dest   -1009876543210 \
  --type all \
  --delay 0.5
```

The general topic (id=1) requires a full scan; named topics are fetched directly via `messages.getReplies`.

### Download media to disk

```bash
python cli.py download --chat -1001234567890 --type documents --limit 500
```

Files are written under `<download_path>/<chat title>/{media,documents,messages}/`.

### Export message history to JSON

```bash
python cli.py export --chat -1001234567890 --output messages.json --limit 1000
```

## Limitations and notes

- **You can only forward from chats you're a member of.** Telegram doesn't expose messages to non-members.
- **Copy mode is slower than native forwarding.** Each message is round-tripped through your machine. For large protected channels, plan for it: ~1 message/second with a 1s delay is sustainable.
- **FloodWait is real.** If you blast a channel with thousands of forwards, Telegram will rate-limit you for minutes or hours. The default `--delay 1.0` is a safe starting point.
- **Use a userbot account, not your main one, for heavy automation.** Telegram has banned accounts for aggressive forwarding patterns.
- **Per Telegram ToS**, only forward content you have the right to redistribute. This tool is a transport — what you do with it is on you.

## Automation (`automate.py`)

`automate.py` is a long-running poller that copies only the new messages since each pair's last successful copy. State (per-pair watermarks) lives in `watermarks.json` — survives restarts.

### Local

```bash
cp pairs.example.json pairs.json
# edit pairs.json — set your (source, dest, type) pairs
python automate.py
```

`pairs.json` schema:

```json
{
  "interval_seconds": 3600,
  "pairs": [
    {
      "name": "my-feed",
      "source": -1001234567890,
      "dest":   -1009876543210,
      "type":   "all",
      "delay_seconds": 1.0,
      "max_per_run":   200
    }
  ]
}
```

- `name` — unique key for the watermark. Don't change after first run.
- `type` — `all`, `media`, `documents`, or `messages` (same as the CLI).
- `max_per_run` — per-run cap; protects you from FloodWait if the source has a backlog.

`RUN_ONCE_AND_EXIT=1 python automate.py` runs one pass and exits — useful for testing or one-shot cron jobs.

## Web UI (`server.py`)

`server.py` is a Quart (async Flask) app that serves a single-page UI for managing forwards, plus the same hourly scheduler as `automate.py` running in the background.

Local:

```bash
pip install -r requirements.txt
export DASH_USER=admin
export DASH_PASS=pick-a-strong-password
python server.py
# UI on http://localhost:5000
```

What you can do from the browser:
- Browse all your chats with search/type filter; click to fill the source or dest field
- Add, edit, delete recurring pairs (inline "edit" on each row pre-fills the form, including the resolved source/dest topic dropdowns)
- Trigger "Run now" on a single pair without waiting for the next interval
- Send a one-shot forward (latest N messages of a given type) — doesn't touch watermarks
- **Clone a forum end-to-end**: provide a source forum chat ID and a destination title; the server creates a new private supergroup with forum=True, mirrors every topic, and writes one recurring pair per topic. Skips General by default (Telegram doesn't allow filtering by `reply_to=1`).
- **Cancel a running job** at any time — works during the pre-fetch scan as well as between copies. Mid-file cancellation is not supported (Telethon doesn't expose download/upload cancellation).
- See the run log (manual + scheduled events, with errors) and a live job tracker (3 s poll) with per-pair progress bars.

### Bulk run

`POST /api/run-all` queues every pair (or a filtered subset) to run **one at a time** in the background and returns immediately. Each pair shows up as its own job; the response carries a `bulk_id` you can cancel via `POST /api/run-all/<bulk_id>/cancel` (cancellation takes effect between pairs, not mid-run).

```bash
# all pairs
curl -u $DASH_USER:$DASH_PASS -X POST $URL/api/run-all -d '{}'
# only pairs whose name starts with a prefix
curl -u $DASH_USER:$DASH_PASS -X POST $URL/api/run-all \
  -H 'Content-Type: application/json' \
  -d '{"prefix": "my-clone--"}'
# specific names
curl -u $DASH_USER:$DASH_PASS -X POST $URL/api/run-all \
  -H 'Content-Type: application/json' \
  -d '{"names": ["pair-a", "pair-b"]}'
```

Auth: HTTP Basic Auth using `DASH_USER` + `DASH_PASS` env vars. If `DASH_PASS` is unset, auth is **disabled** (only do that for local dev).

### Watermark repair

`POST /api/pairs/<name>/watermark` overrides a pair's stored watermark. Use this to skip ahead (so historical messages aren't re-forwarded after a clone) or to roll backwards to re-forward a range. Always allowed to move the value in either direction.

```bash
curl -u $DASH_USER:$DASH_PASS -X POST $URL/api/pairs/my-pair/watermark \
  -H 'Content-Type: application/json' \
  -d '{"last_msg_id": 12345}'
```

Internally, per-message watermark saves go through `automate.py::save_pair_watermark`, an atomic read-modify-write of one pair's key under a process-wide lock. This prevents concurrent runners (scheduler + bulk + manual on different pairs) from clobbering each other when each holds a stale full-state snapshot. The save also refuses to write a value LOWER than what's on disk unless `allow_regression=True` (only the repair endpoint sets that flag), so a stale in-flight job cannot zap a manual repair.

### Deploy on Railway (24/7)

This repo includes `Dockerfile` and `railway.json` for a one-service deployment that runs both the UI and the scheduler.

1. **Generate a session string locally** (one-time):

   ```bash
   # After logging in locally with the CLI at least once:
   python convert_session.py > session_string.txt
   ```

   Treat `session_string.txt` like a password — it grants full account access.

2. **Create a new Railway project** from this repo (`apppurchasespro-hash/telegram-forwarder`).

3. **Set env vars** in the Railway service:

   | Variable | Value |
   |---|---|
   | `TELEGRAM_API_ID` | from <https://my.telegram.org> |
   | `TELEGRAM_API_HASH` | from <https://my.telegram.org> |
   | `TELETHON_SESSION_STRING` | contents of `session_string.txt` |
   | `PAIRS_JSON` *(optional)* | inline JSON pair config; seeds `/app/data/pairs.json` on first boot if missing. After that, manage pairs via the UI. |
   | `STATE_PATH` | `/app/data/watermarks.json` (set by the Dockerfile already) |
   | `PAIRS_PATH` | `/app/data/pairs.json` (set by the Dockerfile already) |
   | `RUN_LOG_PATH` | `/app/data/run_log.json` (set by the Dockerfile already) |
   | `INITIAL_WATERMARKS_JSON` *(optional)* | seed value applied on first boot if `STATE_PATH` doesn't yet exist — e.g. `{"my-feed":{"last_msg_id":12345}}`. Prevents re-forwarding history when redeploying. |
   | `DASH_USER` | username for the web UI Basic Auth |
   | `DASH_PASS` | password for the web UI Basic Auth (set this — if unset, UI is open) |

4. **Add a Railway volume** mounted at `/app/data` so pairs + watermarks + run log survive redeploys.

5. **Expose a public domain.** After the first deploy, run `railway domain` (or use the UI) to generate a `*.up.railway.app` URL.

6. **Deploy.** Tail the logs — you should see `Logged in as: ...` then `server ready` and the UI will be reachable at the public domain. Browser will prompt for `DASH_USER`/`DASH_PASS`.

The container runs `python server.py`, which serves the UI and runs the scheduler in the same process.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `ChatForwardsRestricted` | Source is protected. Update to the latest version of this tool — it auto-switches to copy mode. |
| `FloodWaitError: A wait of N seconds is required` | You're being rate-limited. Increase `--delay` or wait it out. |
| `Could not find the input entity for PeerChannel(channel_id=...)` | Either you're not a member of the source, or the ID is wrong. Run `list-chats` to confirm. |
| First run hangs on phone number prompt | Run from a real terminal (not a non-interactive shell). |
| Hyperlinks/bold/italics lost | Update to the latest version — `msg.entities` is now passed through on copy. |

## License

MIT — see [LICENSE](LICENSE).

## Contributing

Issues and PRs welcome. Keep changes minimal and avoid adding dependencies unless there's a strong reason.

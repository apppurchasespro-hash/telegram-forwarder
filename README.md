# telegram-forwarder

> Robust Telegram channel forwarder with **atomic watermarks**, **native batch forward + drop_author**, **OOM-safe streaming** for 80k+ message channels, **live edit/delete propagation**, and a web UI. Built on [Telethon](https://github.com/LonamiWebs/Telethon).

Works on protected (restrict-saving) channels via automatic copy-mode fallback. Self-hostable in a single Docker container, runs on a $5 VPS.

## Why this exists

Most existing Telegram forwarders (tgcf, telemirror, the small cloners) have at least one of these problems:

- **Duplicate posting** when multiple runners share a single watermark file — concurrent saves clobber each other's keys and the next cycle re-forwards old messages.
- **OOM crashes** when you try to clone a channel with tens of thousands of messages (they build the full message list in RAM before forwarding).
- **One-at-a-time copy mode** even on un-protected sources — wastes bandwidth and runs ~50× slower than necessary.
- **No live edit/delete propagation** — when the source channel edits a message, the mirror goes stale.

This tool fixes all four:

- Atomic per-pair read-modify-write watermark with regression protection (`automate.save_pair_watermark`).
- Streaming `iter_messages(reverse=True, min_id=watermark)` with per-batch flush — memory stays flat regardless of channel size.
- Native server-side `forward_messages` at 100 messages per call, with `drop_author=True` to strip the "Forwarded from X" header — falls back to copy mode only when the source forbids forwarding (`noforwards=True`). Per-pair text replacements stay on the native path and are applied as a follow-up `edit_message` on the destination caption.
- Telethon event handlers on `MessageEdited` / `MessageDeleted` with a persistent `src_id → dest_id` map.

## Compared to similar tools

| Feature | this repo | [tgcf](https://github.com/aahnik/tgcf) | [telemirror](https://github.com/khoben/telemirror) |
|---|---|---|---|
| Stars at time of writing | 0 (new) | 1.6k | 303 |
| Last commit (as of 2026-05) | active | Dec 2022 (stale) | active |
| Atomic concurrent watermark save | ✅ | ⚠ vulnerable | ⚠ vulnerable |
| OOM-safe streaming on 80k+ channels | ✅ | ❌ builds full list | ❌ builds full list |
| Native batch forward (100/call) | ✅ | ❌ one-at-a-time | ⚠ partial |
| `drop_author=True` (strip forward tag) | ✅ | ❌ | ❌ |
| Auto copy-mode for protected sources | ✅ | ✅ | ✅ |
| Live edit/delete propagation | ✅ | ❌ | ✅ |
| Per-pair regex find/replace | ✅ | ✅ | ⚠ partial |
| Forum-clone end-to-end (create supergroup + mirror topics) | ✅ | ❌ | ❌ |
| Web UI for pair management | ✅ | ✅ | ❌ |
| Pause / resume kill switch | ✅ | ❌ | ❌ |
| Single Docker container | ✅ | ✅ | ✅ |

See [CREDITS.md](CREDITS.md) for the inspiration we drew from tgcf (text replacement plugin pattern) and telemirror (live edit/delete pattern).

## Features

**Forwarding modes**
- Native server-side `forward_messages` at 100/call with optional `drop_author=True` — fast and bandwidth-free. Also used when the destination is a forum topic (via raw `ForwardMessagesRequest` with `top_msg_id`).
- Copy mode (download to temp + re-upload) — automatic fallback only when the source is protected (`noforwards=True`).
- Preserves text formatting, inline hyperlinks, and the original filename on documents.

**Backfill at scale**
- Streaming iteration over `iter_messages(reverse=True, min_id=watermark)` — memory stays flat on arbitrarily large channels.
- Per-batch flush + atomic watermark save — a crash mid-clone resumes cleanly without dupes or skips.
- Per-batch error recovery: one bad message (expired media, MessageService, etc.) doesn't stall the run.

**Pair management**
- Long-running poller (`automate.py`): config-driven (source, dest, type) pairs on an interval. Watermarks survive restarts.
- Web UI (`server.py`): browse chats, add/edit/delete recurring pairs, trigger one-shot forwards, view run history — all in one page behind HTTP Basic Auth.
- Forum clone end-to-end: one click creates a new private supergroup, mirrors every source topic, and registers one recurring pair per topic.
- Bulk backfill: kick off all (or a filtered subset of) pairs serially with `POST /api/run-all`. Each pair appears as a separate job.

**Live mirroring** *(new)*
- Telethon `MessageEdited` event handler — when the source edits a message, the corresponding mirror message is edited too.
- `MessageDeleted` event handler — source deletions cascade to the mirror.
- Persistent `src_id → dest_id` map (`message_map.json`) — atomic writes under an asyncio lock.

**Content transformation** *(new)*
- Per-pair `replacements` list of `{find, replace, regex}` rules — applied to message text and media captions. Inspired by tgcf's format plugin.
- Stays on the fast native-forward path: each batch is server-side forwarded, then `edit_message` rewrites the destination caption with the transformed text (two API calls per changed message instead of a full download + re-upload). Caption edits use `parse_mode=None` so stray `*`/`_`/`[` left over from regex strips don't trip Telethon's markdown parser.

**Operations**
- Pause / resume kill switch (`POST /api/pause` / `POST /api/resume`) — cancels in-flight jobs, halts the scheduler, blocks bulk/manual/one-shot triggers until resumed.
- Live job tracker with per-pair progress bars and cancel buttons that work mid-scan (not just between message copies).
- Watermark repair endpoint to roll a pair backwards (re-forward a range) or forwards (skip ahead after a clone).
- FloodWait-aware throughout: sleeps when Telegram tells it to, retries cleanly.

## When NOT to use this

- **You only want to forward bot-sent messages.** Telethon needs a real user account — use the official Bot API for bot-to-bot forwarding instead.
- **You expect zero "(edited)" tags on edited messages.** Telegram always tags edited messages, even when the edit is just a caption replacement. Unavoidable.
- **You need backwards-compatible edits to already-mirrored messages.** Only messages forwarded *after* this version of the tool was running will have `message_map` entries — older messages can't be live-edited retroactively without a separate batch job.
- **You want to forward from a chat you're not a member of.** Telegram doesn't expose messages to non-members. There's no workaround.

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

The general topic (id=1) requires a full scan; named topics are fetched directly via `messages.getReplies`. `forward-topic` also accepts `--type docs_and_text` (documents + text-only, skips photos/voice notes) in addition to the four types above.

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
      "source_topic": 141,
      "dest_topic":   13,
      "type":   "all",
      "delay_seconds": 1.0,
      "max_per_run":   2000,
      "drop_author": true,
      "paused": false,
      "max_file_size_mb": 0,
      "replacements": [
        {"find": "Join @oldchannel", "replace": "Visit @newchannel"},
        {"find": "https?://t\\.me/\\S+", "replace": "", "regex": true}
      ]
    }
  ]
}
```

| Field | Required | Default | Notes |
|---|---|---|---|
| `name` | yes | — | Unique watermark key. Don't rename after first run. |
| `source` | yes | — | Negative chat id (`-100…` for channels/supergroups). |
| `dest` | yes | — | Negative chat id. |
| `source_topic` | no | — | Forum topic id. `1` = General (requires full chat scan). Skip for non-forum sources. |
| `dest_topic` | no | — | Destination forum topic id. Skip if dest is not a forum. |
| `type` | no | `all` | One of `all` / `media` / `documents` / `messages` / `docs_and_text`. |
| `delay_seconds` | no | `1.0` | Seconds between sends. Raise on FloodWait. |
| `max_per_run` | no | `0` (unlimited) | Per-run cap; protects you from FloodWait if the source has a backlog. |
| `drop_author` | no | `true` | Strip "Forwarded from X" header on native forwards. Requires Premium sending account. |
| `paused` | no | `false` | `true` makes the scheduler skip this pair. Manual `/api/pairs/<name>/run` still works. |
| `max_file_size_mb` | no | `0` (unlimited) | Copy-mode only: skip messages whose media exceeds this cap **before** download. |
| `replacements` | no | `[]` | List of `{find, replace, regex?}` rules. Forwards natively + edits caption after — does **not** force copy-mode. |

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

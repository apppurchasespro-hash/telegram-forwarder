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

# Credits & Inspirations

This project borrows ideas (not code) from the wider Telegram-forwarder
open-source ecosystem. Listed here so the people who paved the way get
visible credit.

## Direct inspirations

### Per-pair text replacement (find / replace / regex)
Inspired by the **format / replace** plugin from
[aahnik/tgcf](https://github.com/aahnik/tgcf) (GPL-3.0).

We implemented the rule pipeline independently in `automate.apply_replacements`
(no source code copied — only the API shape and the idea of forcing
copy-mode when content is mutated). Thanks to @aahnik for showing that a
simple list of `{find, replace, regex}` is enough for 90 % of caption-clean-up
needs.

### Live edit / delete propagation
Inspired by the source-edit / source-delete event handlers in
[khoben/telemirror](https://github.com/khoben/telemirror) (MIT License).

We register Telethon `events.MessageEdited` and `events.MessageDeleted`
handlers in `server.py` (`_on_source_edited`, `_on_source_deleted`) and
back them with our own `message_map.json` (a `{pair_name: {src_id: dest_id}}`
dict, atomic writes under an asyncio lock). The pattern of "keep a
src-id → dest-id map and replay updates on it" is the contribution we
copied; the implementation is ours.

## Libraries we stand on

- **[Telethon](https://github.com/LonamiWebs/Telethon)** (MIT) — the
  Telegram MTProto client doing all the real work.
- **[Quart](https://github.com/pallets/quart)** (MIT) — async Flask
  replacement powering the dashboard.

## Things we did differently

For anyone comparing forwarders, the parts of this codebase that aren't
borrowed from anywhere we know of:

- **Atomic per-pair watermark save** (`automate.save_pair_watermark`)
  — fixes a duplicate-posting race we observed when multiple runners hold
  stale snapshots of a single state file. Most forwarders we surveyed
  appear vulnerable to this.
- **Pause / resume kill switch** — global flag that cancels in-flight
  jobs + bulks and blocks the scheduler from launching new pair runs.
- **Native server-side batch forward with `drop_author=True`** — 100
  messages per `forward_messages` call, "Forwarded from X" header stripped.
  Falls back to copy-mode only when the source forbids forwarding
  (`noforwards=True`). Forum-topic destinations stay on the native path via
  raw `ForwardMessagesRequest` with `top_msg_id`; per-pair replacements stay
  on it too by editing the destination caption after forwarding.
- **Streaming iteration** (`iter_messages(reverse=True, min_id=watermark)`)
  with per-batch flush — handles 80 k+ message channels in flat memory.
- **Forum clone-end-to-end endpoint** (`/api/clone-forum`) — create dest
  supergroup with `forum=True`, mirror every topic, write `pairs.json`
  entries in one shot.

## License of this project

MIT — see [LICENSE](LICENSE). The borrowed ideas above are credited under
their original licenses; if you redistribute any of the clearly-credited
helpers, please carry the credit forward.

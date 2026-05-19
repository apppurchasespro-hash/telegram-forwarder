"""
Long-running forwarder: poll configured (source, dest) pairs on an interval,
copy only new messages since each pair's persisted watermark.

Config is pairs.json (or PAIRS_JSON env var). State lives in watermarks.json
(or STATE_PATH env var) so it survives container restarts.

Run modes:
    python automate.py                  # loop forever
    RUN_ONCE_AND_EXIT=1 python automate.py   # one pass, then exit (for tests)
"""

import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

from telethon.errors import FloodWaitError

from downloader import TelegramDownloader, load_config

BASE_DIR = Path(__file__).parent
DEFAULT_PAIRS = BASE_DIR / "pairs.json"
DEFAULT_STATE = BASE_DIR / "watermarks.json"
DEFAULT_MSG_MAP = BASE_DIR / "message_map.json"


def _resolve_pairs_path() -> Path:
    # PAIRS_PATH wins (cloud volume), else local default.
    return Path(os.environ.get("PAIRS_PATH", str(DEFAULT_PAIRS)))


def _resolve_state_path() -> Path:
    return Path(os.environ.get("STATE_PATH", str(DEFAULT_STATE)))


def _resolve_msg_map_path() -> Path:
    return Path(os.environ.get("MSG_MAP_PATH", str(DEFAULT_MSG_MAP)))


# Per-pair regex find/replace rules applied to message text + caption in
# copy-mode. Inspired by aahnik/tgcf's "format" plugin. Returns a new string —
# never mutates input. Bad regex patterns are skipped silently.
def apply_replacements(text: Optional[str], rules: Optional[list]) -> str:
    if not text:
        return text or ""
    if not rules:
        return text
    out = text
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        find = rule.get("find", "")
        replace = rule.get("replace", "")
        if not find:
            continue
        if rule.get("regex", False):
            try:
                out = re.sub(find, replace, out)
            except re.error:
                continue
        else:
            out = out.replace(find, replace)
    return out


# ── Source-message-id → destination-message-id map ────────────────────────
# Powers live edit/delete propagation (telemirror-style). When the source
# edits or deletes a message we forwarded, the event handler in server.py
# looks up the dest id here and applies the same change. Structure:
#   {pair_name: {str(src_id): dest_id}}
# JSON requires string keys; we cast on read.
_msg_map: dict[str, dict[str, int]] = {}
_msg_map_lock = asyncio.Lock()
_msg_map_loaded = False


def load_message_map() -> dict:
    """Read from disk into in-memory _msg_map. Idempotent."""
    global _msg_map, _msg_map_loaded
    path = _resolve_msg_map_path()
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                _msg_map = data
        except (json.JSONDecodeError, OSError) as e:
            print(f"message_map.json unreadable, starting fresh: {e}", file=sys.stderr)
            _msg_map = {}
    _msg_map_loaded = True
    return _msg_map


async def _flush_message_map_locked() -> None:
    """Caller must hold _msg_map_lock. Atomic write via .tmp+replace."""
    path = _resolve_msg_map_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(_msg_map, f)
    tmp.replace(path)


async def record_mappings(pair_name: str, pairs_iter) -> None:
    """Record many (src_id, dest_id) pairs atomically + flush.

    pairs_iter: iterable of (src_id, dest_id) tuples.
    Skips Nones (failed sends). Safe to call concurrently from multiple runners.
    """
    async with _msg_map_lock:
        bucket = _msg_map.setdefault(pair_name, {})
        any_added = False
        for src_id, dest_id in pairs_iter:
            if src_id is None or dest_id is None:
                continue
            bucket[str(src_id)] = int(dest_id)
            any_added = True
        if any_added:
            await _flush_message_map_locked()


def lookup_dest_id(pair_name: str, src_id: int) -> Optional[int]:
    """Return mapped dest id or None if pair/src isn't recorded."""
    bucket = _msg_map.get(pair_name)
    if not bucket:
        return None
    val = bucket.get(str(src_id))
    return int(val) if val is not None else None


async def forget_mappings(pair_name: str, src_ids) -> None:
    """Remove recorded entries (called after delete-propagation runs).
    Stops the map from growing forever for ephemeral source messages."""
    async with _msg_map_lock:
        bucket = _msg_map.get(pair_name)
        if not bucket:
            return
        changed = False
        for sid in src_ids:
            if bucket.pop(str(sid), None) is not None:
                changed = True
        if changed:
            await _flush_message_map_locked()


def load_pairs() -> dict:
    path = _resolve_pairs_path()
    if not path.exists():
        # First boot: seed from PAIRS_JSON env var if provided (inline JSON).
        # Lets a fresh container start with a known pair config.
        seed = os.environ.get("PAIRS_JSON")
        if seed and seed.lstrip().startswith("{"):
            try:
                cfg = json.loads(seed)
                save_pairs(cfg)
                print(f"Seeded pairs from PAIRS_JSON ({len(cfg.get('pairs', []))} pairs)")
                return cfg
            except json.JSONDecodeError as e:
                print(f"PAIRS_JSON is not valid JSON: {e}", file=sys.stderr)
                raise
        raise FileNotFoundError(
            f"Pairs config not found: {path}. Copy pairs.example.json to pairs.json, "
            "or set the PAIRS_JSON env var (inline JSON, seeds the file)."
        )
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_pairs(cfg: dict) -> None:
    path = _resolve_pairs_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, sort_keys=True)
    tmp.replace(path)


def load_state() -> dict:
    path = _resolve_state_path()
    if not path.exists():
        # First boot: seed from INITIAL_WATERMARKS_JSON if provided.
        # Stops us from re-forwarding history when a deployment starts with
        # an empty volume but the user has already copied messages elsewhere.
        seed = os.environ.get("INITIAL_WATERMARKS_JSON")
        if seed:
            try:
                state = json.loads(seed)
                save_state(state)
                print(f"Seeded watermarks from INITIAL_WATERMARKS_JSON ({len(state)} pairs)")
                return state
            except json.JSONDecodeError as e:
                print(f"INITIAL_WATERMARKS_JSON is not valid JSON: {e}", file=sys.stderr)
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: dict) -> None:
    path = _resolve_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    tmp.replace(path)


# Serializes the per-pair atomic save. Without this, two concurrent runners
# each hold a stale snapshot of the full state dict and clobber each other's
# keys when they call save_state(state). Observed 2026-05-19: scheduled jobs
# successfully forwarded N messages and saved wm=X, but concurrent bulk runs
# on different pairs wrote stale dicts that reset that pair's wm back to its
# pre-run value (or 0). Next bulk iteration on the reset pair re-forwarded
# everything → duplicates in destination.
_save_lock = asyncio.Lock()


async def save_pair_watermark(name: str, last_msg_id: int, updated_at: int, *, allow_regression: bool = False) -> None:
    """Atomic read-modify-write of one pair's watermark. Reload latest from
    disk, update only this pair's key, write back. Preserves any updates other
    runners made to other keys between our last load and now.

    By default refuses to write a value LOWER than what's already on disk.
    This protects against a stale in-flight run (loaded state when wm was X,
    started copying at X+1) zapping a manual repair (`/api/pairs/.../watermark`
    set wm=Y where Y > X). Pass `allow_regression=True` to override (used by
    the repair endpoint itself when you want to roll a watermark backwards).
    """
    async with _save_lock:
        path = _resolve_state_path()
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                state = json.load(f)
        else:
            state = {}
        if not allow_regression:
            current = int(state.get(name, {}).get("last_msg_id", 0))
            if last_msg_id < current:
                return
        state[name] = {"last_msg_id": last_msg_id, "updated_at": updated_at}
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)
        tmp.replace(path)


def _pair_key(pair: dict) -> str:
    return pair.get("name") or f"{pair['source']}:{pair['dest']}"


def _matches_type(msg, ftype: str, dl: TelegramDownloader) -> bool:
    # Telegram service messages ("user joined", "channel created", "pinned X",
    # etc.) cannot be forwarded — forward_messages errors out and copy-mode has
    # nothing to send. Skip them regardless of ftype.
    from telethon.tl.patched import MessageService
    if isinstance(msg, MessageService):
        return False
    if ftype == "all":
        return True
    if ftype == "media":
        return dl._is_media(msg)
    if ftype == "documents":
        return dl._is_document(msg)
    if ftype == "messages":
        return bool(msg.message and not msg.media)
    if ftype == "docs_and_text":
        # documents (with or without caption) + text-only messages.
        # Skips photos/videos/voice/etc.
        return dl._is_document(msg) or bool(msg.message and not msg.media)
    return False


# Per-pair locks so the bulk runner, scheduler, and manual UI clicks can't
# concurrently iterate the same source — concurrent runs double-post because
# each runner loads the same watermark and walks the same message range.
_pair_locks: dict[str, asyncio.Lock] = {}


def _get_pair_lock(name: str) -> asyncio.Lock:
    lock = _pair_locks.get(name)
    if lock is None:
        lock = asyncio.Lock()
        _pair_locks[name] = lock
    return lock


async def run_pair(dl: TelegramDownloader, pair: dict, state: dict, job: Optional[dict] = None) -> dict:
    name = _pair_key(pair)
    lock = _get_pair_lock(name)
    if lock.locked():
        # Another runner has this pair. Bail out instead of double-posting.
        print(f"[{name}] another runner holds the lock — skipping this attempt")
        if job:
            job.update({"status": "finished", "total": 0})
        return {"forwarded": 0, "failed": 0, "last_id": int(state.get(name, {}).get("last_msg_id", 0)),
                "skipped_locked": True}
    async with lock:
        return await _run_pair_locked(dl, pair, state, job)


async def _run_pair_locked(dl: TelegramDownloader, pair: dict, state: dict, job: Optional[dict] = None) -> dict:
    name = _pair_key(pair)
    source = pair["source"]
    dest = pair["dest"]
    source_topic = pair.get("source_topic")
    dest_topic = pair.get("dest_topic")
    ftype = pair.get("type", "all")
    delay = float(pair.get("delay_seconds", 1.0))
    max_per_run = int(pair.get("max_per_run", 0)) or None
    # Strip the "Forwarded from X" header on native forwards.
    # Default True (most cloning use-cases want a clean mirror).
    # Telegram requires the SENDING account to have Premium for this flag
    # to work; non-premium accounts should set drop_author:false in their pair.
    drop_author = bool(pair.get("drop_author", True))
    # Per-pair text replacements (list of {find, replace, regex}). Forces
    # copy-mode because native forward_messages can't alter message bytes.
    replacements = pair.get("replacements") or []

    watermark = int(state.get(name, {}).get("last_msg_id", 0))

    # Pick transport: native server-side forward (fast, no bandwidth) when
    # allowed, otherwise copy-mode (download + re-upload). Native requires:
    #   - source has forwarding enabled (noforwards=False)
    #   - dest is not a forum topic (telethon 1.43 forward_messages has no
    #     top_msg_id; forum-topic dests stay on copy-mode for now)
    #   - no replacements configured (forward_messages re-sends original bytes)
    use_native = False
    try:
        src_entity = await dl.client.get_entity(source)
        src_protected = bool(getattr(src_entity, "noforwards", False))
        if not src_protected and not (dest_topic and dest_topic > 1) and not replacements:
            use_native = True
    except Exception as e:
        print(f"[{name}] couldn't resolve source for noforwards check: {e} — using copy-mode")

    mode = "native" if use_native else "copy"
    topic_note = ""
    if source_topic:
        topic_note += f" src_topic={source_topic}"
    if dest_topic:
        topic_note += f" dst_topic={dest_topic}"
    print(f"[{name}] source={source} dest={dest} type={ftype}{topic_note} since=#{watermark} mode={mode}")

    # Topic-aware iteration. reply_to=topic_id calls messages.getReplies and
    # returns only that topic's messages. Topic id=1 ("General") doesn't carry
    # reply_to, so server-side filter doesn't work — fall back to whole-chat
    # iteration in that case (handled by caller setting source_topic to null).
    iter_kwargs = {"min_id": watermark}
    if source_topic and source_topic > 1:
        iter_kwargs["reply_to"] = source_topic
    # When ftype="all" every message matches, so cap the fetch at max_per_run
    # — for cloned pairs sitting at watermark=0 this stops us from walking the
    # entire topic just to throw most of it away. For selective types we'd
    # undershoot, so leave uncapped there.
    if max_per_run and ftype == "all":
        iter_kwargs["limit"] = max_per_run

    ok = fail = 0
    last_ok_id = watermark

    if use_native:
        # Native server-side forward, STREAMING in batches of up to 100.
        # Telegram allows up to 100 ids per messages.forwardMessages call.
        # Streaming (instead of build-full-list-then-forward) keeps memory
        # flat on huge channels — 80k messages * 5KB/msg-object would otherwise
        # eat ~400MB before any forwarding starts and crash the worker.
        # iter_messages(reverse=True, min_id=X) yields msg.id > X in ASCENDING
        # order, so we forward chronologically and each successful batch
        # advances the watermark.
        BATCH = 100
        iter_kwargs.pop("limit", None)  # streaming: paginate via Telethon, cap by max_per_run below
        batch = []
        i = 0
        progress_printed_at = 0
        async def _flush_batch():
            nonlocal ok, fail, last_ok_id, i, batch, progress_printed_at
            if not batch:
                return False
            try:
                forwarded = await dl.client.forward_messages(dest, batch, source, drop_author=drop_author)
            except FloodWaitError as fw:
                print(f"[{name}] flood wait {fw.seconds}s")
                await asyncio.sleep(fw.seconds)
                try:
                    forwarded = await dl.client.forward_messages(dest, batch, source, drop_author=drop_author)
                except Exception as e:
                    print(f"[{name}] batch failed after flood-wait retry: {e} — skipping {len(batch)} msgs (last id #{batch[-1].id})")
                    fail += len(batch)
                    i += len(batch)
                    batch.clear()
                    return False
            except Exception as e:
                # One bad message (e.g. expired/unsupported media) would 500
                # the whole batch. Mark batch as failed and advance so the
                # run doesn't get stuck retrying the same broken span forever.
                print(f"[{name}] batch failed: {e} — skipping {len(batch)} msgs (last id #{batch[-1].id})")
                fail += len(batch)
                # Advance watermark past the bad batch so we don't loop on it.
                last_ok_id = max(last_ok_id, batch[-1].id)
                now = int(time.time())
                state[name] = {"last_msg_id": last_ok_id, "updated_at": now}
                await save_pair_watermark(name, last_ok_id, now)
                i += len(batch)
                batch.clear()
                return False
            # Single-message forward_messages returns a Message (not a list)
            # in some Telethon versions. Normalize so we can always zip.
            forwarded_list = forwarded if isinstance(forwarded, (list, tuple)) else [forwarded]
            mappings = []
            for m, f in zip(batch, forwarded_list):
                if f is not None:
                    ok += 1
                    last_ok_id = m.id
                    # f.id is the destination message id — record for live
                    # edit/delete propagation in server.py event handlers.
                    mappings.append((m.id, getattr(f, "id", None)))
                else:
                    fail += 1
            if mappings:
                await record_mappings(name, mappings)
            i += len(batch)
            now = int(time.time())
            state[name] = {"last_msg_id": last_ok_id, "updated_at": now}
            await save_pair_watermark(name, last_ok_id, now)
            if job:
                job.update({"done": i, "ok": ok, "fail": fail, "last_id": last_ok_id, "status": "running"})
            # Print every batch (more visible than every 10) — native is fast enough.
            print(f"[{name}] progress {i} (ok={ok} fail={fail} last_id=#{last_ok_id})")
            progress_printed_at = i
            batch.clear()
            return True

        async for m in dl.client.iter_messages(source, reverse=True, **iter_kwargs):
            if job and job.get("cancel"):
                print(f"[{name}] cancelled during scan at i={i}")
                job["status"] = "cancelled"
                break
            if not _matches_type(m, ftype, dl):
                continue
            batch.append(m)
            if len(batch) >= BATCH:
                await _flush_batch()
                if max_per_run and i >= max_per_run:
                    break
                await asyncio.sleep(delay)
        # Final partial batch.
        if batch and not (job and job.get("cancel")):
            await _flush_batch()
        if job and job["status"] == "running":
            job["status"] = "finished"
        print(f"[{name}] done: forwarded={ok} failed={fail} new_watermark=#{last_ok_id}")
        return {"forwarded": ok, "failed": fail, "last_id": last_ok_id}

    # ── Copy-mode (protected source or forum-topic dest) — per-message D+U.
    # Still builds the full list first; copy mode is slow per-message anyway so
    # the scan overhead is negligible relative to one big file's upload time.
    msgs = []
    async for m in dl.client.iter_messages(source, **iter_kwargs):
        if job and job.get("cancel"):
            print(f"[{name}] cancelled during scan ({len(msgs)} matched so far)")
            job["status"] = "cancelled"
            job["finished_at"] = int(time.time())
            return {"forwarded": 0, "failed": 0, "last_id": watermark, "cancelled": True}
        if _matches_type(m, ftype, dl):
            msgs.append(m)

    msgs.sort(key=lambda m: m.id)
    if max_per_run:
        msgs = msgs[:max_per_run]

    if not msgs:
        print(f"[{name}] no new messages")
        if job:
            job.update({"total": 0, "status": "finished"})
        return {"forwarded": 0, "failed": 0, "last_id": watermark}

    print(f"[{name}] copying {len(msgs)} new (#{msgs[0].id} -> #{msgs[-1].id})")
    (BASE_DIR / "temp").mkdir(exist_ok=True)

    if job:
        job.update({"total": len(msgs), "status": "running"})
    for i, m in enumerate(msgs, 1):
        if job and job.get("cancel"):
            print(f"[{name}] cancelled at {i}/{len(msgs)}")
            job["status"] = "cancelled"
            break
        # Apply per-pair text replacements before send. Pass None when no
        # rules so _copy_message_to preserves the original entities.
        override = None
        if replacements and m.message:
            transformed = apply_replacements(m.message, replacements)
            if transformed != m.message:
                override = transformed
        sent = await dl._copy_message_to(m, dest, dest_topic=dest_topic, text_override=override)
        if sent:
            ok += 1
            last_ok_id = m.id
            # Persist watermark per-message so a crash doesn't re-send anything.
            # Atomic per-pair RMW — see save_pair_watermark docstring.
            now = int(time.time())
            state[name] = {"last_msg_id": last_ok_id, "updated_at": now}
            await save_pair_watermark(name, last_ok_id, now)
            dest_msg_id = getattr(sent, "id", None)
            if dest_msg_id is not None:
                await record_mappings(name, [(m.id, dest_msg_id)])
        else:
            fail += 1
        if job:
            job.update({"done": i, "ok": ok, "fail": fail, "last_id": last_ok_id})
        if i % 10 == 0 or i == len(msgs):
            print(f"[{name}] progress {i}/{len(msgs)} (ok={ok} fail={fail})")
        await asyncio.sleep(delay)
    if job and job["status"] == "running":
        job["status"] = "finished"

    try:
        import shutil
        shutil.rmtree(BASE_DIR / "temp", ignore_errors=True)
    except Exception:
        pass

    print(f"[{name}] done: forwarded={ok} failed={fail} new_watermark=#{last_ok_id}")
    return {"forwarded": ok, "failed": fail, "last_id": last_ok_id}


async def run_once(dl: Optional[TelegramDownloader] = None) -> dict:
    cfg = load_pairs()
    state = load_state()
    pairs = cfg.get("pairs", [])
    if not pairs:
        print("No pairs configured — nothing to do.")
        return {}

    own = False
    if dl is None:
        dl = TelegramDownloader(load_config())
        await dl.start()
        own = True

    summary = {}
    try:
        for pair in pairs:
            try:
                summary[_pair_key(pair)] = await run_pair(dl, pair, state)
            except Exception as e:
                print(f"[{_pair_key(pair)}] FAILED: {e}", file=sys.stderr)
                summary[_pair_key(pair)] = {"error": str(e)}
    finally:
        if own:
            await dl.stop()
    return summary


async def main():
    cfg = load_pairs()
    interval = int(cfg.get("interval_seconds", 3600))
    run_once_only = os.environ.get("RUN_ONCE_AND_EXIT") == "1"

    dl = TelegramDownloader(load_config())
    await dl.start()

    try:
        while True:
            t0 = time.time()
            try:
                await run_once(dl=dl)
            except Exception as e:
                print(f"run_once crashed: {e}", file=sys.stderr)
            if run_once_only:
                return
            elapsed = time.time() - t0
            sleep_for = max(60, interval - int(elapsed))
            print(f"sleeping {sleep_for}s until next run...")
            await asyncio.sleep(sleep_for)
    finally:
        await dl.stop()


if __name__ == "__main__":
    asyncio.run(main())

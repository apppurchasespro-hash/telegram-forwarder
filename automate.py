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
import sys
import time
from pathlib import Path
from typing import Optional

from downloader import TelegramDownloader, load_config

BASE_DIR = Path(__file__).parent
DEFAULT_PAIRS = BASE_DIR / "pairs.json"
DEFAULT_STATE = BASE_DIR / "watermarks.json"


def _resolve_pairs_path() -> Path:
    # PAIRS_PATH wins (cloud volume), else local default.
    return Path(os.environ.get("PAIRS_PATH", str(DEFAULT_PAIRS)))


def _resolve_state_path() -> Path:
    return Path(os.environ.get("STATE_PATH", str(DEFAULT_STATE)))


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


def _pair_key(pair: dict) -> str:
    return pair.get("name") or f"{pair['source']}:{pair['dest']}"


def _matches_type(msg, ftype: str, dl: TelegramDownloader) -> bool:
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


async def run_pair(dl: TelegramDownloader, pair: dict, state: dict, job: Optional[dict] = None) -> dict:
    name = _pair_key(pair)
    source = pair["source"]
    dest = pair["dest"]
    source_topic = pair.get("source_topic")
    dest_topic = pair.get("dest_topic")
    ftype = pair.get("type", "all")
    delay = float(pair.get("delay_seconds", 1.0))
    max_per_run = int(pair.get("max_per_run", 0)) or None

    watermark = int(state.get(name, {}).get("last_msg_id", 0))
    topic_note = ""
    if source_topic:
        topic_note += f" src_topic={source_topic}"
    if dest_topic:
        topic_note += f" dst_topic={dest_topic}"
    print(f"[{name}] source={source} dest={dest} type={ftype}{topic_note} since=#{watermark}")

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

    ok = fail = 0
    last_ok_id = watermark
    for i, m in enumerate(msgs, 1):
        if job and job.get("cancel"):
            print(f"[{name}] cancelled at {i}/{len(msgs)}")
            job["status"] = "cancelled"
            break
        success = await dl._copy_message_to(m, dest, dest_topic=dest_topic)
        if success:
            ok += 1
            last_ok_id = m.id
            # Persist watermark per-message so a crash doesn't re-send anything.
            state[name] = {"last_msg_id": last_ok_id, "updated_at": int(time.time())}
            save_state(state)
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

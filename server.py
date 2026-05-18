"""
Web UI + REST API for telegram-forwarder.

One Quart app that:
  - serves the single-page UI on /
  - exposes JSON endpoints (auth required) for chat lists, pair CRUD,
    manual runs, one-shot forwards, and run history
  - runs the background scheduler in the same event loop, sharing one
    TelegramClient with the HTTP handlers
  - exposes /healthz unauthenticated for platform healthchecks
"""

import asyncio
import json
import os
import re
import secrets
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from quart import Quart, request, jsonify, render_template, Response

from telethon import utils as tg_utils
from telethon.tl.functions.channels import CreateChannelRequest, ToggleForumRequest
from telethon.tl.functions.messages import CreateForumTopicRequest

from downloader import TelegramDownloader, load_config
from automate import (
    BASE_DIR,
    load_pairs,
    save_pairs,
    load_state,
    save_state,
    save_pair_watermark,
    run_pair,
    _pair_key,
    _matches_type,
)

# ───── State ──────────────────────────────────────────────────────────────

app = Quart(__name__)
app.config["PROVIDE_AUTOMATIC_OPTIONS"] = True

DASH_USER = os.environ.get("DASH_USER", "admin")
DASH_PASS = os.environ.get("DASH_PASS")  # if unset, auth disabled (local dev only)

_state_lock = asyncio.Lock()        # protects pairs.json + watermarks.json + run_log
_dl: Optional[TelegramDownloader] = None  # shared Telegram client
_scheduler_task: Optional[asyncio.Task] = None
_run_log: list[dict] = []           # in-memory ring buffer
_RUN_LOG_MAX = 500
RUN_LOG_PATH = Path(os.environ.get("RUN_LOG_PATH", str(BASE_DIR / "run_log.json")))

# Job registry — live + recently-finished jobs. status: queued|running|finished|cancelled|error
_jobs: dict[str, dict] = {}
_JOBS_MAX = 200
_job_counter = 0

# Global pause flag. When True: scheduler skips new pair runs, bulks halt
# between pairs, all in-flight jobs receive cancel. Survives nothing on
# restart — re-pause after deploy if you still want the brake on.
_paused: bool = False


def _new_job(kind: str, label: str, total: int = 0) -> dict:
    global _job_counter
    _job_counter += 1
    job_id = f"job-{_job_counter}-{int(time.time())}"
    job = {
        "id": job_id,
        "kind": kind,
        "label": label,
        "status": "queued",
        "started_at": int(time.time()),
        "finished_at": None,
        "total": total,
        "done": 0,
        "ok": 0,
        "fail": 0,
        "last_id": None,
        "cancel": False,
    }
    _jobs[job_id] = job
    if len(_jobs) > _JOBS_MAX:
        # Evict oldest finished jobs.
        finished = [j for j in _jobs.values() if j["status"] in ("finished", "cancelled", "error")]
        finished.sort(key=lambda j: j["finished_at"] or 0)
        for j in finished[: len(_jobs) - _JOBS_MAX]:
            _jobs.pop(j["id"], None)
    return job


def _log_event(event: dict) -> None:
    event = {"ts": int(time.time()), **event}
    _run_log.append(event)
    if len(_run_log) > _RUN_LOG_MAX:
        del _run_log[: len(_run_log) - _RUN_LOG_MAX]
    try:
        RUN_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(RUN_LOG_PATH, "w", encoding="utf-8") as f:
            json.dump(_run_log[-_RUN_LOG_MAX:], f)
    except Exception:
        pass


def _load_run_log() -> None:
    if RUN_LOG_PATH.exists():
        try:
            with open(RUN_LOG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    _run_log.extend(data[-_RUN_LOG_MAX:])
        except Exception as e:
            print(f"failed to load run log: {e}", file=sys.stderr)


# ───── Auth ───────────────────────────────────────────────────────────────


def _check_auth(header: Optional[str]) -> bool:
    if not DASH_PASS:
        return True  # disabled in local dev
    if not header or not header.startswith("Basic "):
        return False
    import base64
    try:
        raw = base64.b64decode(header[6:]).decode("utf-8")
    except Exception:
        return False
    if ":" not in raw:
        return False
    user, _, pw = raw.partition(":")
    return secrets.compare_digest(user, DASH_USER) and secrets.compare_digest(pw, DASH_PASS)


def _auth_required():
    return Response(
        "Authentication required",
        401,
        {"WWW-Authenticate": 'Basic realm="telegram-forwarder"'},
    )


@app.before_request
async def _auth_gate():
    path = request.path or ""
    if path == "/healthz":
        return None
    if not _check_auth(request.headers.get("Authorization")):
        return _auth_required()


# ───── Routes: HTML ───────────────────────────────────────────────────────


@app.route("/")
async def index():
    return await render_template("index.html")


@app.route("/healthz")
async def healthz():
    return jsonify({"ok": True, "telegram_ready": _dl is not None and _dl.client is not None})


# ───── Routes: chats ──────────────────────────────────────────────────────


@app.route("/api/chats")
async def api_chats():
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    limit = int(request.args.get("limit", 300))
    chats = await _dl.list_chats(limit=limit)
    return jsonify({"chats": chats})


@app.route("/api/topics")
async def api_topics():
    """List forum topics for a chat. Returns {"topics": []} if the chat isn't
    a forum supergroup (so the UI can hide the topic picker)."""
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    chat_id_str = request.args.get("chat")
    if not chat_id_str:
        return jsonify({"error": "chat parameter required"}), 400
    try:
        chat_id = int(chat_id_str)
    except ValueError:
        return jsonify({"error": "chat must be an integer"}), 400
    try:
        topics = await _dl.list_topics(chat_id)
    except Exception:
        # Not a forum, or no access — caller treats as "no topics".
        return jsonify({"topics": []})
    return jsonify({"topics": topics})


# ───── Routes: pairs ──────────────────────────────────────────────────────


@app.route("/api/pairs")
async def api_pairs_get():
    async with _state_lock:
        cfg = load_pairs() if _pairs_file_exists() else {"interval_seconds": 3600, "pairs": []}
        state = load_state()
    enriched = []
    for p in cfg.get("pairs", []):
        key = _pair_key(p)
        wm = state.get(key, {})
        enriched.append({**p, "watermark": wm.get("last_msg_id", 0), "updated_at": wm.get("updated_at", 0)})
    return jsonify({"interval_seconds": cfg.get("interval_seconds", 3600), "pairs": enriched})


def _pairs_file_exists() -> bool:
    from automate import _resolve_pairs_path
    return _resolve_pairs_path().exists()


@app.route("/api/pairs", methods=["POST"])
async def api_pairs_post():
    body = await request.get_json(force=True)
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400
    def _opt_int(v):
        if v in (None, "", "null"):
            return None
        return int(v)
    try:
        new_pair = {
            "name": name,
            "source": int(body["source"]),
            "dest": int(body["dest"]),
            "source_topic": _opt_int(body.get("source_topic")),
            "dest_topic": _opt_int(body.get("dest_topic")),
            "type": body.get("type", "all"),
            "delay_seconds": float(body.get("delay_seconds", 1.0)),
            "max_per_run": int(body.get("max_per_run", 200)),
        }
        # Only persist drop_author when caller actually sent it — otherwise
        # leave it absent so run_pair's default (True) applies.
        if "drop_author" in body:
            new_pair["drop_author"] = bool(body["drop_author"])
        # Drop nulls so JSON stays clean for users who don't use topics.
        new_pair = {k: v for k, v in new_pair.items() if v is not None}
    except (KeyError, ValueError, TypeError) as e:
        return jsonify({"error": f"invalid pair: {e}"}), 400

    if new_pair["type"] not in ("all", "media", "documents", "messages", "docs_and_text"):
        return jsonify({"error": "type must be one of all|media|documents|messages|docs_and_text"}), 400

    async with _state_lock:
        cfg = load_pairs() if _pairs_file_exists() else {"interval_seconds": 3600, "pairs": []}
        existing = next((p for p in cfg["pairs"] if p.get("name") == name), None)
        if existing:
            existing.update(new_pair)
            action = "updated"
        else:
            cfg.setdefault("pairs", []).append(new_pair)
            action = "added"
        if body.get("interval_seconds"):
            cfg["interval_seconds"] = int(body["interval_seconds"])
        save_pairs(cfg)
    _log_event({"kind": "pair_saved", "pair": name, "action": action})
    return jsonify({"ok": True, "action": action, "pair": new_pair})


@app.route("/api/pairs/<name>", methods=["DELETE"])
async def api_pair_delete(name: str):
    async with _state_lock:
        cfg = load_pairs() if _pairs_file_exists() else {"interval_seconds": 3600, "pairs": []}
        before = len(cfg.get("pairs", []))
        cfg["pairs"] = [p for p in cfg.get("pairs", []) if p.get("name") != name]
        save_pairs(cfg)
        removed = before - len(cfg["pairs"])
        # Don't delete watermark — keeping it means if you re-add the pair you don't re-forward history.
    _log_event({"kind": "pair_deleted", "pair": name, "removed": removed})
    return jsonify({"ok": True, "removed": removed})


@app.route("/api/pairs/<name>/watermark", methods=["POST"])
async def api_set_watermark(name: str):
    """Manually set a pair's watermark. Body: {"last_msg_id": int}.
    Used to repair clobbered watermarks (see 2026-05-19 race) or to skip ahead.
    """
    body = await request.get_json(force=True)
    wm = int(body.get("last_msg_id", 0))
    # Manual repair allowed to roll backwards (e.g., to re-forward a range).
    await save_pair_watermark(name, wm, int(time.time()), allow_regression=True)
    _log_event({"kind": "watermark_set", "pair": name, "wm": wm})
    return jsonify({"ok": True, "pair": name, "watermark": wm})


@app.route("/api/pairs/<name>/run", methods=["POST"])
async def api_pair_run(name: str):
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    if _paused:
        return jsonify({"error": "paused — POST /api/resume first"}), 409
    cfg = load_pairs() if _pairs_file_exists() else {"pairs": []}
    pair = next((p for p in cfg.get("pairs", []) if p.get("name") == name), None)
    if not pair:
        return jsonify({"error": f"pair '{name}' not found"}), 404

    async with _state_lock:
        state = load_state()
    job = _new_job(kind="pair-manual", label=f"pair:{name} (manual)")
    _log_event({"kind": "run_started", "pair": name, "trigger": "manual", "job_id": job["id"]})
    try:
        result = await run_pair(_dl, pair, state, job=job)
        # No save_state here — run_pair persists per-pair via save_pair_watermark.
        job["finished_at"] = int(time.time())
        _log_event({"kind": "run_finished", "pair": name, "result": result, "trigger": "manual", "job_id": job["id"]})
        return jsonify({"ok": True, "result": result, "job_id": job["id"]})
    except Exception as e:
        job.update({"status": "error", "finished_at": int(time.time())})
        _log_event({"kind": "run_error", "pair": name, "error": str(e), "trigger": "manual", "job_id": job["id"]})
        return jsonify({"error": str(e)}), 500


# ───── Routes: bulk run ──────────────────────────────────────────────────


_bulk_runs: dict[str, dict] = {}  # bulk_id -> {names, status, current, started_at, cancel}


async def _bulk_runner(bulk_id: str, names: list[str]) -> None:
    state_holder = {}  # filled in from disk per-pair
    bulk = _bulk_runs[bulk_id]
    for idx, name in enumerate(names, 1):
        if bulk.get("cancel") or _paused:
            bulk["status"] = "cancelled"
            break
        bulk["current"] = name
        bulk["index"] = idx
        cfg = load_pairs() if _pairs_file_exists() else {"pairs": []}
        pair = next((p for p in cfg.get("pairs", []) if p.get("name") == name), None)
        if not pair:
            _log_event({"kind": "bulk_skip", "pair": name, "reason": "not found", "bulk_id": bulk_id})
            continue
        async with _state_lock:
            state_holder["state"] = load_state()
        job = _new_job(kind="pair-bulk", label=f"pair:{name} (bulk {idx}/{len(names)})")
        _log_event({"kind": "run_started", "pair": name, "trigger": "bulk", "job_id": job["id"], "bulk_id": bulk_id})
        try:
            result = await run_pair(_dl, pair, state_holder["state"], job=job)
            # Per-pair watermark already persisted atomically inside run_pair
            # via save_pair_watermark. Don't save_state(full_dict) here — it
            # would write our stale snapshot over keys other runners updated.
            if job["status"] == "running":
                job["status"] = "finished"
            job["finished_at"] = int(time.time())
            _log_event({"kind": "run_finished", "pair": name, "result": result, "trigger": "bulk", "job_id": job["id"], "bulk_id": bulk_id})
        except Exception as e:
            job.update({"status": "error", "finished_at": int(time.time())})
            _log_event({"kind": "run_error", "pair": name, "error": str(e), "trigger": "bulk", "job_id": job["id"], "bulk_id": bulk_id})
        # Honor cancel between pairs without aborting mid-run.
        if bulk.get("cancel"):
            bulk["status"] = "cancelled"
            break
    if bulk["status"] == "running":
        bulk["status"] = "finished"
    bulk["finished_at"] = int(time.time())


@app.route("/api/run-all", methods=["POST"])
async def api_run_all():
    """Fire-and-forget serial backfill. Body: {"names": [...]} or {"prefix": "..."}.

    Returns immediately with the bulk_id and the queued names. Each pair runs
    one-at-a-time in a background task and appears as a separate job in
    /api/jobs so the existing job panel shows live progress. Cancel a single
    pair via /api/jobs/<id>/cancel; cancel the whole bulk via
    /api/run-all/<bulk_id>/cancel.
    """
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    if _paused:
        return jsonify({"error": "paused — POST /api/resume first"}), 409
    body = await request.get_json(silent=True) or {}
    cfg = load_pairs() if _pairs_file_exists() else {"pairs": []}
    all_names = [p.get("name") for p in cfg.get("pairs", []) if p.get("name")]
    names: list[str]
    if body.get("names"):
        wanted = set(body["names"])
        names = [n for n in all_names if n in wanted]
    elif body.get("prefix"):
        prefix = body["prefix"]
        names = [n for n in all_names if n.startswith(prefix)]
    else:
        names = list(all_names)
    if not names:
        return jsonify({"error": "no pairs matched"}), 400

    bulk_id = f"bulk-{int(time.time())}-{secrets.token_hex(3)}"
    _bulk_runs[bulk_id] = {
        "id": bulk_id, "names": names, "status": "running",
        "index": 0, "total": len(names), "current": None,
        "started_at": int(time.time()), "finished_at": None, "cancel": False,
    }
    asyncio.create_task(_bulk_runner(bulk_id, names))
    _log_event({"kind": "bulk_started", "bulk_id": bulk_id, "count": len(names)})
    return jsonify({"ok": True, "bulk_id": bulk_id, "queued": names})


@app.route("/api/run-all/<bulk_id>/cancel", methods=["POST"])
async def api_bulk_cancel(bulk_id: str):
    bulk = _bulk_runs.get(bulk_id)
    if not bulk:
        return jsonify({"error": "bulk run not found"}), 404
    bulk["cancel"] = True
    _log_event({"kind": "bulk_cancel_requested", "bulk_id": bulk_id})
    return jsonify({"ok": True, "bulk": {k: v for k, v in bulk.items() if k != "cancel"}})


@app.route("/api/run-all")
async def api_run_all_status():
    """List recent bulk runs (live + finished)."""
    runs = sorted(_bulk_runs.values(), key=lambda b: b["started_at"], reverse=True)[:20]
    return jsonify({"bulks": [{k: v for k, v in b.items() if k != "cancel"} for b in runs]})


# ───── Routes: pause / resume ───────────────────────────────────────────


@app.route("/api/pause", methods=["GET"])
async def api_pause_status():
    return jsonify({"paused": _paused})


@app.route("/api/pause", methods=["POST"])
async def api_pause():
    """Halt everything: cancel in-flight jobs + bulks, block scheduler from
    starting new pair runs. Until /api/resume is hit.
    """
    global _paused
    _paused = True
    cancelled_jobs = []
    for jid, job in _jobs.items():
        if job.get("status") in ("running", "queued"):
            job["cancel"] = True
            cancelled_jobs.append(jid)
    cancelled_bulks = []
    for bid, bulk in _bulk_runs.items():
        if bulk.get("status") == "running":
            bulk["cancel"] = True
            cancelled_bulks.append(bid)
    _log_event({"kind": "paused", "cancelled_jobs": cancelled_jobs, "cancelled_bulks": cancelled_bulks})
    return jsonify({"ok": True, "paused": True, "cancelled_jobs": cancelled_jobs, "cancelled_bulks": cancelled_bulks})


@app.route("/api/resume", methods=["POST"])
async def api_resume():
    global _paused
    was = _paused
    _paused = False
    _log_event({"kind": "resumed", "was_paused": was})
    return jsonify({"ok": True, "paused": False})


# ───── Routes: one-shot forward ──────────────────────────────────────────


@app.route("/api/forward-once", methods=["POST"])
async def api_forward_once():
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    if _paused:
        return jsonify({"error": "paused — POST /api/resume first"}), 409
    body = await request.get_json(force=True)
    def _opt_int(v):
        if v in (None, "", "null"):
            return None
        return int(v)
    try:
        source = int(body["source"])
        dest = int(body["dest"])
        source_topic = _opt_int(body.get("source_topic"))
        dest_topic = _opt_int(body.get("dest_topic"))
        ftype = body.get("type", "all")
        limit = int(body.get("limit", 50))
        delay = float(body.get("delay_seconds", 1.0))
    except (KeyError, ValueError, TypeError) as e:
        return jsonify({"error": f"invalid params: {e}"}), 400

    job = _new_job(kind="oneshot", label=f"oneshot {source}->{dest}", total=limit)
    _log_event({"kind": "oneshot_started", "source": source, "dest": dest, "type": ftype, "limit": limit, "job_id": job["id"]})

    iter_kwargs = {"limit": limit or None}
    if source_topic and source_topic > 1:
        iter_kwargs["reply_to"] = source_topic

    msgs = []
    async for m in _dl.client.iter_messages(source, **iter_kwargs):
        if job.get("cancel"):
            print(f"[oneshot {source}->{dest}] cancelled during scan ({len(msgs)} matched)")
            job["status"] = "cancelled"
            job["finished_at"] = int(time.time())
            return jsonify({"ok": True, "result": {
                "forwarded": 0, "failed": 0, "scanned": len(msgs), "cancelled": True,
            }, "job_id": job["id"]})
        if _matches_type(m, ftype, _dl):
            msgs.append(m)
    msgs.sort(key=lambda m: m.id)
    (BASE_DIR / "temp").mkdir(exist_ok=True)

    job["total"] = len(msgs)
    job["status"] = "running"
    ok = fail = 0
    try:
        for i, m in enumerate(msgs, 1):
            if job.get("cancel"):
                job["status"] = "cancelled"
                break
            success = await _dl._copy_message_to(m, dest, dest_topic=dest_topic)
            if success:
                ok += 1
            else:
                fail += 1
            job.update({"done": i, "ok": ok, "fail": fail, "last_id": m.id})
            await asyncio.sleep(delay)
        if job["status"] != "cancelled":
            job["status"] = "finished"
    finally:
        job["finished_at"] = int(time.time())
        try:
            import shutil
            shutil.rmtree(BASE_DIR / "temp", ignore_errors=True)
        except Exception:
            pass

    result = {"forwarded": ok, "failed": fail, "scanned": len(msgs), "cancelled": job["status"] == "cancelled"}
    _log_event({"kind": "oneshot_finished", "source": source, "dest": dest, "result": result, "job_id": job["id"]})
    return jsonify({"ok": True, "result": result, "job_id": job["id"]})


# ───── Routes: clone forum end-to-end ───────────────────────────────────


def _slug(s: str, fallback: str = "x", maxlen: int = 40) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "-", (s or "").lower()).strip("-")
    return (s or fallback)[:maxlen]


async def _clone_forum_impl(*, source_id: int, dest_title: str, skip_general: bool,
                            ftype: str, delay_seconds: float, max_per_run: int,
                            job: dict) -> dict:
    """Create dest forum, mirror topics, write pairs.json. Mutates `job` for progress."""
    # 1. Validate source — list_topics raises if not a forum / no access.
    src_topics = await _dl.list_topics(source_id)
    if not src_topics:
        raise ValueError(f"source chat {source_id} is not a forum supergroup, or has no topics")

    to_clone = [t for t in src_topics if not (skip_general and t["id"] == 1)]
    skipped = [t["title"] for t in src_topics if (skip_general and t["id"] == 1)]
    job["total"] = len(to_clone)

    # 2. Create destination supergroup with forum flag.
    create_result = await _dl.client(CreateChannelRequest(
        title=dest_title,
        about="",
        megagroup=True,
        forum=True,
    ))
    dest_channel = create_result.chats[0]
    dest_chat_id = tg_utils.get_peer_id(dest_channel)

    # If forum flag didn't take (older Telegram cores), toggle explicitly.
    if not getattr(dest_channel, "forum", False):
        try:
            await _dl.client(ToggleForumRequest(channel=dest_channel, enabled=True, tabs=False))
        except Exception as e:
            print(f"ToggleForum failed (may be benign): {e}", file=sys.stderr)

    dest_peer = await _dl.client.get_input_entity(dest_chat_id)

    # 3. Create matching topics, throttled to avoid FloodWait.
    for idx, st in enumerate(to_clone, 1):
        if job.get("cancel"):
            job["status"] = "cancelled"
            break
        try:
            await _dl.client(CreateForumTopicRequest(
                peer=dest_peer,
                title=st["title"],
                random_id=secrets.randbits(63),
            ))
            job["ok"] += 1
        except Exception as e:
            job["fail"] += 1
            print(f"failed to create topic {st['title']!r}: {e}", file=sys.stderr)
        job["done"] = idx
        await asyncio.sleep(0.5)  # gentle throttle

    # 4. List dest topics to resolve title -> id, then write pairs.
    new_topics = await _dl.list_topics(dest_chat_id)
    # First occurrence wins (Telegram permits duplicate titles; src shouldn't have any).
    title_to_id = {}
    for t in new_topics:
        title_to_id.setdefault(t["title"], t["id"])

    dest_slug = _slug(dest_title, fallback="clone")
    pairs_added = []
    pairs_missing = []

    async with _state_lock:
        cfg = load_pairs() if _pairs_file_exists() else {"interval_seconds": 3600, "pairs": []}
        existing_names = {p.get("name") for p in cfg.get("pairs", [])}

        for st in to_clone:
            dest_topic_id = title_to_id.get(st["title"])
            if not dest_topic_id:
                pairs_missing.append(st["title"])
                continue
            slug_fallback = f"topic-{st['id']}"
            base_name = f"{dest_slug}--{_slug(st['title'], fallback=slug_fallback)}"
            name = base_name
            n = 2
            while name in existing_names:
                name = f"{base_name}-{n}"
                n += 1
            existing_names.add(name)
            cfg.setdefault("pairs", []).append({
                "name": name,
                "source": source_id,
                "dest": dest_chat_id,
                "source_topic": st["id"],
                "dest_topic": dest_topic_id,
                "type": ftype,
                "delay_seconds": delay_seconds,
                "max_per_run": max_per_run,
            })
            pairs_added.append(name)

        save_pairs(cfg)

    return {
        "dest_chat_id": dest_chat_id,
        "dest_title": dest_title,
        "topics_attempted": len(to_clone),
        "topics_created": len(pairs_added),
        "pairs_added": pairs_added,
        "skipped_general": skipped,
        "pairs_missing": pairs_missing,  # source topics whose dest twin we couldn't resolve
    }


@app.route("/api/clone-forum", methods=["POST"])
async def api_clone_forum():
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    body = await request.get_json(force=True)
    try:
        source = int(body["source"])
        dest_title = (body.get("dest_title") or "").strip()
        if not dest_title:
            return jsonify({"error": "dest_title required"}), 400
        skip_general = bool(body.get("skip_general", True))
        ftype = body.get("type", "all")
        delay = float(body.get("delay_seconds", 1.0))
        max_per_run = int(body.get("max_per_run", 200))
    except (KeyError, ValueError, TypeError) as e:
        return jsonify({"error": f"invalid params: {e}"}), 400

    if ftype not in ("all", "media", "documents", "messages", "docs_and_text"):
        return jsonify({"error": "type must be one of all|media|documents|messages|docs_and_text"}), 400

    job = _new_job(kind="clone-forum", label=f"clone {source} -> {dest_title!r}")
    job["status"] = "running"
    _log_event({"kind": "clone_started", "source": source, "dest_title": dest_title, "job_id": job["id"]})
    try:
        result = await _clone_forum_impl(
            source_id=source, dest_title=dest_title, skip_general=skip_general,
            ftype=ftype, delay_seconds=delay, max_per_run=max_per_run, job=job,
        )
        if job["status"] != "cancelled":
            job["status"] = "finished"
        job["finished_at"] = int(time.time())
        _log_event({"kind": "clone_finished", "source": source, "result": result, "job_id": job["id"]})
        return jsonify({"ok": True, "result": result, "job_id": job["id"]})
    except Exception as e:
        job.update({"status": "error", "finished_at": int(time.time())})
        _log_event({"kind": "clone_error", "source": source, "error": str(e), "job_id": job["id"]})
        return jsonify({"error": str(e)}), 500


# ───── Routes: run log ───────────────────────────────────────────────────


@app.route("/api/runs")
async def api_runs():
    n = int(request.args.get("n", 50))
    return jsonify({"events": _run_log[-n:][::-1]})


@app.route("/api/jobs")
async def api_jobs():
    """All recent jobs (live + finished). Filter with ?status=running for just live."""
    status_filter = request.args.get("status")
    jobs = list(_jobs.values())
    if status_filter:
        jobs = [j for j in jobs if j["status"] == status_filter]
    jobs.sort(key=lambda j: j["started_at"], reverse=True)
    # Strip the internal `cancel` flag from the response.
    return jsonify({"jobs": [{k: v for k, v in j.items() if k != "cancel"} for j in jobs[:100]]})


@app.route("/api/jobs/<job_id>/cancel", methods=["POST"])
async def api_job_cancel(job_id: str):
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404
    if job["status"] not in ("running", "queued"):
        return jsonify({"error": f"job is {job['status']}, can't cancel"}), 400
    job["cancel"] = True
    _log_event({"kind": "job_cancel_requested", "job_id": job_id, "label": job["label"]})
    return jsonify({"ok": True, "job": {k: v for k, v in job.items() if k != "cancel"}})


# ───── Background scheduler ──────────────────────────────────────────────


async def _scheduler_loop():
    while True:
        try:
            cfg = load_pairs() if _pairs_file_exists() else {"interval_seconds": 3600, "pairs": []}
            interval = max(60, int(cfg.get("interval_seconds", 3600)))
            for pair in cfg.get("pairs", []):
                if _paused:
                    break
                name = _pair_key(pair)
                job = _new_job(kind="pair-scheduled", label=f"pair:{name} (scheduled)")
                try:
                    async with _state_lock:
                        state = load_state()
                    _log_event({"kind": "run_started", "pair": name, "trigger": "scheduler", "job_id": job["id"]})
                    result = await run_pair(_dl, pair, state, job=job)
                    # No save_state here — see _bulk_runner comment.
                    job["finished_at"] = int(time.time())
                    _log_event({"kind": "run_finished", "pair": name, "result": result, "trigger": "scheduler", "job_id": job["id"]})
                except Exception as e:
                    job.update({"status": "error", "finished_at": int(time.time())})
                    print(f"scheduler error for {name}: {e}", file=sys.stderr)
                    _log_event({"kind": "run_error", "pair": name, "error": str(e), "trigger": "scheduler", "job_id": job["id"]})
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"scheduler loop crashed: {e}", file=sys.stderr)
            await asyncio.sleep(60)


# ───── Lifecycle ──────────────────────────────────────────────────────────


@app.before_serving
async def _startup():
    global _dl, _scheduler_task
    _load_run_log()
    # Trigger one-time pairs.json seeding from PAIRS_JSON env var if applicable.
    # Without this, a fresh volume + env var seed never lands on disk because
    # downstream handlers short-circuit on _pairs_file_exists().
    try:
        load_pairs()
    except FileNotFoundError:
        print("no pairs configured yet — add some via the web UI")
    _dl = TelegramDownloader(load_config())
    await _dl.start()
    _scheduler_task = asyncio.create_task(_scheduler_loop())
    print(f"server ready — dash_user={DASH_USER!r} auth={'on' if DASH_PASS else 'OFF (set DASH_PASS)'}")


@app.after_serving
async def _shutdown():
    if _scheduler_task:
        _scheduler_task.cancel()
    if _dl:
        await _dl.stop()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)

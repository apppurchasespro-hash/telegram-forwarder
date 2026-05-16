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
import secrets
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from quart import Quart, request, jsonify, render_template, Response

from downloader import TelegramDownloader, load_config
from automate import (
    BASE_DIR,
    load_pairs,
    save_pairs,
    load_state,
    save_state,
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
    try:
        new_pair = {
            "name": name,
            "source": int(body["source"]),
            "dest": int(body["dest"]),
            "type": body.get("type", "all"),
            "delay_seconds": float(body.get("delay_seconds", 1.0)),
            "max_per_run": int(body.get("max_per_run", 200)),
        }
    except (KeyError, ValueError, TypeError) as e:
        return jsonify({"error": f"invalid pair: {e}"}), 400

    if new_pair["type"] not in ("all", "media", "documents", "messages"):
        return jsonify({"error": "type must be one of all|media|documents|messages"}), 400

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


@app.route("/api/pairs/<name>/run", methods=["POST"])
async def api_pair_run(name: str):
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    cfg = load_pairs() if _pairs_file_exists() else {"pairs": []}
    pair = next((p for p in cfg.get("pairs", []) if p.get("name") == name), None)
    if not pair:
        return jsonify({"error": f"pair '{name}' not found"}), 404

    async with _state_lock:
        state = load_state()
    _log_event({"kind": "run_started", "pair": name, "trigger": "manual"})
    try:
        result = await run_pair(_dl, pair, state)
        async with _state_lock:
            save_state(state)
        _log_event({"kind": "run_finished", "pair": name, "result": result, "trigger": "manual"})
        return jsonify({"ok": True, "result": result})
    except Exception as e:
        _log_event({"kind": "run_error", "pair": name, "error": str(e), "trigger": "manual"})
        return jsonify({"error": str(e)}), 500


# ───── Routes: one-shot forward ──────────────────────────────────────────


@app.route("/api/forward-once", methods=["POST"])
async def api_forward_once():
    if not _dl:
        return jsonify({"error": "telegram client not ready"}), 503
    body = await request.get_json(force=True)
    try:
        source = int(body["source"])
        dest = int(body["dest"])
        ftype = body.get("type", "all")
        limit = int(body.get("limit", 50))
        delay = float(body.get("delay_seconds", 1.0))
    except (KeyError, ValueError, TypeError) as e:
        return jsonify({"error": f"invalid params: {e}"}), 400

    _log_event({"kind": "oneshot_started", "source": source, "dest": dest, "type": ftype, "limit": limit})

    msgs = []
    async for m in _dl.client.iter_messages(source, limit=limit or None):
        if _matches_type(m, ftype, _dl):
            msgs.append(m)
    msgs.sort(key=lambda m: m.id)
    (BASE_DIR / "temp").mkdir(exist_ok=True)

    ok = fail = 0
    try:
        for m in msgs:
            success = await _dl._copy_message_to(m, dest)
            if success:
                ok += 1
            else:
                fail += 1
            await asyncio.sleep(delay)
    finally:
        try:
            import shutil
            shutil.rmtree(BASE_DIR / "temp", ignore_errors=True)
        except Exception:
            pass

    result = {"forwarded": ok, "failed": fail, "scanned": len(msgs)}
    _log_event({"kind": "oneshot_finished", "source": source, "dest": dest, "result": result})
    return jsonify({"ok": True, "result": result})


# ───── Routes: run log ───────────────────────────────────────────────────


@app.route("/api/runs")
async def api_runs():
    n = int(request.args.get("n", 50))
    return jsonify({"events": _run_log[-n:][::-1]})


# ───── Background scheduler ──────────────────────────────────────────────


async def _scheduler_loop():
    while True:
        try:
            cfg = load_pairs() if _pairs_file_exists() else {"interval_seconds": 3600, "pairs": []}
            interval = max(60, int(cfg.get("interval_seconds", 3600)))
            for pair in cfg.get("pairs", []):
                name = _pair_key(pair)
                try:
                    async with _state_lock:
                        state = load_state()
                    _log_event({"kind": "run_started", "pair": name, "trigger": "scheduler"})
                    result = await run_pair(_dl, pair, state)
                    async with _state_lock:
                        save_state(state)
                    _log_event({"kind": "run_finished", "pair": name, "result": result, "trigger": "scheduler"})
                except Exception as e:
                    print(f"scheduler error for {name}: {e}", file=sys.stderr)
                    _log_event({"kind": "run_error", "pair": name, "error": str(e), "trigger": "scheduler"})
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

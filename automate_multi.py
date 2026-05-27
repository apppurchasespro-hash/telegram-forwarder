"""Multi-topic backfill: scan one source channel, dedupe against a destination
supergroup (across all topics), forward missing files into per-file topics
chosen by regex rules on the filename.

Phases:
    1. index-source : iter_messages(source, reverse=True) → SQLite src_msgs
    2. index-dest   : iter_messages(dest) across all topics → SQLite dst_msgs
    3. plan         : SQL diff (filename + file_size match) → {topic_id: [src_ids]}
    4. forward      : per-topic batches of 100, native server-side forward,
                      results recorded in message_map for live edit/delete.

The SQLite DB lives at {STATE_DIR}/multi_forward_{abs(source_id)}.db, where
STATE_DIR is the directory of STATE_PATH (the watermarks file). Same DB is
reusable across re-runs — index phase is idempotent (INSERT OR REPLACE) and
skip-resumable (uses max(msg_id) as min_id).

Drives are coordinated via a "job" dict (the same shape the dashboard uses
for /api/jobs) so the existing UI/job tracker can report progress.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

from telethon.errors import FloodWaitError

from automate import record_mappings


# ── Paths ─────────────────────────────────────────────────────────────────
def _state_dir() -> Path:
    p = Path(os.environ.get("STATE_PATH", "watermarks.json"))
    return p.parent


def db_path_for(source_id: int) -> Path:
    return _state_dir() / f"multi_forward_{abs(source_id)}.db"


# ── DB schema ─────────────────────────────────────────────────────────────
def setup_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS src_msgs (
            msg_id INTEGER PRIMARY KEY,
            date INTEGER,
            kind TEXT,
            file_name TEXT,
            file_size INTEGER,
            classified_topic INTEGER
        );
        CREATE TABLE IF NOT EXISTS dst_msgs (
            msg_id INTEGER PRIMARY KEY,
            file_name TEXT,
            file_size INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_dst_fnsize ON dst_msgs(file_name, file_size);
        CREATE TABLE IF NOT EXISTS forward_log (
            src_msg_id INTEGER PRIMARY KEY,
            dst_msg_id INTEGER,
            topic_id INTEGER,
            forwarded_at INTEGER
        );
        """
    )
    conn.commit()
    return conn


# ── Fingerprinting + classification ───────────────────────────────────────
def fingerprint(msg) -> tuple[str, Optional[str], int]:
    """Return (kind, filename, file_size_bytes)."""
    if getattr(msg, "document", None):
        for a in msg.document.attributes:
            if hasattr(a, "file_name") and a.file_name:
                return ("doc", a.file_name, int(msg.document.size or 0))
        return ("doc", None, int(msg.document.size or 0))
    if getattr(msg, "photo", None):
        return ("photo", None, 0)
    if getattr(msg, "video", None):
        return ("video", None, int(getattr(msg.video, "size", 0) or 0))
    if getattr(msg, "message", None):
        return ("text", None, 0)
    return ("other", None, 0)


def compile_rules(rules: list[dict]) -> list[dict]:
    out = []
    for r in rules:
        try:
            pat = re.compile(r["pattern"], re.IGNORECASE)
        except re.error as e:
            raise ValueError(f"bad regex {r['pattern']!r}: {e}")
        out.append({"pattern": pat, "topic": int(r["topic"]), "label": r.get("label", "")})
    return out


def classify(filename: Optional[str], rules: list[dict], default_topic: int) -> int:
    if not filename:
        return default_topic
    for rule in rules:
        if rule["pattern"].search(filename):
            return rule["topic"]
    return default_topic


# ── Indexing ──────────────────────────────────────────────────────────────
async def index_source(
    dl, source_id: int, conn: sqlite3.Connection, rules: list[dict],
    default_topic: int, job: dict, batch_size: int = 500,
) -> int:
    """Stream all source messages into src_msgs. Resumable: skip ids ≤ max we have."""
    cur = conn.cursor()
    max_indexed = cur.execute("SELECT COALESCE(MAX(msg_id), 0) FROM src_msgs").fetchone()[0]
    job["phase"] = "index-source"
    job["src_indexed_before"] = max_indexed
    job["src_indexed"] = 0
    kwargs: dict[str, Any] = {"reverse": True}
    if max_indexed > 0:
        kwargs["min_id"] = max_indexed
    count = 0
    batch: list[tuple] = []
    async for m in dl.client.iter_messages(source_id, **kwargs):
        if job.get("cancel"):
            break
        kind, fn, sz = fingerprint(m)
        topic = classify(fn, rules, default_topic)
        batch.append((m.id, int(m.date.timestamp()), kind, fn, sz, topic))
        count += 1
        if len(batch) >= batch_size:
            cur.executemany(
                "INSERT OR REPLACE INTO src_msgs VALUES (?,?,?,?,?,?)", batch
            )
            conn.commit()
            job["src_indexed"] = count
            batch.clear()
    if batch:
        cur.executemany("INSERT OR REPLACE INTO src_msgs VALUES (?,?,?,?,?,?)", batch)
        conn.commit()
    job["src_indexed"] = count
    return count


async def index_dest(
    dl, dest_id: int, conn: sqlite3.Connection, job: dict, batch_size: int = 500,
) -> int:
    """Stream all dest messages (every topic) into dst_msgs for dedup."""
    cur = conn.cursor()
    max_indexed = cur.execute("SELECT COALESCE(MAX(msg_id), 0) FROM dst_msgs").fetchone()[0]
    job["phase"] = "index-dest"
    job["dst_indexed_before"] = max_indexed
    job["dst_indexed"] = 0
    kwargs: dict[str, Any] = {"reverse": True}
    if max_indexed > 0:
        kwargs["min_id"] = max_indexed
    count = 0
    batch: list[tuple] = []
    async for m in dl.client.iter_messages(dest_id, **kwargs):
        if job.get("cancel"):
            break
        kind, fn, sz = fingerprint(m)
        if kind != "doc" or not fn:
            continue
        batch.append((m.id, fn, sz))
        count += 1
        if len(batch) >= batch_size:
            cur.executemany("INSERT OR REPLACE INTO dst_msgs VALUES (?,?,?)", batch)
            conn.commit()
            job["dst_indexed"] = count
            batch.clear()
    if batch:
        cur.executemany("INSERT OR REPLACE INTO dst_msgs VALUES (?,?,?)", batch)
        conn.commit()
    job["dst_indexed"] = count
    return count


# ── Plan computation ──────────────────────────────────────────────────────
def compute_plan(conn: sqlite3.Connection) -> dict[int, list[int]]:
    """Return {topic_id: [src_msg_id ASC]} for files in source missing from dest.
    Dedup by (file_name, file_size). Only considers 'doc' kind with size > 0.
    """
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT s.msg_id, s.classified_topic
        FROM src_msgs s
        WHERE s.kind='doc' AND s.file_size > 0 AND s.file_name IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM dst_msgs d
              WHERE d.file_name = s.file_name AND d.file_size = s.file_size
          )
          AND NOT EXISTS (
              SELECT 1 FROM forward_log f WHERE f.src_msg_id = s.msg_id
          )
        ORDER BY s.msg_id ASC
        """
    ).fetchall()
    plan: dict[int, list[int]] = {}
    for msg_id, topic in rows:
        plan.setdefault(topic, []).append(msg_id)
    return plan


def plan_summary(conn: sqlite3.Connection, plan: dict[int, list[int]]) -> dict:
    """Counts useful for the dry-run report."""
    cur = conn.cursor()
    src_total = cur.execute("SELECT COUNT(*) FROM src_msgs").fetchone()[0]
    src_docs = cur.execute(
        "SELECT COUNT(*) FROM src_msgs WHERE kind='doc' AND file_size>0 AND file_name IS NOT NULL"
    ).fetchone()[0]
    dst_total = cur.execute("SELECT COUNT(*) FROM dst_msgs").fetchone()[0]
    matched = cur.execute(
        """
        SELECT COUNT(*) FROM src_msgs s
        WHERE s.kind='doc' AND s.file_size>0 AND s.file_name IS NOT NULL
          AND EXISTS (SELECT 1 FROM dst_msgs d WHERE d.file_name=s.file_name AND d.file_size=s.file_size)
        """
    ).fetchone()[0]
    return {
        "src_total": src_total,
        "src_docs": src_docs,
        "dst_indexed": dst_total,
        "already_in_dest": matched,
        "to_forward": sum(len(v) for v in plan.values()),
        "by_topic": {str(k): len(v) for k, v in plan.items()},
    }


# ── Forwarding ────────────────────────────────────────────────────────────
async def forward_plan(
    dl, source_id: int, dest_id: int, plan: dict[int, list[int]],
    conn: sqlite3.Connection, job: dict, pair_name: str,
    drop_author: bool = True, inter_batch_delay: float = 0.5,
) -> None:
    """Forward each topic's batch via dl.forward_batch (native server-side).
    Records (src_id → dst_id) into message_map and forward_log."""
    job["phase"] = "forward"
    total = sum(len(v) for v in plan.values())
    job["total"] = total
    job["done"] = 0
    job["ok"] = 0
    job["fail"] = 0
    BATCH = 100
    for topic_id, msg_ids in plan.items():
        job[f"topic_{topic_id}_pending"] = len(msg_ids)
        for i in range(0, len(msg_ids), BATCH):
            if job.get("cancel"):
                job["status"] = "cancelled"
                return
            batch_ids = msg_ids[i : i + BATCH]
            # Fetch the actual Message objects (forward_batch needs them, not ids).
            try:
                batch_msgs = await dl.client.get_messages(source_id, ids=batch_ids)
                batch_msgs = [m for m in batch_msgs if m is not None]
            except FloodWaitError as fw:
                await asyncio.sleep(fw.seconds + 1)
                batch_msgs = await dl.client.get_messages(source_id, ids=batch_ids)
                batch_msgs = [m for m in batch_msgs if m is not None]
            if not batch_msgs:
                job["fail"] += len(batch_ids)
                job["done"] += len(batch_ids)
                continue
            top_msg = topic_id if topic_id and topic_id > 1 else None
            try:
                forwarded = await dl.forward_batch(
                    source_id, dest_id, batch_msgs,
                    drop_author=drop_author, top_msg_id=top_msg,
                )
            except FloodWaitError as fw:
                await asyncio.sleep(fw.seconds + 1)
                try:
                    forwarded = await dl.forward_batch(
                        source_id, dest_id, batch_msgs,
                        drop_author=drop_author, top_msg_id=top_msg,
                    )
                except Exception as e:
                    print(f"[multi] topic {topic_id} batch failed post-flood: {e}")
                    job["fail"] += len(batch_msgs)
                    job["done"] += len(batch_msgs)
                    continue
            except Exception as e:
                print(f"[multi] topic {topic_id} batch failed: {e}")
                job["fail"] += len(batch_msgs)
                job["done"] += len(batch_msgs)
                continue
            # Record successes
            mappings: list[tuple[int, int]] = []
            log_rows: list[tuple] = []
            now = int(time.time())
            for src_msg, fwd in zip(batch_msgs, forwarded):
                if fwd is not None:
                    dst_id = getattr(fwd, "id", None)
                    if dst_id is not None:
                        mappings.append((src_msg.id, dst_id))
                        log_rows.append((src_msg.id, dst_id, topic_id, now))
                        job["ok"] += 1
                    else:
                        job["fail"] += 1
                else:
                    job["fail"] += 1
                job["done"] += 1
            if mappings:
                await record_mappings(pair_name, mappings)
            if log_rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO forward_log VALUES (?,?,?,?)", log_rows
                )
                conn.commit()
            # Tiny breather between batches so we don't spike FloodWait.
            await asyncio.sleep(inter_batch_delay)


# ── Orchestrator ──────────────────────────────────────────────────────────
async def run_multi_forward(dl, params: dict, job: dict) -> dict:
    """Drive index → diff → (optional) forward. params keys:
        source        (int, required) — source chat id
        dest          (int, required) — destination chat id
        rules         (list of {pattern: str, topic: int, label: str}, required)
        default_topic (int, required) — fallback topic
        dry_run       (bool, default False) — index + plan only, no forward
        skip_dest_index (bool, default False) — re-use existing dst_msgs
        pair_name     (str, optional) — for message_map key, default "multi-{source}"
        drop_author   (bool, default True)
    """
    source_id = int(params["source"])
    dest_id = int(params["dest"])
    rules = compile_rules(params["rules"])
    default_topic = int(params["default_topic"])
    dry_run = bool(params.get("dry_run", False))
    skip_dest_index = bool(params.get("skip_dest_index", False))
    pair_name = params.get("pair_name") or f"multi-{abs(source_id)}"
    drop_author = bool(params.get("drop_author", True))

    db = db_path_for(source_id)
    job["db_path"] = str(db)
    conn = setup_db(db)
    job["status"] = "running"

    try:
        await index_source(dl, source_id, conn, rules, default_topic, job)
        if not skip_dest_index:
            await index_dest(dl, dest_id, conn, job)
        job["phase"] = "plan"
        plan = compute_plan(conn)
        summary = plan_summary(conn, plan)
        job["summary"] = summary
        print(f"[multi-fwd] plan summary: {json.dumps(summary)}")
        if dry_run:
            job["phase"] = "dry-run-done"
            job["status"] = "finished"
            return summary
        if not plan:
            job["phase"] = "nothing-to-forward"
            job["status"] = "finished"
            return summary
        await forward_plan(
            dl, source_id, dest_id, plan, conn, job, pair_name, drop_author=drop_author
        )
        if job.get("status") not in ("cancelled",):
            job["status"] = "finished"
        return job.get("summary") or summary
    finally:
        conn.close()
        job["finished_at"] = int(time.time())

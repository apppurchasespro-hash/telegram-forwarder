"""Compare a pair's source and destination message-by-message.

Fetches full message lists from both sides via Telethon, stores in SQLite,
then runs analysis queries. Useful for diagnosing pair config mistakes
(e.g., dest already populated from a different source).

Usage:
    py -3.11 scripts/compare_pair.py --src -1002051910833 --dst -1003776591963 \
        --dst-topic 11 --db data_acct2/compare_combined.db
"""
from __future__ import annotations
import argparse
import asyncio
import json
import sqlite3
import sys
from pathlib import Path

# Force stdout to utf-8 on Windows so unicode channel names print without errors.
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from telethon import TelegramClient
from telethon.sessions import StringSession

ROOT = Path(__file__).resolve().parent.parent


def fingerprint(msg):
    """Return (kind, file_name, file_size, caption_preview) for a message."""
    caption = (msg.message or "")[:80].replace("\n", "/")
    if msg.document:
        name = "<no-name>"
        for a in msg.document.attributes:
            if hasattr(a, "file_name") and a.file_name:
                name = a.file_name
                break
        return ("doc", name, int(msg.document.size or 0), caption)
    if msg.photo:
        return ("photo", "", 0, caption)
    if msg.message:
        return ("text", "", 0, caption)
    return ("service", "", 0, caption)


def setup_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS src_msgs (
            channel_id INTEGER NOT NULL,
            msg_id INTEGER NOT NULL,
            date INTEGER,
            kind TEXT NOT NULL,
            file_name TEXT,
            file_size INTEGER,
            caption_preview TEXT,
            PRIMARY KEY (channel_id, msg_id)
        );
        CREATE TABLE IF NOT EXISTS dst_msgs (
            channel_id INTEGER NOT NULL,
            topic_id INTEGER,
            msg_id INTEGER NOT NULL,
            date INTEGER,
            kind TEXT NOT NULL,
            file_name TEXT,
            file_size INTEGER,
            caption_preview TEXT,
            PRIMARY KEY (channel_id, topic_id, msg_id)
        );
        CREATE INDEX IF NOT EXISTS idx_src_fname ON src_msgs(file_name, file_size);
        CREATE INDEX IF NOT EXISTS idx_dst_fname ON dst_msgs(file_name, file_size);
    """)
    conn.commit()
    return conn


async def fetch_side(client, conn, table, *, channel_id, topic_id=None, label, limit=None):
    """Stream messages from a chat (optionally a forum topic) into the DB."""
    kwargs = {"limit": limit}
    if topic_id and topic_id > 1:
        kwargs["reply_to"] = topic_id
    print(f"[{label}] fetching from {channel_id}" + (f" topic {topic_id}" if topic_id else ""))
    batch = []
    count = 0
    last_id = 0
    async for m in client.iter_messages(channel_id, **kwargs):
        kind, fname, fsize, caption = fingerprint(m)
        if table == "src_msgs":
            batch.append((channel_id, m.id, int(m.date.timestamp()), kind, fname, fsize, caption))
        else:
            batch.append((channel_id, topic_id or 0, m.id, int(m.date.timestamp()), kind, fname, fsize, caption))
        count += 1
        last_id = m.id
        if len(batch) >= 500:
            if table == "src_msgs":
                conn.executemany("INSERT OR REPLACE INTO src_msgs VALUES (?,?,?,?,?,?,?)", batch)
            else:
                conn.executemany("INSERT OR REPLACE INTO dst_msgs VALUES (?,?,?,?,?,?,?,?)", batch)
            conn.commit()
            print(f"[{label}] {count:,} fetched (last id #{last_id})")
            batch.clear()
    if batch:
        if table == "src_msgs":
            conn.executemany("INSERT OR REPLACE INTO src_msgs VALUES (?,?,?,?,?,?,?)", batch)
        else:
            conn.executemany("INSERT OR REPLACE INTO dst_msgs VALUES (?,?,?,?,?,?,?,?)", batch)
        conn.commit()
    print(f"[{label}] done — {count:,} messages stored")
    return count


def analyze(conn, src_id, dst_id, dst_topic):
    print("\n" + "=" * 70)
    print("ANALYSIS")
    print("=" * 70)
    cur = conn.cursor()

    # Counts by kind
    print("\n--- SOURCE breakdown ---")
    for kind, n in cur.execute("SELECT kind, COUNT(*) FROM src_msgs WHERE channel_id=? GROUP BY kind ORDER BY 2 DESC", (src_id,)):
        print(f"  {kind:<8} {n:>6,}")
    print("\n--- DEST breakdown ---")
    for kind, n in cur.execute("SELECT kind, COUNT(*) FROM dst_msgs WHERE channel_id=? AND topic_id=? GROUP BY kind ORDER BY 2 DESC", (dst_id, dst_topic)):
        print(f"  {kind:<8} {n:>6,}")

    # Media: files in dest but missing from source (orphans — wrong-source fwd)
    print("\n--- Files in DEST topic but NOT in SOURCE channel ---")
    print("    (these were forwarded from somewhere else, or live in dest only)")
    orphans = cur.execute("""
        SELECT d.msg_id, d.file_name, d.file_size
        FROM dst_msgs d
        WHERE d.channel_id=? AND d.topic_id=? AND d.kind='doc' AND d.file_size > 0
        AND NOT EXISTS (
            SELECT 1 FROM src_msgs s
            WHERE s.channel_id=? AND s.file_name=d.file_name AND s.file_size=d.file_size
        )
        ORDER BY d.msg_id DESC
    """, (dst_id, dst_topic, src_id)).fetchall()
    print(f"  total orphans (in dest, not in source): {len(orphans):,}")
    if orphans:
        print(f"  sample (newest 10):")
        for mid, fn, sz in orphans[:10]:
            print(f"    dst#{mid}  [{sz:>13,} B]  {fn[:70]}")

    # Media: files in source but not yet in dest (pending forwards)
    print("\n--- Files in SOURCE but NOT in DEST topic ---")
    print("    (these still need to be forwarded)")
    pending = cur.execute("""
        SELECT s.msg_id, s.file_name, s.file_size
        FROM src_msgs s
        WHERE s.channel_id=? AND s.kind='doc' AND s.file_size > 0
        AND NOT EXISTS (
            SELECT 1 FROM dst_msgs d
            WHERE d.channel_id=? AND d.topic_id=? AND d.file_name=s.file_name AND d.file_size=s.file_size
        )
        ORDER BY s.msg_id DESC
    """, (src_id, dst_id, dst_topic)).fetchall()
    print(f"  total pending (in source, not in dest): {len(pending):,}")
    if pending:
        print(f"  sample (newest 10):")
        for mid, fn, sz in pending[:10]:
            print(f"    src#{mid}  [{sz:>13,} B]  {fn[:70]}")

    # Successful matches
    matches = cur.execute("""
        SELECT COUNT(DISTINCT s.msg_id)
        FROM src_msgs s
        WHERE s.channel_id=? AND s.kind='doc' AND s.file_size > 0
        AND EXISTS (
            SELECT 1 FROM dst_msgs d
            WHERE d.channel_id=? AND d.topic_id=? AND d.file_name=s.file_name AND d.file_size=s.file_size
        )
    """, (src_id, dst_id, dst_topic)).fetchone()[0]
    print(f"\n--- Match summary (by file_name + file_size) ---")
    print(f"  source docs matched in dest: {matches:,}")
    src_doc_count = cur.execute("SELECT COUNT(*) FROM src_msgs WHERE channel_id=? AND kind='doc' AND file_size > 0", (src_id,)).fetchone()[0]
    print(f"  source docs total:           {src_doc_count:,}")
    if src_doc_count:
        print(f"  coverage:                    {100.0 * matches / src_doc_count:.1f}%")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", type=int, required=True)
    ap.add_argument("--dst", type=int, required=True)
    ap.add_argument("--dst-topic", type=int, default=None)
    ap.add_argument("--db", required=True)
    ap.add_argument("--limit", type=int, default=None, help="cap per side (debug)")
    args = ap.parse_args()

    cfg = json.loads((ROOT / "config.json").read_text())
    sess = (ROOT / "secrets" / "acct2_session_string.txt").read_text().strip()
    client = TelegramClient(StringSession(sess), cfg["api_id"], cfg["api_hash"])
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: session not authorized", file=sys.stderr)
        return 1

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = setup_db(db_path)

    await fetch_side(client, conn, "src_msgs", channel_id=args.src, label="src", limit=args.limit)
    await fetch_side(client, conn, "dst_msgs", channel_id=args.dst, topic_id=args.dst_topic, label="dst", limit=args.limit)
    await client.disconnect()

    analyze(conn, args.src, args.dst, args.dst_topic or 0)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

"""SQLite + FTS5 catalog.

Everything the rest of the package touches goes through ``Catalog`` so the
storage engine is swappable (e.g. MongoDB) without changing the bot/indexer.

Schema:
  files       — one row per servable message in the storage channel.
  files_fts   — FTS5 external-content index over (title, file_name, caption),
                kept in sync with triggers; ``rowid`` == ``files.message_id``.
  users       — every user who touched the bot (the audience asset).
  meta         — key/value, e.g. the indexer high-water mark.
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Iterable, Optional


_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    message_id INTEGER PRIMARY KEY,
    title      TEXT NOT NULL,
    year       INTEGER,
    quality    TEXT,
    language   TEXT,
    size       INTEGER,
    file_name  TEXT,
    caption    TEXT,
    mime       TEXT,
    added_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
    title, file_name, caption,
    content='files', content_rowid='message_id'
);

CREATE TRIGGER IF NOT EXISTS files_ai AFTER INSERT ON files BEGIN
    INSERT INTO files_fts(rowid, title, file_name, caption)
    VALUES (new.message_id, new.title, new.file_name, new.caption);
END;

CREATE TRIGGER IF NOT EXISTS files_ad AFTER DELETE ON files BEGIN
    INSERT INTO files_fts(files_fts, rowid, title, file_name, caption)
    VALUES ('delete', old.message_id, old.title, old.file_name, old.caption);
END;

CREATE TRIGGER IF NOT EXISTS files_au AFTER UPDATE ON files BEGIN
    INSERT INTO files_fts(files_fts, rowid, title, file_name, caption)
    VALUES ('delete', old.message_id, old.title, old.file_name, old.caption);
    INSERT INTO files_fts(rowid, title, file_name, caption)
    VALUES (new.message_id, new.title, new.file_name, new.caption);
END;

CREATE TABLE IF NOT EXISTS users (
    user_id    INTEGER PRIMARY KEY,
    username   TEXT,
    language   TEXT,
    first_seen TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

# Keep only word characters and spaces, then turn each token into a prefix
# query. This makes arbitrary user input safe for FTS5 MATCH.
_TOKEN_RE = re.compile(r"[^\w\s]", re.UNICODE)


def _fts_query(text: str) -> str:
    cleaned = _TOKEN_RE.sub(" ", text or "")
    tokens = [t for t in cleaned.split() if t]
    if not tokens:
        return ""
    # implicit AND of prefix matches: `incep* 2010*`
    return " ".join(f"{t}*" for t in tokens)


class Catalog:
    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(_SCHEMA)
        self.conn.commit()

    # ---- catalog writes (indexer) ----------------------------------------
    def upsert_file(
        self,
        message_id: int,
        title: str,
        year: Optional[int],
        quality: Optional[str],
        language: Optional[str],
        size: Optional[int],
        file_name: Optional[str],
        caption: Optional[str],
        mime: Optional[str],
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO files
                (message_id, title, year, quality, language, size, file_name, caption, mime)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(message_id) DO UPDATE SET
                title=excluded.title, year=excluded.year, quality=excluded.quality,
                language=excluded.language, size=excluded.size, file_name=excluded.file_name,
                caption=excluded.caption, mime=excluded.mime
            """,
            (message_id, title, year, quality, language, size, file_name, caption, mime),
        )
        self.conn.commit()

    def delete_file(self, message_id: int) -> None:
        self.conn.execute("DELETE FROM files WHERE message_id=?", (message_id,))
        self.conn.commit()

    # ---- catalog reads (bot) ---------------------------------------------
    def search(self, text: str, limit: int = 10) -> list[sqlite3.Row]:
        q = _fts_query(text)
        if not q:
            return []
        rows = self.conn.execute(
            """
            SELECT f.* FROM files_fts
            JOIN files f ON f.message_id = files_fts.rowid
            WHERE files_fts MATCH ?
            ORDER BY rank
            LIMIT ?
            """,
            (q, limit),
        ).fetchall()
        return rows

    def get(self, message_id: int) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM files WHERE message_id=?", (message_id,)
        ).fetchone()

    def file_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]

    # ---- users (audience) -------------------------------------------------
    def add_user(self, user_id: int, username: Optional[str], language: Optional[str]) -> None:
        self.conn.execute(
            """INSERT INTO users (user_id, username, language) VALUES (?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET username=excluded.username""",
            (user_id, username, language),
        )
        self.conn.commit()

    def user_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]

    def all_user_ids(self) -> Iterable[int]:
        for row in self.conn.execute("SELECT user_id FROM users"):
            yield row[0]

    # ---- meta -------------------------------------------------------------
    def get_meta(self, key: str) -> Optional[str]:
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

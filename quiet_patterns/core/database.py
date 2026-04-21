"""
core/database.py – SQLite schema initialization and helper utilities.
All tables are created idempotently; safe to call on each startup.
"""

import sqlite3
import logging
from pathlib import Path
from typing import Any

import sys, os
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_PATH

logger = logging.getLogger(__name__)

# ─── Schema ──────────────────────────────────────────────────────────────────
_SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS scripts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    content_hash    TEXT    NOT NULL UNIQUE,
    hook            TEXT    NOT NULL,
    body            TEXT    NOT NULL,
    closing         TEXT    NOT NULL,
    theme           TEXT,
    full_text       TEXT    NOT NULL,
    word_count      INTEGER NOT NULL,
    novelty_score   REAL    NOT NULL DEFAULT 0.0,
    viral_score     REAL    NOT NULL,
    retention_pct   REAL    NOT NULL,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
    used            INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS videos (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    script_id       INTEGER NOT NULL REFERENCES scripts(id),
    file_path       TEXT    NOT NULL,
    duration_sec    REAL    NOT NULL,
    rendered_at     TEXT    NOT NULL DEFAULT (datetime('now')),
    uploaded        INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS uploads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id        INTEGER NOT NULL REFERENCES videos(id),
    youtube_id      TEXT    NOT NULL UNIQUE,
    title           TEXT    NOT NULL,
    description     TEXT    NOT NULL,
    tags            TEXT    NOT NULL,
    privacy         TEXT    NOT NULL DEFAULT 'public',
    category_id     TEXT    NOT NULL DEFAULT '27',
    scheduled_time  TEXT,
    uploaded_at     TEXT    NOT NULL DEFAULT (datetime('now')),
    post_hour       INTEGER,
    status          TEXT    NOT NULL DEFAULT 'uploaded'
);

CREATE TABLE IF NOT EXISTS analytics (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    upload_id           INTEGER NOT NULL REFERENCES uploads(id),
    fetched_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    views               INTEGER NOT NULL DEFAULT 0,
    avg_view_duration   REAL    NOT NULL DEFAULT 0.0,
    retention_pct       REAL    NOT NULL DEFAULT 0.0,
    like_count          INTEGER NOT NULL DEFAULT 0,
    comment_count       INTEGER NOT NULL DEFAULT 0,
    post_hour           INTEGER,
    hook_pattern        TEXT,
    script_word_count   INTEGER,
    emotional_score     REAL
);

CREATE TABLE IF NOT EXISTS quota_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    date        TEXT    NOT NULL,
    units_used  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS theme_cooldown (
    theme               TEXT PRIMARY KEY,
    last_used_script_id INTEGER,
    last_used_at        TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS script_variants (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    script_id   INTEGER NOT NULL REFERENCES scripts(id),
    language    TEXT NOT NULL,
    hook        TEXT NOT NULL,
    body        TEXT NOT NULL,
    closing     TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS hook_variants (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    script_id   INTEGER NOT NULL REFERENCES scripts(id),
    hook_text   TEXT NOT NULL,
    variant_ix  INTEGER NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS social_posts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id        INTEGER NOT NULL REFERENCES videos(id),
    platform        TEXT NOT NULL,
    external_id     TEXT NOT NULL,
    permalink       TEXT,
    caption         TEXT,
    posted_at       TEXT NOT NULL DEFAULT (datetime('now')),
    status          TEXT NOT NULL DEFAULT 'published'
);

CREATE INDEX IF NOT EXISTS idx_scripts_hash    ON scripts(content_hash);
CREATE INDEX IF NOT EXISTS idx_uploads_yt_id   ON uploads(youtube_id);
CREATE INDEX IF NOT EXISTS idx_analytics_upl   ON analytics(upload_id);
CREATE INDEX IF NOT EXISTS idx_quota_date      ON quota_log(date);
CREATE INDEX IF NOT EXISTS idx_variants_script ON script_variants(script_id);
CREATE INDEX IF NOT EXISTS idx_hooks_script    ON hook_variants(script_id);
CREATE INDEX IF NOT EXISTS idx_social_platform ON social_posts(platform);
CREATE INDEX IF NOT EXISTS idx_social_video    ON social_posts(video_id);
"""


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def initialize_database() -> None:
    """Create all tables and indexes if they don't exist."""
    conn = get_connection()
    try:
        conn.executescript(_SCHEMA)
        _ensure_column(conn, "scripts", "theme", "TEXT")
        _ensure_column(conn, "scripts", "novelty_score", "REAL NOT NULL DEFAULT 0.0")
        _ensure_column(conn, "scripts", "experiment_arm", "TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scripts_theme ON scripts(theme)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_scripts_experiment_arm ON scripts(experiment_arm)")
        conn.commit()
        logger.info("Database initialized at %s", DB_PATH)
    except sqlite3.Error as exc:
        logger.exception("Database initialization failed: %s", exc)
        raise
    finally:
        conn.close()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    """Add a column if missing (idempotent migration helper)."""
    cur = conn.execute(f"PRAGMA table_info({table})")
    cols = {str(row[1]).strip().lower() for row in cur.fetchall()}
    target = str(column).strip().lower()
    if target in cols:
        return

    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
    except sqlite3.OperationalError as exc:
        # Safe idempotency guard for concurrent/previous migrations.
        if "duplicate column name" in str(exc).lower():
            return
        raise


# ─── Generic helpers ─────────────────────────────────────────────────────────

def execute_query(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    """Execute a SELECT query and return all rows."""
    conn = get_connection()
    try:
        cur = conn.execute(sql, params)
        return cur.fetchall()
    finally:
        conn.close()


def execute_write(sql: str, params: tuple = ()) -> int:
    """Execute an INSERT/UPDATE/DELETE; return lastrowid."""
    conn = get_connection()
    try:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError as exc:
        logger.warning("IntegrityError during write: %s", exc)
        raise
    finally:
        conn.close()


def execute_many(sql: str, params_list: list[tuple]) -> None:
    """Execute an INSERT/UPDATE in batch."""
    conn = get_connection()
    try:
        conn.executemany(sql, params_list)
        conn.commit()
    finally:
        conn.close()


# ─── Quota helpers ───────────────────────────────────────────────────────────

def get_quota_used_today() -> int:
    from datetime import date
    today = date.today().isoformat()
    rows = execute_query(
        "SELECT COALESCE(SUM(units_used), 0) AS total FROM quota_log WHERE date = ?",
        (today,)
    )
    return int(rows[0]["total"]) if rows else 0


def log_quota_usage(units: int) -> None:
    from datetime import date
    today = date.today().isoformat()
    execute_write(
        "INSERT INTO quota_log (date, units_used) VALUES (?, ?)",
        (today, units)
    )

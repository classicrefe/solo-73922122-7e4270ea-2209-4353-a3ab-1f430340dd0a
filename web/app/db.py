"""SQLite persistence for acquisition jobs.

All functions are short, synchronous and guarded by a module-level lock, so they
can be called safely from the asyncio event loop or a worker thread.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS acquisitions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    channel         INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'queued',
    error           TEXT,
    created_at      REAL NOT NULL,
    started_at      REAL,
    finished_at     REAL,
    sample_interval REAL,
    volt_scale      REAL,
    volt_offset     REAL,
    points          INTEGER,
    samples         BLOB
);
"""

_lock = threading.Lock()
_path: str | None = None


def init(path: str) -> None:
    global _path
    _path = path
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with _lock, _db() as con:
        con.executescript(SCHEMA)


@contextmanager
def _db():
    con = sqlite3.connect(_path, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        with con:  # commit on success, rollback on exception
            yield con
    finally:
        con.close()


def create(channel: int) -> int:
    with _lock, _db() as con:
        cur = con.execute(
            "INSERT INTO acquisitions (channel, status, created_at) VALUES (?, 'queued', ?)",
            (channel, time.time()),
        )
        return int(cur.lastrowid)


def get(aid: int) -> dict | None:
    with _lock, _db() as con:
        row = con.execute("SELECT * FROM acquisitions WHERE id = ?", (aid,)).fetchone()
        return dict(row) if row is not None else None


def list_recent(limit: int = 50) -> list[dict]:
    with _lock, _db() as con:
        rows = con.execute(
            "SELECT * FROM acquisitions ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def set_status(
    aid: int,
    status: str,
    error: str | None = None,
    started: bool = False,
    finished: bool = False,
) -> None:
    sets = ["status = ?"]
    args: list = [status]
    if error is not None:
        sets.append("error = ?")
        args.append(error)
    if started:
        sets.append("started_at = ?")
        args.append(time.time())
    if finished:
        sets.append("finished_at = ?")
        args.append(time.time())
    args.append(aid)
    with _lock, _db() as con:
        con.execute(f"UPDATE acquisitions SET {', '.join(sets)} WHERE id = ?", args)


def save_result(aid: int, result) -> None:
    """Store capture parameters and the complete sample block in ONE statement,
    so parameters and samples always belong to the same capture."""
    with _lock, _db() as con:
        con.execute(
            "UPDATE acquisitions SET status = 'done', finished_at = ?, "
            "sample_interval = ?, volt_scale = ?, volt_offset = ?, points = ?, samples = ? "
            "WHERE id = ?",
            (
                time.time(),
                result.sample_interval,
                result.volt_scale,
                result.volt_offset,
                result.points,
                result.samples,
                aid,
            ),
        )


def sweep_interrupted() -> None:
    """At startup, mark jobs that can no longer be running (previous process)."""
    with _lock, _db() as con:
        con.execute(
            "UPDATE acquisitions SET status = 'error', "
            "error = 'interrupted by server restart', finished_at = ? "
            "WHERE status IN ('queued', 'running', 'cancelling')",
            (time.time(),),
        )

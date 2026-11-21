"""SQLite persistence for acquisition jobs.

All functions are short, synchronous and guarded by a module-level lock, so they
can be called safely from the asyncio event loop or a worker thread.

Incremental refresh support
---------------------------
Every mutation (create / status change / result) consumes a globally increasing
revision number and stores it in ``acquisitions.rev``.  ``created_rev`` records
the revision at which the row entered the list, so the set of rows that were
visible at an older cursor can be reconstructed from the table itself.

The server keeps a bounded, in-memory log of recent revision numbers.  A client
cursor older than that log (or from a previous process epoch) cannot be served
incrementally, so the caller falls back to a full page.  The log never grows
beyond ``REFRESH_HISTORY_CAP`` entries (default 1000).

Each response also carries ``epoch`` (random per process start) and ``started``
(process boot time, seconds).  ``started`` orders server processes, so a page
response that was still in flight when the server restarted is recognised as
older by the browser even if it is a full page.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
import uuid
from collections import deque
from contextlib import contextmanager

DEFAULT_HISTORY_CAP = 1000
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
    samples         BLOB,
    rev             INTEGER NOT NULL DEFAULT 0,
    created_rev     INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

# Metadata-only projection: the sample BLOB is never read for list/refresh
# queries.  ``has_samples`` tells the UI whether a download is available
# without transferring 8 KiB per row.
META_SELECT = (
    "id, channel, status, error, created_at, started_at, finished_at, "
    "sample_interval, volt_scale, volt_offset, points, rev, created_rev, "
    "(samples IS NOT NULL) AS has_samples"
)


def _meta(row: sqlite3.Row) -> dict:
    """Public metadata row: no sample BLOB, no internal revision columns, and
    a real JSON boolean for the download flag."""
    out = dict(row)
    out.pop("rev", None)
    out.pop("created_rev", None)
    out["has_samples"] = bool(out.get("has_samples"))
    return out

_lock = threading.Lock()
_path: str | None = None
_epoch: str | None = None
_started: float | None = None
_history: deque[int] = deque(maxlen=DEFAULT_HISTORY_CAP)


def _history_cap() -> int:
    try:
        return max(1, int(os.environ.get("REFRESH_HISTORY_CAP", DEFAULT_HISTORY_CAP)))
    except (TypeError, ValueError):
        return DEFAULT_HISTORY_CAP


def init(path: str) -> None:
    global _path, _epoch, _started, _history
    _path = path
    _epoch = uuid.uuid4().hex
    _started = time.time()
    _history = deque(maxlen=_history_cap())
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with _lock, _db() as con:
        con.executescript(SCHEMA)
        # Migrate databases created before incremental refresh existed.
        columns = {row[1] for row in con.execute("PRAGMA table_info(acquisitions)")}
        if "rev" not in columns:
            con.execute(
                "ALTER TABLE acquisitions ADD COLUMN rev INTEGER NOT NULL DEFAULT 0"
            )
        if "created_rev" not in columns:
            con.execute(
                "ALTER TABLE acquisitions ADD COLUMN created_rev INTEGER NOT NULL DEFAULT 0"
            )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_acquisitions_rev "
            "ON acquisitions(rev)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_acquisitions_created_rev "
            "ON acquisitions(created_rev)"
        )
        con.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('revision', '0')"
        )
        # Keep the global counter at least as high as any stored row revision.
        con.execute(
            "UPDATE meta SET value = CAST((SELECT COALESCE(MAX(rev), 0) "
            "FROM acquisitions) AS TEXT) WHERE key = 'revision' "
            "AND CAST(value AS INTEGER) < "
            "(SELECT COALESCE(MAX(rev), 0) FROM acquisitions)"
        )


@contextmanager
def _db():
    con = sqlite3.connect(_path, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        with con:  # commit on success, rollback on exception
            yield con
    finally:
        con.close()


def _current_rev(con: sqlite3.Connection) -> int:
    row = con.execute("SELECT value FROM meta WHERE key = 'revision'").fetchone()
    return int(row[0]) if row is not None else 0


def _next_rev(con: sqlite3.Connection) -> int:
    """Allocate the next global revision inside the caller's transaction."""
    con.execute(
        "UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) "
        "WHERE key = 'revision'"
    )
    return _current_rev(con)


def _note_rev(rev: int) -> None:
    _history.append(rev)


def create(channel: int) -> int:
    with _lock:
        with _db() as con:
            rev = _next_rev(con)
            cur = con.execute(
                "INSERT INTO acquisitions "
                "(channel, status, created_at, rev, created_rev) "
                "VALUES (?, 'queued', ?, ?, ?)",
                (channel, time.time(), rev, rev),
            )
            aid = int(cur.lastrowid)
        # Only committed revisions enter the history; still under _lock so
        # readers never observe the new cursor without its history entry.
        _note_rev(rev)
        return aid


def get(aid: int) -> dict | None:
    """Full row including the sample BLOB (used by the CSV download)."""
    with _lock, _db() as con:
        row = con.execute("SELECT * FROM acquisitions WHERE id = ?", (aid,)).fetchone()
        return dict(row) if row is not None else None


def get_meta(aid: int) -> dict | None:
    """Single row without the sample BLOB."""
    with _lock, _db() as con:
        row = con.execute(
            f"SELECT {META_SELECT} FROM acquisitions WHERE id = ?", (aid,)
        ).fetchone()
        return _meta(row) if row is not None else None


def list_recent(limit: int = 50) -> list[dict]:
    """Recent jobs for the original list endpoint, metadata only."""
    with _lock, _db() as con:
        rows = con.execute(
            f"SELECT {META_SELECT} FROM acquisitions ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [_meta(r) for r in rows]


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
    with _lock:
        with _db() as con:
            rev = _next_rev(con)
            sets.append("rev = ?")
            args.append(rev)
            args.append(aid)
            con.execute(
                f"UPDATE acquisitions SET {', '.join(sets)} WHERE id = ?", args
            )
        # Record every allocated revision so the bounded history stays
        # contiguous even for a no-op update.
        _note_rev(rev)


def save_result(aid: int, result) -> None:
    """Store capture parameters and the complete sample block in ONE statement,
    so parameters and samples always belong to the same capture."""
    with _lock:
        with _db() as con:
            rev = _next_rev(con)
            con.execute(
                "UPDATE acquisitions SET status = 'done', finished_at = ?, "
                "sample_interval = ?, volt_scale = ?, volt_offset = ?, points = ?, "
                "samples = ?, rev = ? WHERE id = ?",
                (
                    time.time(),
                    result.sample_interval,
                    result.volt_scale,
                    result.volt_offset,
                    result.points,
                    result.samples,
                    rev,
                    aid,
                ),
            )
        _note_rev(rev)


def sweep_interrupted() -> None:
    """At startup, mark jobs that can no longer be running (previous process)."""
    with _lock:
        with _db() as con:
            rev = _next_rev(con)
            con.execute(
                "UPDATE acquisitions SET status = 'error', rev = ?, "
                "error = 'interrupted by server restart', finished_at = ? "
                "WHERE status IN ('queued', 'running', 'cancelling')",
                (rev, time.time()),
            )
        _note_rev(rev)


def changes(
    since: int | None, client_epoch: str | None, limit: int = 50
) -> dict:
    """Return a full page or an incremental delta for the refresh endpoint.

    * Full page: no ``since``, a different process ``epoch``, a cursor in the
      future, or a cursor older than the bounded revision history.
    * Delta: rows in the current visible window whose ``rev > since``, plus the
      ids that were visible at ``since`` but have since fallen out of the
      window.  When nothing changed, neither ``records`` nor ``removed`` is
      present in the response.
    """
    with _lock, _db() as con:
        current = _current_rev(con)

        def full() -> dict:
            rows = con.execute(
                f"SELECT {META_SELECT} FROM acquisitions "
                "ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return {
                "mode": "full",
                "epoch": _epoch,
                "started": _started,
                "cursor": current,
                "records": [_meta(r) for r in rows],
            }

        if since is None or client_epoch != _epoch or since < 0 or since > current:
            return full()

        if since == current:
            return {
                "mode": "delta",
                "epoch": _epoch,
                "started": _started,
                "cursor": current,
            }

        oldest = _history[0] if _history else None
        if oldest is None or since < oldest - 1:
            # The changes needed to advance this cursor have been pruned (or
            # the process restarted and the history is empty).
            return full()

        rows = con.execute(
            f"SELECT {META_SELECT} FROM acquisitions "
            "ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        current_ids = [int(r["id"]) for r in rows]
        current_id_set = set(current_ids)

        # Rows that were inside the window at the client's cursor.
        previous_ids = [
            int(r["id"])
            for r in con.execute(
                "SELECT id FROM acquisitions WHERE created_rev <= ? "
                "ORDER BY id DESC LIMIT ?",
                (since, limit),
            ).fetchall()
        ]

        records = [_meta(r) for r in rows if int(r["rev"]) > since]
        removed = [aid for aid in previous_ids if aid not in current_id_set]

        response: dict = {
            "mode": "delta",
            "epoch": _epoch,
            "started": _started,
            "cursor": current,
        }
        if records:
            response["records"] = records
        if removed:
            response["removed"] = removed
        return response

"""Acquisition manager: serializes all instrument access through one worker.

* One asyncio worker processes jobs from a queue -> the instrument is used
  strictly serially, no overlapping sessions.
* Cancelling a QUEUED job only flips its database status; the worker skips it
  before opening a connection, so no SCPI command is ever sent for it.
* Cancelling a RUNNING job is accepted immediately (status -> 'cancelling' and
  the cancel event is set). The job is reported 'cancelled' only after the
  in-flight communication round has actually stopped and its connection has
  been closed.
"""
from __future__ import annotations

import asyncio

from . import db
from .instrument import AcquisitionCancelled, run_acquisition

TERMINAL_STATES = ("done", "error", "cancelled")


class AcquisitionManager:
    def __init__(self, host: str, port: int, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._queue: asyncio.Queue[int] = asyncio.Queue()
        self._cancel_events: dict[int, asyncio.Event] = {}
        self._worker: asyncio.Task | None = None

    def start(self) -> None:
        self._worker = asyncio.create_task(self._run(), name="acquisition-worker")

    async def stop(self) -> None:
        if self._worker is not None:
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
            self._worker = None

    # ---- API-facing operations (called from the event loop) ----

    def submit(self, channel: int) -> int:
        aid = db.create(channel)
        self._cancel_events[aid] = asyncio.Event()
        self._queue.put_nowait(aid)
        return aid

    def cancel(self, aid: int) -> dict | None:
        """Returns the job row after the cancel attempt, or None if unknown."""
        row = db.get_meta(aid)
        if row is None:
            return None
        status = row["status"]
        if status == "queued":
            # Never started -> never will send a command; worker skips it.
            db.set_status(aid, "cancelled", finished=True)
        elif status in ("running", "cancelling"):
            # Accepted immediately; the worker reports 'cancelled' only after
            # the current communication round has stopped.
            if status == "running":
                db.set_status(aid, "cancelling")
            event = self._cancel_events.get(aid)
            if event is not None:
                event.set()
        return db.get_meta(aid)

    # ---- worker ----

    async def _run(self) -> None:
        while True:
            aid = await self._queue.get()
            try:
                row = db.get_meta(aid)
                if row is None or row["status"] != "queued":
                    continue  # cancelled while queued: skip without any command
                event = self._cancel_events.setdefault(aid, asyncio.Event())
                db.set_status(aid, "running", started=True)
                try:
                    result = await run_acquisition(
                        self.host, self.port, row["channel"], self.timeout, event
                    )
                except AcquisitionCancelled:
                    # The round has stopped and its connection is closed by now.
                    db.set_status(aid, "cancelled", finished=True)
                except Exception as exc:  # timeout / disconnect / bad data
                    db.set_status(aid, "error", error=str(exc), finished=True)
                else:
                    db.save_result(aid, result)
            finally:
                self._cancel_events.pop(aid, None)

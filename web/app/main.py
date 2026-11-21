"""FastAPI application: oscilloscope waveform acquisition web service."""
from __future__ import annotations

import os
import struct
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import db
from .manager import AcquisitionManager

SCOPE_HOST = os.environ.get("SCOPE_HOST", "127.0.0.1")
SCOPE_PORT = int(os.environ.get("SCOPE_PORT", "5025"))
SCOPE_TIMEOUT = float(os.environ.get("SCOPE_TIMEOUT", "5"))
DB_PATH = os.environ.get("DB_PATH", "acquisitions.db")
STATIC_DIR = Path(__file__).parent / "static"

manager: AcquisitionManager | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global manager
    db.init(DB_PATH)
    db.sweep_interrupted()
    manager = AcquisitionManager(SCOPE_HOST, SCOPE_PORT, SCOPE_TIMEOUT)
    manager.start()
    yield
    await manager.stop()


app = FastAPI(title="Oscilloscope Acquisition", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class AcquireIn(BaseModel):
    channel: int = Field(..., ge=1, le=2)


def public(row: dict | None) -> dict | None:
    if row is None:
        return None
    out = {k: v for k, v in row.items() if k != "samples"}
    # List/refresh queries only select row metadata plus a has_samples flag;
    # never load the BLOB just to answer a list request.
    if "has_samples" not in out:
        out["has_samples"] = row.get("samples") is not None
    return out


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
async def health():
    return {
        "ok": True,
        "scope": f"{SCOPE_HOST}:{SCOPE_PORT}",
        "round_timeout_s": SCOPE_TIMEOUT,
    }


@app.get("/api/acquisitions")
async def list_acquisitions(limit: int = 50):
    return [public(r) for r in db.list_recent(limit)]


@app.get("/api/acquisitions/changes")
async def acquisition_changes(
    since: int | None = Query(default=None, ge=0),
    epoch: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
):
    """Incremental refresh for the waveform page.

    Without ``since`` (first load, or the client lost its cursor) this returns
    a full page of the newest ``limit`` jobs, newest first.  With a cursor that
    is still covered by the bounded server history it returns only:
      * ``records``: rows currently in the window whose revision is newer than
        ``since`` (new jobs and in-place status changes);
      * ``removed``: ids that were in the window at ``since`` but have since
        dropped out of it.
    An unchanged poll contains neither key.  ``epoch`` identifies the server
    process: a mismatch means the history was reset and a full page is sent.
    """
    return db.changes(since, epoch, limit)


@app.post("/api/acquisitions", status_code=201)
async def create_acquisition(req: AcquireIn):
    aid = manager.submit(req.channel)
    return public(db.get_meta(aid))


@app.get("/api/acquisitions/{aid}")
async def get_acquisition(aid: int):
    row = public(db.get_meta(aid))
    if row is None:
        raise HTTPException(404, "acquisition not found")
    return row


@app.post("/api/acquisitions/{aid}/cancel")
async def cancel_acquisition(aid: int):
    row = manager.cancel(aid)
    if row is None:
        raise HTTPException(404, "acquisition not found")
    return public(db.get_meta(aid))


@app.get("/api/acquisitions/{aid}/csv")
async def acquisition_csv(aid: int):
    row = db.get(aid)
    if row is None:
        raise HTTPException(404, "acquisition not found")
    # Only a fully received block (parameters + samples stored together) may
    # be downloaded.
    if row["status"] != "done" or row["samples"] is None:
        raise HTTPException(409, "capture not complete; CSV unavailable")
    points = int(row["points"])
    raw = struct.unpack(f"<{points}h", row["samples"])
    dt = float(row["sample_interval"])
    scale = float(row["volt_scale"])
    offset = float(row["volt_offset"])
    lines = ["time_s,voltage_v"]
    append = lines.append
    for i, value in enumerate(raw):
        append(f"{i * dt:.9e},{value * scale + offset:.6e}")
    filename = f"acquisition_{aid}_ch{row['channel']}.csv"
    return Response(
        "\n".join(lines) + "\n",
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

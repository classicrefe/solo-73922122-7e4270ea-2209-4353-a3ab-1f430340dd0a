"""FastAPI application: oscilloscope waveform acquisition web service."""
from __future__ import annotations

import os
import struct
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
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


class AcquireIn(BaseModel):
    channel: int = Field(..., ge=1, le=2)


def public(row: dict | None) -> dict | None:
    if row is None:
        return None
    out = {k: v for k, v in row.items() if k != "samples"}
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


@app.post("/api/acquisitions", status_code=201)
async def create_acquisition(req: AcquireIn):
    aid = manager.submit(req.channel)
    return public(db.get(aid))


@app.get("/api/acquisitions/{aid}")
async def get_acquisition(aid: int):
    row = public(db.get(aid))
    if row is None:
        raise HTTPException(404, "acquisition not found")
    return row


@app.post("/api/acquisitions/{aid}/cancel")
async def cancel_acquisition(aid: int):
    row = manager.cancel(aid)
    if row is None:
        raise HTTPException(404, "acquisition not found")
    return public(row)


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

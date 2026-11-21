#!/usr/bin/env python3
"""End-to-end tests for the incremental refresh API.

Starts the real SCPI simulator + FastAPI app (same harness style as
selftest.py) and verifies:

 1. first load (no cursor) returns a full page, newest first, metadata only
 2. an unchanged poll carries neither records nor removed
 3. a queued job that completes is delivered as an in-place update
    (status + has_samples), not as a duplicate/new row
 4. rows that fall out of the fixed-size window are reported in `removed`
 5. old cursors beyond the bounded server history fall back to a full page
 6. a server restart invalidates the epoch and forces a full page
 7. the original list endpoint and the CSV download still behave as before
 8. a stale cursor never yields a record that would move the page backwards

Usage: python3 tests/test_refresh.py
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

failures: list[str] = []


def check(cond: bool, label: str) -> None:
    print(("  PASS  " if cond else "  FAIL  ") + label, flush=True)
    if not cond:
        failures.append(label)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(method: str, base: str, path: str, body: dict | None = None,
         timeout: float = 5.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except urllib.error.URLError:
        return 0, b""


def http_json(method: str, base: str, path: str, body: dict | None = None):
    status, raw = http(method, base, path, body)
    return status, json.loads(raw)


def changes(base: str, **params) -> dict:
    qs = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    status, data = http_json("GET", base, "/api/acquisitions/changes?" + qs)
    assert status == 200, (status, data)
    return data


def wait_status(base: str, aid: int, wanted: tuple[str, ...], timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        _, job = http_json("GET", base, f"/api/acquisitions/{aid}")
        last = job
        if job["status"] in wanted:
            return job
        time.sleep(0.05)
    raise AssertionError(f"acquisition {aid} did not reach {wanted} (last={last})")


_WRAPPER = """
import os, sys, threading
def _watch_stdin():
    sys.stdin.buffer.read()
    os._exit(0)
threading.Thread(target=_watch_stdin, daemon=True).start()
threading.Timer(float(os.environ.get("SELFTEST_TTL", "240")), os._exit, (0,)).start()
target = os.environ["SELFTEST_TARGET"]
if target == "sim":
    sys.path.insert(0, os.environ["SELFTEST_SIMDIR"])
    import asyncio
    import scpi_simulator
    asyncio.run(scpi_simulator.main())
else:
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1",
                port=int(os.environ["SELFTEST_PORT"]))
"""


class Stack:
    """Simulator + web app subprocesses; the web side can be restarted."""

    def __init__(self, db_path: str, sim_port: int, web_port: int,
                 history_cap: int = 1000):
        self.db_path = db_path
        self.sim_port = sim_port
        self.web_port = web_port
        self.history_cap = history_cap
        self.base = f"http://127.0.0.1:{web_port}"
        self.sim_proc = None
        self.web_proc = None
        self.sim_log = None
        self.web_log = None

    def start_sim(self) -> None:
        sim_dir = os.path.join(ROOT, "simulator")
        env = dict(os.environ, SIM_HOST="127.0.0.1",
                   SIM_PORT=str(self.sim_port), SIM_WAVEFORM="sine",
                   SIM_SEGMENT="1024", SELFTEST_TARGET="sim",
                   SELFTEST_SIMDIR=sim_dir)
        self.sim_proc = subprocess.Popen(
            [sys.executable, "-c", _WRAPPER], stdin=subprocess.PIPE, env=env,
            stdout=self.sim_log, stderr=subprocess.STDOUT)
        for _ in range(100):
            try:
                with socket.create_connection(("127.0.0.1", self.sim_port),
                                              timeout=0.2):
                    return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("simulator did not start")

    def start_web(self) -> None:
        env = dict(os.environ, SCOPE_HOST="127.0.0.1",
                   SCOPE_PORT=str(self.sim_port), SCOPE_TIMEOUT="5",
                   DB_PATH=self.db_path,
                   REFRESH_HISTORY_CAP=str(self.history_cap),
                   SELFTEST_TARGET="web", SELFTEST_PORT=str(self.web_port))
        self.web_proc = subprocess.Popen(
            [sys.executable, "-c", _WRAPPER], stdin=subprocess.PIPE,
            cwd=os.path.join(ROOT, "web"), env=env,
            stdout=self.web_log, stderr=subprocess.STDOUT)
        for _ in range(100):
            if http("GET", self.base, "/api/health")[0] == 200:
                return
            time.sleep(0.1)
        raise RuntimeError("web app did not start")

    def stop_web(self) -> None:
        proc = self.web_proc
        self.web_proc = None
        if proc is None:
            return
        try:
            proc.stdin.close()
        except (OSError, ValueError):
            pass
        try:
            proc.terminate()
        except (ProcessLookupError, PermissionError, OSError):
            pass
        try:
            proc.wait(timeout=8)
        except (subprocess.TimeoutExpired, ProcessLookupError,
                PermissionError, OSError):
            try:
                proc.kill()
            except (ProcessLookupError, PermissionError, OSError):
                pass

    def stop(self) -> None:
        self.stop_web()
        proc = self.sim_proc
        self.sim_proc = None
        if proc is not None:
            try:
                proc.stdin.close()
            except (OSError, ValueError):
                pass
            try:
                proc.terminate()
            except (ProcessLookupError, PermissionError, OSError):
                pass
            try:
                proc.wait(timeout=8)
            except (subprocess.TimeoutExpired, ProcessLookupError,
                    PermissionError, OSError):
                try:
                    proc.kill()
                except (ProcessLookupError, PermissionError, OSError):
                    pass
        for fh in (self.sim_log, self.web_log):
            if fh is not None:
                fh.close()


def main() -> int:
    sim_port = free_port()
    web_port = free_port()
    tmpdir = tempfile.mkdtemp(prefix="acq-refresh-")
    db_path = os.path.join(tmpdir, "test.db")
    sim_log = open(os.path.join(tmpdir, "sim.log"), "w")
    web_log = open(os.path.join(tmpdir, "web.log"), "w")
    stack = Stack(db_path, sim_port, web_port, history_cap=20)
    stack.sim_log = sim_log
    stack.web_log = web_log
    try:
        stack.start_sim()
        stack.start_web()
        base = stack.base
        print(f"simulator :{sim_port}  web :{web_port}  db {db_path}  "
              f"history cap 20", flush=True)

        # ---- 1. first load: full page, metadata only ----------------------
        print("[1] first load returns a full page (metadata only)")
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        a1 = job["id"]
        wait_status(base, a1, ("done", "error"), 10)
        first = changes(base, limit=50)
        check(first["mode"] == "full", "first poll is a full page")
        check(isinstance(first.get("epoch"), str) and first["cursor"] >= 1,
              "epoch and cursor present")
        check([r["id"] for r in first["records"]] == sorted(
            [r["id"] for r in first["records"]], reverse=True),
            "records newest first")
        check(all("samples" not in r for r in first["records"]),
              "no sample blob in list payload")
        check("has_samples" in first["records"][0], "has_samples flag present")
        cursor, epoch = first["cursor"], first["epoch"]

        # ---- 2. unchanged poll carries no records -------------------------
        print("[2] unchanged poll is empty")
        same = changes(base, since=cursor, epoch=epoch, limit=50)
        check(same["mode"] == "delta", "unchanged poll is a delta")
        check("records" not in same and "removed" not in same,
              "no records / removed keys when nothing changed")
        check(same["cursor"] == cursor, "cursor unchanged")

        # ---- 3. queued -> done delivered as in-place update ---------------
        print("[3] completion of an existing row is an in-place update")
        # Slow the transfer so the new job is observably queued/running.
        simctl = socket.create_connection(("127.0.0.1", sim_port), timeout=5)
        simctl.sendall(b":SIM:SEGD 0.2\n:SIM:CONFIG?\n")
        simctl.makefile("rb").readline()
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 2})
        a3 = job["id"]
        seen = changes(base, since=cursor, epoch=epoch, limit=50)
        ids = [r["id"] for r in seen.get("records", [])]
        check(a3 in ids, "new queued row appears in the delta")
        check(seen["mode"] == "delta" and "removed" not in seen,
              "delta did not resend the whole page")
        queued = next(r for r in seen["records"] if r["id"] == a3)
        check(queued["status"] in ("queued", "running"), "new row initial status")
        check(queued["has_samples"] is False, "new row has no download yet")

        wait_status(base, a3, ("done", "error"), 15)
        after = changes(base, since=seen["cursor"], epoch=seen["epoch"], limit=50)
        recs = {r["id"]: r for r in after.get("records", [])}
        check(a3 in recs, "completion delivered as a change record")
        check(recs[a3]["status"] == "done", "row status updated to done")
        check(recs[a3]["has_samples"] is True, "download flag turned on")
        check("removed" not in after, "completed row was not removed")
        check(recs[a3]["id"] == a3 and a3 not in [r for r in after.get("removed", [])],
              "completed row is not reported as removed")
        cursor, epoch = after["cursor"], after["epoch"]
        # The old row is not duplicated anywhere in the client model.
        full_now = changes(base, limit=50)
        check(sum(1 for r in full_now["records"] if r["id"] == a3) == 1,
              "exactly one row for the completed id")
        simctl.sendall(b":SIM:SEGD 0\n:SIM:CONFIG?\n")
        simctl.makefile("rb").readline()
        simctl.close()

        # ---- 4. rows leaving the fixed window are reported as removed -----
        print("[4] rows that leave the window are reported in removed")
        # Window of 2: newest two ids only.
        small = changes(base, limit=2)
        check(len(small["records"]) == 2, "page limited to 2 rows")
        old_ids = [r["id"] for r in small["records"]]
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        a4 = job["id"]
        wait_status(base, a4, ("done", "error"), 10)
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        a4b = job["id"]
        wait_status(base, a4b, ("done", "error"), 10)
        delta = changes(base, since=small["cursor"], epoch=small["epoch"], limit=2)
        check(delta["mode"] == "delta", "delta for the small window")
        check(a4b in [r["id"] for r in delta.get("records", [])],
              "newest row delivered")
        check(sorted(delta.get("removed", [])) == sorted(old_ids),
              f"both old rows reported removed (got {delta.get('removed')})")
        rids = {r["id"] for r in delta.get("records", [])}
        check(not (rids & set(old_ids)), "removed rows not resent as records")

        # ---- 5. cursor older than the bounded history -> full page --------
        print("[5] old cursor beyond bounded history falls back to full page")
        # The server keeps the last 20 revisions. Push the cursor we hold well
        # past that window (each job consumes 3 revisions), then request it.
        old_cursor, old_epoch = small["cursor"], small["epoch"]
        for _ in range(8):
            _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
            wait_status(base, job["id"], ("done", "error"), 10)
        stale = changes(base, since=old_cursor, epoch=old_epoch, limit=50)
        check(stale["mode"] == "full", "cursor older than history -> full page")
        check(len(stale["records"]) >= 1, "full page has rows")
        check(stale["cursor"] - old_cursor > 20,
              f"revision span exceeds the 20-entry history "
              f"({old_cursor} -> {stale['cursor']})")
        # A cursor from the future is also untrusted.
        future = changes(base, since=stale["cursor"] + 1000,
                         epoch=stale["epoch"], limit=50)
        check(future["mode"] == "full", "cursor in the future -> full page")

        # ---- 6. server restart forces a full page -------------------------
        print("[6] server restart invalidates the cursor")
        before = changes(base, limit=50)
        stack.stop_web()
        stack.start_web()
        after_restart = changes(base, since=before["cursor"], epoch=before["epoch"],
                                limit=50)
        check(after_restart["mode"] == "full", "old epoch -> full page")
        check(after_restart["epoch"] != before["epoch"], "epoch changed on restart")
        check(len(after_restart.get("records", [])) == len(before["records"]),
              "restart page has the same rows")
        # A missing cursor also requests a full page.
        nocur = changes(base, limit=50)
        check(nocur["mode"] == "full", "no cursor -> full page")

        # ---- 7. original list endpoint + CSV unchanged --------------------
        print("[7] original list endpoint and CSV download unchanged")
        status, raw = http("GET", base, "/api/acquisitions")
        listing = json.loads(raw)
        check(status == 200 and isinstance(listing, list), "GET list returns a JSON array")
        check(len(listing) <= 50 and "samples" not in listing[0],
              "list is capped and metadata only")
        check({k for k in ("id", "channel", "status", "has_samples")} <= set(listing[0]),
              "list row keeps its original fields")
        status, raw = http("GET", base, "/api/acquisitions")
        check(raw == http("GET", base, "/api/acquisitions")[1],
              "list response stable across calls")
        done_ids = [r["id"] for r in listing if r["status"] == "done"
                    and r["has_samples"]]
        check(bool(done_ids), "at least one completed capture to download")
        status, raw = http("GET", base, f"/api/acquisitions/{done_ids[0]}/csv")
        lines = raw.decode().strip().split("\n")
        check(status == 200 and lines[0] == "time_s,voltage_v", "CSV header intact")
        check(len(lines) == 4097, "CSV still has 4096 data rows")
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        running_id = job["id"]
        status, _ = http("GET", base, f"/api/acquisitions/{running_id}/csv")
        check(status == 409, "incomplete capture still refused (409)")

        # ---- 8. no stale records in a newer page version ------------------
        print("[8] stale cursor does not resurrect old state")
        # Restart again so history is empty; an old cursor must full-page.
        latest = changes(base, limit=50)
        stack.stop_web()
        stack.start_web()
        # The old cursor is now from a previous epoch -> full page with the
        # current (post-restart) state, never the old snapshot.
        resp = changes(base, since=latest["cursor"], epoch=latest["epoch"], limit=50)
        check(resp["mode"] == "full", "stale epoch yields a full page")
        _, get_job = http_json("GET", base, f"/api/acquisitions/{running_id}")
        check(get_job["status"] == "error" and
              "interrupted by server restart" in (get_job["error"] or ""),
              "interrupted job reported as error after restart")
        matching = [r for r in resp["records"] if r["id"] == running_id]
        check(bool(matching) and matching[0]["status"] == "error",
              "full page reflects the interrupted status")
    finally:
        stack.stop()

    print()
    if failures:
        print(f"{len(failures)} FAILURE(S):")
        for f in failures:
            print("  - " + f)
        print(f"(logs in {tmpdir})")
        return 1
    print("ALL REFRESH CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())

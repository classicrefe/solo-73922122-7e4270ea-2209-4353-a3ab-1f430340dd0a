#!/usr/bin/env python3
"""End-to-end self test for the oscilloscope acquisition stack.

Starts the SCPI simulator and the FastAPI web app as local subprocesses, then
verifies the key behaviours required by the task:

 1. happy path: acquire -> done -> CSV download (4096 rows, params applied)
 2. samples really contain newline (0x0A) / CR (0x0D) bytes
 3. per-channel parameters (CH2 uses its own scale/offset)
 4. queued cancel sends NO commands to the instrument
 5. running cancel: accepted promptly, reported cancelled only after the round
    stops; CSV not downloadable while incomplete
 6. 5 s round timeout on a delayed tail; later acquisitions recover w/o restart
 7. mid-transfer disconnect -> error; next acquisition recovers and returns the
    CURRENT waveform (no stale bytes leaking into later orders)
 8. slow instrument responses do not block the web API

Usage: python3 tests/selftest.py
"""
from __future__ import annotations

import json
import math
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "simulator"))
import scpi_simulator as sim  # noqa: E402

YMULT = {1: 1e-2, 2: 2.5e-2}
YOFF = {1: 0.0, 2: 0.5}
XINCR = 1e-6
POINTS = 4096

failures: list[str] = []


def check(cond: bool, label: str) -> None:
    print(("  PASS  " if cond else "  FAIL  ") + label, flush=True)
    if not cond:
        failures.append(label)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(method: str, base: str, path: str, body: dict | None = None, timeout: float = 5.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
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


class SimControl:
    """One persistent control connection to the simulator, so test-side SIM
    commands do not perturb the connection-count statistics."""

    def __init__(self, port: int):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.f = self.sock.makefile("rb")

    def cmd(self, name: str, is_query: bool = False) -> str | None:
        self.sock.sendall(name.encode() + b"\n")
        if is_query:
            return self.f.readline().decode().strip()
        return None

    def set(self, **kwargs) -> None:
        mapping = {
            "waveform": ":SIM:WAVE {}",
            "segment": ":SIM:SEG {}",
            "segment_delay": ":SIM:SEGD {}",
            "tail_delay": ":SIM:TAILD {}",
            "drop_after": ":SIM:DROP {}",
            "response_delay": ":SIM:RESPD {}",
        }
        for key, value in kwargs.items():
            self.cmd(mapping[key].format(value))
        # Synchronise: the CONFIG? reply is sent only after all previous
        # commands on this connection were processed (TCP ordering), so the
        # new settings are guaranteed to be in effect when set() returns.
        self.cmd(":SIM:CONFIG?", is_query=True)

    def stats(self) -> dict:
        reply = self.cmd(":SIM:STATS?", is_query=True)
        return dict(kv.split("=") for kv in reply.split(","))

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


def wait_status(base: str, aid: int, wanted: tuple[str, ...], timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        _, job = http_json("GET", base, f"/api/acquisitions/{aid}")
        last = job
        if job["status"] in wanted:
            return job
        time.sleep(0.1)
    raise AssertionError(
        f"acquisition {aid} did not reach {wanted} within {timeout}s (last={last})"
    )


def expected_csv_rows(kind: str, channel: int) -> list[tuple[float, float]]:
    raw = sim.generate_waveform(kind, channel)
    return [
        (i * XINCR, v * YMULT[channel] + YOFF[channel]) for i, v in enumerate(raw)
    ]


def verify_csv(base: str, aid: int, kind: str, channel: int, label: str) -> None:
    status, raw = http("GET", base, f"/api/acquisitions/{aid}/csv")
    check(status == 200, f"{label}: CSV downloadable (HTTP {status})")
    text = raw.decode()
    lines = text.strip().split("\n")
    check(lines[0] == "time_s,voltage_v", f"{label}: CSV header")
    check(len(lines) == POINTS + 1, f"{label}: CSV has {POINTS} data rows (got {len(lines) - 1})")
    expected = expected_csv_rows(kind, channel)
    ok = True
    for idx in (0, 1, 100, 1024, 3000, POINTS - 1):
        t_str, v_str = lines[1 + idx].split(",")
        t_exp, v_exp = expected[idx]
        if not (math.isclose(float(t_str), t_exp, rel_tol=1e-6, abs_tol=1e-12)
                and math.isclose(float(v_str), v_exp, rel_tol=1e-6, abs_tol=1e-9)):
            ok = False
            print(f"    row {idx}: got ({t_str},{v_str}) expected ({t_exp},{v_exp})")
    check(ok, f"{label}: CSV values match {kind}/CH{channel} with per-channel params")


# Subprocess wrapper: exits when stdin reaches EOF (parent test exited or
# closed the pipe) and self-terminates via os._exit after SELFTEST_TTL seconds
# as a backstop. This guarantees no leaked processes even in sandboxes where
# signals between processes are not permitted (proc.terminate() no-ops there).
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


def main() -> int:
    sim_port = free_port()
    web_port = free_port()
    base = f"http://127.0.0.1:{web_port}"
    tmpdir = tempfile.mkdtemp(prefix="acq-test-")
    db_path = os.path.join(tmpdir, "test.db")
    sim_log = open(os.path.join(tmpdir, "sim.log"), "w")
    web_log = open(os.path.join(tmpdir, "web.log"), "w")

    sim_dir = os.path.join(ROOT, "simulator")
    sim_env = dict(os.environ, SIM_HOST="127.0.0.1", SIM_PORT=str(sim_port),
                   SIM_WAVEFORM="sine", SIM_SEGMENT="1024",
                   SELFTEST_TARGET="sim", SELFTEST_SIMDIR=sim_dir)
    web_env = dict(os.environ, SCOPE_HOST="127.0.0.1", SCOPE_PORT=str(sim_port),
                   SCOPE_TIMEOUT="5", DB_PATH=db_path,
                   SELFTEST_TARGET="web", SELFTEST_PORT=str(web_port))

    sim_proc = subprocess.Popen(
        [sys.executable, "-c", _WRAPPER], stdin=subprocess.PIPE,
        env=sim_env, stdout=sim_log, stderr=subprocess.STDOUT)
    web_proc = subprocess.Popen(
        [sys.executable, "-c", _WRAPPER], stdin=subprocess.PIPE,
        cwd=os.path.join(ROOT, "web"), env=web_env,
        stdout=web_log, stderr=subprocess.STDOUT)

    simctl = None
    try:
        # wait for both ends
        for _ in range(100):
            try:
                with socket.create_connection(("127.0.0.1", sim_port), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.1)
        for _ in range(100):
            status, _ = http("GET", base, "/api/health")
            if status == 200:
                break
            time.sleep(0.2)
        else:
            raise RuntimeError("web app did not start")
        print(f"simulator :{sim_port}  web :{web_port}  db {db_path}", flush=True)
        simctl = SimControl(sim_port)

        # ---- 1. happy path, CH1 ------------------------------------------
        print("[1] happy path acquisition (CH1)")
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        a1 = job["id"]
        job = wait_status(base, a1, ("done", "error"), 10)
        check(job["status"] == "done", f"acquisition #{a1} done (status={job['status']}, err={job['error']})")
        check(math.isclose(job["sample_interval"], XINCR), "sample interval = 1e-6 s")
        check(math.isclose(job["volt_scale"], YMULT[1]), "volt scale = 1e-2 V/LSB (CH1)")
        check(math.isclose(job["volt_offset"], YOFF[1]), "volt offset = 0 (CH1)")
        check(job["points"] == POINTS, "points = 4096")
        verify_csv(base, a1, "sine", 1, "[1]")

        # ---- 2. samples contain newline/CR bytes --------------------------
        print("[2] binary samples contain 0x0A / 0x0D bytes")
        with sqlite3.connect(db_path) as con:
            (blob,) = con.execute("SELECT samples FROM acquisitions WHERE id=?", (a1,)).fetchone()
        check(b"\x0a" in blob, "sample blob contains 0x0A (newline) bytes")
        check(b"\x0d" in blob, "sample blob contains 0x0D (CR) bytes")
        check(len(blob) == POINTS * 2, "blob is exactly 8192 bytes")

        # ---- 3. per-channel parameters ------------------------------------
        print("[3] CH2 acquisition uses CH2 parameters")
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 2})
        a3 = job["id"]
        job = wait_status(base, a3, ("done", "error"), 10)
        check(job["status"] == "done", f"acquisition #{a3} done")
        check(math.isclose(job["volt_scale"], YMULT[2]), "volt scale = 2.5e-2 V/LSB (CH2)")
        check(math.isclose(job["volt_offset"], YOFF[2]), "volt offset = 0.5 V (CH2)")
        verify_csv(base, a3, "sine", 2, "[3]")

        # ---- 4. queued cancel sends no commands ---------------------------
        print("[4] cancel while queued -> no commands sent")
        stats0 = simctl.stats()
        simctl.set(segment_delay=0.3)  # slow the block transfer (~2.4 s)
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        a4 = job["id"]
        wait_status(base, a4, ("running",), 5)
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        b4 = job["id"]
        _, job = http_json("POST", base, f"/api/acquisitions/{b4}/cancel")
        check(job["status"] == "cancelled", f"queued job #{b4} cancelled immediately")
        job = wait_status(base, a4, ("done", "error"), 15)
        check(job["status"] == "done", f"slow acquisition #{a4} still completes")
        time.sleep(0.5)  # give the worker a chance to (wrongly) pick up the job
        stats1 = simctl.stats()
        check(int(stats1["connections"]) == int(stats0["connections"]) + 1,
              f"exactly one new connection (got {stats0['connections']} -> {stats1['connections']})")
        check(int(stats1["data_queries"]) == int(stats0["data_queries"]) + 1,
              "exactly one DATA? query (cancelled job sent none)")
        job = http_json("GET", base, f"/api/acquisitions/{b4}")[1]
        check(job["started_at"] is None, "cancelled queued job never started")
        simctl.set(segment_delay=0.0)

        # ---- 5. running cancel --------------------------------------------
        print("[5] cancel during acquisition")
        simctl.set(segment_delay=0.3)
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        a5 = job["id"]
        wait_status(base, a5, ("running",), 5)
        time.sleep(1.0)  # now inside the segmented DATA? transfer
        status, _ = http("GET", base, f"/api/acquisitions/{a5}/csv")
        check(status == 409, "CSV refused (409) while block incomplete")
        t0 = time.monotonic()
        _, job = http_json("POST", base, f"/api/acquisitions/{a5}/cancel")
        ack = time.monotonic() - t0
        check(job["status"] == "cancelling" and ack < 1.0,
              f"cancel accepted promptly (status={job['status']}, {ack:.2f}s)")
        job = wait_status(base, a5, ("cancelled",), 5)
        check(job["status"] == "cancelled", "reported cancelled after the round stopped")
        simctl.set(segment_delay=0.0)
        # subsequent acquisition works without any restart
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        a5b = job["id"]
        job = wait_status(base, a5b, ("done", "error"), 10)
        check(job["status"] == "done", "next acquisition completes without restart")
        verify_csv(base, a5b, "sine", 1, "[5]")

        # ---- 6. round timeout on delayed tail ------------------------------
        print("[6] 5 s round timeout (delayed tail)")
        simctl.set(tail_delay=6.0)
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        a6 = job["id"]
        t0 = time.monotonic()
        job = wait_status(base, a6, ("error",), 15)
        elapsed = time.monotonic() - t0
        check(job["status"] == "error" and "timed out" in (job["error"] or ""),
              f"timeout error reported ({job['error']!r})")
        check(4.0 < elapsed < 10.0, f"timed out after ~5 s (took {elapsed:.1f}s)")
        simctl.set(tail_delay=0.0)
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        a6b = job["id"]
        job = wait_status(base, a6b, ("done", "error"), 10)
        check(job["status"] == "done", "acquisition after timeout recovers without restart")
        verify_csv(base, a6b, "sine", 1, "[6]")

        # ---- 7. mid-transfer disconnect, then no stale bytes ---------------
        print("[7] mid-transfer disconnect + stale-byte isolation")
        simctl.set(drop_after=1000)
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        a7 = job["id"]
        job = wait_status(base, a7, ("error",), 10)
        check(job["status"] == "error", f"disconnect reported as error ({job['error']!r})")
        simctl.set(drop_after=0, waveform="square")
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        a7b = job["id"]
        job = wait_status(base, a7b, ("done", "error"), 10)
        check(job["status"] == "done", "acquisition after disconnect recovers")
        verify_csv(base, a7b, "square", 1, "[7]")  # would fail on stale sine bytes
        simctl.set(waveform="sine")

        # ---- 8. slow instrument does not block the web API -----------------
        print("[8] slow responses do not block web operations")
        simctl.set(response_delay=2.0)
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        a8 = job["id"]
        wait_status(base, a8, ("running",), 5)
        time.sleep(0.3)
        t0 = time.monotonic()
        status, _ = http("GET", base, "/api/acquisitions")
        latency = time.monotonic() - t0
        check(status == 200 and latency < 1.0, f"list API answered in {latency:.2f}s during slow acquisition")
        t0 = time.monotonic()
        _, job = http_json("POST", base, f"/api/acquisitions/{a8}/cancel")
        latency = time.monotonic() - t0
        check(job["status"] == "cancelling" and latency < 1.0,
              f"cancel accepted in {latency:.2f}s during slow acquisition")
        wait_status(base, a8, ("cancelled",), 8)
        simctl.set(response_delay=0.0)
        _, job = http_json("POST", base, "/api/acquisitions", {"channel": 1})
        a8b = job["id"]
        job = wait_status(base, a8b, ("done", "error"), 10)
        check(job["status"] == "done", "final acquisition completes")

    finally:
        for proc in (web_proc, sim_proc):
            try:
                proc.stdin.close()  # EOF -> wrapper exits even without signals
            except (OSError, ValueError):
                pass
            try:
                proc.terminate()
            except (ProcessLookupError, PermissionError, OSError):
                pass
        for proc in (web_proc, sim_proc):
            try:
                proc.wait(timeout=8)
            except (subprocess.TimeoutExpired, ProcessLookupError, PermissionError, OSError):
                try:
                    proc.kill()
                except (ProcessLookupError, PermissionError, OSError):
                    pass
        if simctl is not None:
            simctl.close()
        sim_log.close()
        web_log.close()

    print()
    if failures:
        print(f"{len(failures)} FAILURE(S):")
        for f in failures:
            print("  - " + f)
        print(f"(logs in {tmpdir})")
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())

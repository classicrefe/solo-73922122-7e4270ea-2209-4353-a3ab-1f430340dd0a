#!/usr/bin/env python3
"""SCPI/TCP oscilloscope simulator.

A small LAN oscilloscope simulator speaking a SCPI-like text protocol over TCP,
with IEEE 488.2 definite-length binary blocks for waveform data.

Supported SCPI commands (case-insensitive, leading ':' optional):
    *IDN?                       instrument identification
    :STOP                       freeze the current capture
    :RUN                        resume free-running acquisition
    :ACQuire:STATe?             "STOP" or "RUN"
    :WAVeform:SOURce CHANnel<n> select channel 1..2
    :WAVeform:SOURce?           selected channel
    :WAVeform:XINcr?            sample interval in seconds
    :WAVeform:YMUlt?            volts per LSB for the selected channel
    :WAVeform:YOFf?             voltage offset for the selected channel
    :WAVeform:POINts?           number of samples (4096)
    :WAVeform:DATA?             IEEE488.2 block: #48192<8192 bytes int16 LE> + \n

Simulator fault-injection / behaviour commands (also configurable via env):
    :SIM:WAVE SINE|SQUARE|RAMP|NOISE   waveform shape        (SIM_WAVEFORM)
    :SIM:SEG <bytes>                   bytes per TCP segment (SIM_SEGMENT)
    :SIM:SEGD <seconds>                delay between segments(SIM_SEGMENT_DELAY)
    :SIM:TAILD <seconds>               extra delay before the last segment (SIM_TAIL_DELAY)
    :SIM:DROP <bytes>                  drop the connection after N block bytes, 0=off (SIM_DROP_AFTER)
    :SIM:RESPD <seconds>               delay before every query response (SIM_RESPONSE_DELAY)
    :SIM:SEED <n>                      noise waveform seed    (SIM_SEED)
    :SIM:RESET                         restore env defaults
    :SIM:STATS?                        "connections=..,commands=..,data_queries=.."
    :SIM:CONFIG?                       current simulator configuration

Waveform payloads deliberately contain 0x0A / 0x0D bytes: clients must parse the
binary block by length, never by lines.
"""
from __future__ import annotations

import asyncio
import math
import os
import random
import socket
import struct

POINTS = 4096
CHANNELS = (1, 2)


def _env_f(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def _env_i(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


class SimConfig:
    """Runtime-tunable simulator behaviour (shared by all connections)."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.waveform = os.environ.get("SIM_WAVEFORM", "sine").strip().lower()
        self.segment = max(1, _env_i("SIM_SEGMENT", 1024))
        self.segment_delay = max(0.0, _env_f("SIM_SEGMENT_DELAY", 0.0))
        self.tail_delay = max(0.0, _env_f("SIM_TAIL_DELAY", 0.0))
        self.drop_after = max(0, _env_i("SIM_DROP_AFTER", 0))
        self.response_delay = max(0.0, _env_f("SIM_RESPONSE_DELAY", 0.0))
        self.seed = _env_i("SIM_SEED", 42)
        # Fixed acquisition constants of the simulated instrument.
        self.xincr = 1e-6
        self.ymult = {1: 1e-2, 2: 2.5e-2}
        self.yoff = {1: 0.0, 2: 0.5}

    def describe(self) -> str:
        return (
            f"waveform={self.waveform},segment={self.segment},"
            f"segment_delay={self.segment_delay},tail_delay={self.tail_delay},"
            f"drop_after={self.drop_after},response_delay={self.response_delay},"
            f"seed={self.seed}"
        )


def generate_waveform(kind: str, channel: int, points: int = POINTS, seed: int = 42) -> list[int]:
    """Deterministic raw int16 samples for a waveform kind / channel."""
    phase = 0.0 if channel == 1 else math.pi / 4
    shift = 0 if channel == 1 else 512
    out: list[int] = []
    if kind == "sine":
        for i in range(points):
            out.append(int(round(12000 * math.sin(2 * math.pi * 8 * i / points + phase))))
    elif kind == "square":
        for i in range(points):
            out.append(10000 if ((i + shift) // 256) % 2 == 0 else -10000)
    elif kind == "ramp":
        for i in range(points):
            pos = (i + shift) % points
            out.append(-12000 + round(24000 * pos / (points - 1)))
    elif kind == "noise":
        rng = random.Random(seed * 1000 + channel)
        out = [rng.randint(-12000, 12000) for _ in range(points)]
    else:
        out = [0] * points
    return out


def samples_to_bytes(samples: list[int]) -> bytes:
    return struct.pack(f"<{len(samples)}h", *samples)


def ieee488_block(payload: bytes) -> bytes:
    """Definite-length block: '#' <n digits> <length> <payload>."""
    length = str(len(payload)).encode("ascii")
    return b"#" + str(len(length)).encode("ascii") + length + payload


class ConnState:
    def __init__(self) -> None:
        self.channel = 1
        self.frozen: dict[int, bytes] | None = None  # captured at :STOP


async def send_block(writer: asyncio.StreamWriter, cfg: SimConfig, payload: bytes) -> bool:
    """Send the IEEE488.2 block in segments. Returns False if the connection
    must be dropped mid-transfer (SIM_DROP_AFTER)."""
    block = ieee488_block(payload)
    total = len(block)
    if cfg.response_delay > 0:
        await asyncio.sleep(cfg.response_delay)
    seg = max(1, cfg.segment)
    sent = 0
    while sent < total:
        end = min(sent + seg, total)
        if cfg.drop_after and end > cfg.drop_after:
            end = cfg.drop_after
        if end > sent:
            writer.write(block[sent:end])
            await writer.drain()
            sent = end
        if cfg.drop_after and sent >= cfg.drop_after:
            return False  # simulate a mid-transfer disconnect
        if sent < total:
            if sent + seg >= total and cfg.tail_delay > 0:
                await asyncio.sleep(cfg.tail_delay)  # delayed tail segment
            elif cfg.segment_delay > 0:
                await asyncio.sleep(cfg.segment_delay)
    writer.write(b"\n")  # many real instruments append a trailing newline
    await writer.drain()
    return True


async def reply(writer: asyncio.StreamWriter, cfg: SimConfig, text: str) -> None:
    if cfg.response_delay > 0:
        await asyncio.sleep(cfg.response_delay)
    writer.write(text.encode("ascii") + b"\n")
    await writer.drain()


def _parse_channel(arg: str) -> int | None:
    digits = "".join(ch for ch in arg if ch.isdigit())
    if not digits:
        return None
    ch = int(digits)
    return ch if ch in CHANNELS else None


async def process_command(
    raw: bytes,
    state: ConnState,
    cfg: SimConfig,
    stats: dict,
    writer: asyncio.StreamWriter,
) -> bool:
    """Handle one command line. Returns False if the connection should close."""
    line = raw.decode("ascii", errors="replace").strip().rstrip("\r")
    if not line:
        return True
    stats["commands"] += 1
    parts = line.split(None, 1)
    name = parts[0].upper()
    arg = parts[1].strip() if len(parts) > 1 else ""
    if not name.startswith(":") and not name.startswith("*"):
        name = ":" + name
    name = name.replace(":SIMULATE:", ":SIM:")

    if name == "*IDN?":
        await reply(writer, cfg, "SIMSCOPE,GSB-4096,000001,1.0")
    elif name == ":STOP":
        state.frozen = {
            ch: samples_to_bytes(generate_waveform(cfg.waveform, ch, POINTS, cfg.seed))
            for ch in CHANNELS
        }
    elif name == ":RUN":
        state.frozen = None
    elif name == ":ACQUIRE:STATE?":
        await reply(writer, cfg, "STOP" if state.frozen is not None else "RUN")
    elif name == ":WAVEFORM:SOURCE?":
        await reply(writer, cfg, f"CHANnel{state.channel}")
    elif name == ":WAVEFORM:SOURCE":
        ch = _parse_channel(arg)
        if ch is not None:
            state.channel = ch
    elif name == ":WAVEFORM:XINCR?":
        await reply(writer, cfg, f"{cfg.xincr:.6E}")
    elif name == ":WAVEFORM:YMULT?":
        await reply(writer, cfg, f"{cfg.ymult[state.channel]:.6E}")
    elif name == ":WAVEFORM:YOFF?":
        await reply(writer, cfg, f"{cfg.yoff[state.channel]:.6E}")
    elif name == ":WAVEFORM:POINTS?":
        await reply(writer, cfg, str(POINTS))
    elif name == ":WAVEFORM:DATA?":
        stats["data_queries"] += 1
        print(f"[sim] DATA? seg_delay={cfg.segment_delay} tail={cfg.tail_delay} drop={cfg.drop_after}", flush=True)
        if state.frozen is not None:
            payload = state.frozen[state.channel]
        else:
            payload = samples_to_bytes(generate_waveform(cfg.waveform, state.channel, POINTS, cfg.seed))
        ok = await send_block(writer, cfg, payload)
        if not ok:
            print(f"[sim] dropping connection mid-transfer after {cfg.drop_after} bytes", flush=True)
            return False
    elif name == ":SIM:WAVE":
        if arg.lower() in ("sine", "square", "ramp", "noise"):
            cfg.waveform = arg.lower()
    elif name == ":SIM:SEG":
        cfg.segment = max(1, int(arg))
    elif name == ":SIM:SEGD":
        cfg.segment_delay = max(0.0, float(arg))
    elif name == ":SIM:TAILD":
        cfg.tail_delay = max(0.0, float(arg))
    elif name == ":SIM:DROP":
        cfg.drop_after = max(0, int(arg))
    elif name == ":SIM:RESPD":
        cfg.response_delay = max(0.0, float(arg))
    elif name == ":SIM:SEED":
        cfg.seed = int(arg)
    elif name == ":SIM:RESET":
        cfg.reset()
    if name.startswith(":SIM:") and not name.endswith("?"):
        print(f"[sim] config: {cfg.describe()}", flush=True)
    elif name == ":SIM:STATS?":
        await reply(
            writer,
            cfg,
            f"connections={stats['connections']},commands={stats['commands']},"
            f"data_queries={stats['data_queries']}",
        )
    elif name == ":SIM:CONFIG?":
        await reply(writer, cfg, cfg.describe())
    # Unknown commands and non-queries get no response, like a real instrument.
    return True


async def handle_connection(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, cfg: SimConfig, stats: dict
) -> None:
    stats["connections"] += 1
    peer = writer.get_extra_info("peername")
    sock = writer.get_extra_info("socket")
    if sock is not None:
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
    print(f"[sim] connection #{stats['connections']} from {peer}", flush=True)
    state = ConnState()
    buf = b""
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                keep_open = await process_command(line, state, cfg, stats, writer)
                if not keep_open:
                    return
    except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError):
        pass  # client went away mid-transfer; nothing to clean up specially
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        print(f"[sim] connection from {peer} closed", flush=True)


async def main() -> None:
    cfg = SimConfig()
    stats = {"connections": 0, "commands": 0, "data_queries": 0}
    host = os.environ.get("SIM_HOST", "0.0.0.0")
    port = int(os.environ.get("SIM_PORT", "5025"))
    server = await asyncio.start_server(
        lambda r, w: handle_connection(r, w, cfg, stats), host, port
    )
    print(f"[sim] SCPI oscilloscope simulator listening on {host}:{port}", flush=True)
    print(f"[sim] config: {cfg.describe()}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

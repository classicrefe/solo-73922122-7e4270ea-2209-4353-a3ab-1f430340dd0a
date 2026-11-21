"""Instrument client: one acquisition round over one dedicated TCP connection.

Design notes
------------
* Every acquisition round opens a FRESH TCP connection and closes it when the
  round ends (success, timeout, cancel or disconnect). Late bytes arriving after
  a round is over therefore land in a dead socket and can never be mistaken for
  the response of a later acquisition.
* Cleanup is per-round: ``InstrumentRound.close()`` only ever closes its own
  writer and nulls the reference first, so a stale cleanup path can never close
  a connection belonging to a newer round.
* Each command/response exchange is bounded by ``timeout`` seconds
  (default 5 s per round-trip).
"""
from __future__ import annotations

import asyncio
import socket
from dataclasses import dataclass

POINTS = 4096
SAMPLE_BYTES = POINTS * 2  # 4096 little-endian int16 samples


class AcquisitionError(Exception):
    """Instrument communication failed (timeout, disconnect, bad data)."""


class AcquisitionCancelled(Exception):
    """The acquisition was cancelled while a round was in flight."""


@dataclass
class CaptureResult:
    sample_interval: float
    volt_scale: float
    volt_offset: float
    points: int
    samples: bytes


class InstrumentRound:
    """One dedicated connection for exactly one acquisition round."""

    def __init__(self, host: str, port: int, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def connect(self) -> None:
        await self._bounded(self._open())

    async def _open(self) -> None:
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        # SCPI is a request/response protocol with small back-to-back writes;
        # disable Nagle so commands are not held back by delayed ACKs.
        sock = self._writer.get_extra_info("socket")
        if sock is not None:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass

    async def _bounded(self, coro):
        try:
            return await asyncio.wait_for(coro, self.timeout)
        except asyncio.TimeoutError:
            raise AcquisitionError(
                f"instrument round timed out after {self.timeout:g}s"
            ) from None
        except (ConnectionResetError, BrokenPipeError, asyncio.IncompleteReadError) as exc:
            raise AcquisitionError(f"connection lost during round: {exc!r}") from None
        except OSError as exc:
            raise AcquisitionError(f"connection failed: {exc}") from None

    async def _send(self, cmd: str) -> None:
        assert self._writer is not None
        self._writer.write(cmd.encode("ascii") + b"\n")
        await self._writer.drain()

    async def _readline(self) -> str:
        assert self._reader is not None
        line = await self._reader.readline()
        if not line:
            raise AcquisitionError("connection closed by instrument")
        return line.decode("ascii", errors="replace").strip()

    async def command(self, cmd: str) -> None:
        """Send a command that has no response."""
        await self._bounded(self._send(cmd))

    async def query_line(self, cmd: str) -> str:
        """Send a query and read one text response line."""

        async def exchange() -> str:
            await self._send(cmd)
            return await self._readline()

        return await self._bounded(exchange())

    async def query_block(self, cmd: str) -> bytes:
        """Send a query and read one IEEE 488.2 definite-length binary block.

        The payload is read by exact byte count, never by lines, because the
        binary samples may contain 0x0A / 0x0D bytes.
        """

        async def exchange() -> bytes:
            await self._send(cmd)
            assert self._reader is not None
            head = await self._reader.readexactly(2)
            if head[0:1] != b"#" or not head[1:2].isdigit():
                raise AcquisitionError(f"invalid block header: {head!r}")
            ndigits = int(head[1:2])
            if ndigits == 0 or ndigits > 6:
                raise AcquisitionError(f"unsupported block header: {head!r}")
            length_raw = await self._reader.readexactly(ndigits)
            length = int(length_raw)
            if length <= 0 or length > 16 * 1024 * 1024:
                raise AcquisitionError(f"implausible block length: {length}")
            return await self._reader.readexactly(length)

        return await self._bounded(exchange())

    def close(self) -> None:
        """Close this round's connection. Idempotent; only ever touches THIS
        round's writer, never a connection owned by a newer round."""
        writer, self._writer = self._writer, None
        self._reader = None
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass


async def _cancellable(coro, cancel_event: asyncio.Event):
    """Run an exchange; abort it promptly if the cancel event is set."""
    if cancel_event.is_set():
        raise AcquisitionCancelled()
    task = asyncio.ensure_future(coro)
    waiter = asyncio.ensure_future(cancel_event.wait())
    try:
        await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
        if waiter.done() and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            raise AcquisitionCancelled()
        return task.result()  # may raise AcquisitionError
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


async def run_acquisition(
    host: str,
    port: int,
    channel: int,
    timeout: float,
    cancel_event: asyncio.Event,
) -> CaptureResult:
    """Freeze a capture and read parameters + the full sample block within one
    communication round, so parameters and samples belong to the same capture.

    On cancel/timeout/disconnect the round's connection is closed and dropped;
    nothing is left behind that could pollute a later acquisition.
    """
    rnd = InstrumentRound(host, port, timeout)
    try:
        await _cancellable(rnd.connect(), cancel_event)
        # Freeze the capture first; everything below reads the frozen record.
        await _cancellable(rnd.command(":STOP"), cancel_event)
        await _cancellable(rnd.command(f":WAVeform:SOURce CHANnel{channel}"), cancel_event)
        xincr = float(await _cancellable(rnd.query_line(":WAVeform:XINcr?"), cancel_event))
        ymult = float(await _cancellable(rnd.query_line(":WAVeform:YMUlt?"), cancel_event))
        yoff = float(await _cancellable(rnd.query_line(":WAVeform:YOFf?"), cancel_event))
        points = int(await _cancellable(rnd.query_line(":WAVeform:POINts?"), cancel_event))
        if points != POINTS:
            raise AcquisitionError(f"unexpected point count: {points}")
        data = await _cancellable(rnd.query_block(":WAVeform:DATA?"), cancel_event)
        if len(data) != points * 2:
            raise AcquisitionError(
                f"incomplete block: got {len(data)} bytes, expected {points * 2}"
            )
        return CaptureResult(xincr, ymult, yoff, points, data)
    finally:
        rnd.close()

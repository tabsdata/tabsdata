#
# Copyright 2026 Tabs Data Inc.
#

"""Subprocess wrapper for the `td_transporter` binary.

Per Q8 of the migration plan, ``td_transporter`` is on ``PATH`` and is
a black box. This module exec's it with ``--request <PATH>`` and
``--report <PATH>`` and forwards every stdout/stderr line through the
``tabsdatak`` logger as it streams.

Two side effects of the stream-and-log pattern:

1. The parent's log already contains the transporter's output, so
   failures don't need a separate inspection step on the transporter
   stdio.
2. Every forwarded line is a log record, which the liveness logging
   handler counts as evidence of progress — long-running transfers
   tick the liveness file naturally, without a separate heartbeat.

Buffering: pipes are opened in text mode with ``bufsize=1`` (line
buffered on the parent side) so each line ships as Python reads it.
The transporter binary itself may block-buffer stdout when it sees a
non-tty; that's the binary's choice and outside our control. Stderr
is conventionally line-buffered by most well-behaved CLI tools.

Used by the ``run-transporter`` command to actually copy parquet files.
"""

from __future__ import annotations

import logging
import subprocess
import sys
import threading
from pathlib import Path
from typing import IO, Sequence

logger = logging.getLogger(__name__)


def _binary_name() -> str:
    """Return the transporter binary name for the current platform."""
    if sys.platform == "win32":
        return "td_transporter.exe"
    return "td_transporter"


TRANSPORTER_BINARY = _binary_name()


class TransporterNotFoundError(FileNotFoundError):
    """The ``td_transporter`` binary is not on ``PATH``."""


def run_transporter(request_yaml: Path, report: Path) -> int:
    """Exec ``td_transporter --request <request_yaml> --report <report>``.

    Returns the binary's exit code. The binary writes its execution
    report to ``report``; that file must not already exist.

    Every stdout/stderr line the binary emits is forwarded through the
    module logger (stdout at INFO, stderr at WARNING) — which both
    captures it for diagnostics and ticks any installed liveness
    handler.

    Raises:
        TransporterNotFoundError: if ``td_transporter`` is not on ``PATH``.
    """
    cmd = [
        TRANSPORTER_BINARY,
        "--request",
        str(request_yaml),
        "--report",
        str(report),
    ]
    return _run(cmd)


def _stream_reader(stream: IO[str], level: int) -> threading.Thread:
    """Spawn a daemon thread that forwards each line of ``stream``
    through the module logger at ``level``.

    Each stream needs its own reader thread because without one, the
    subprocess could block writing to a full pipe and hang. Daemon =
    True so a stuck reader doesn't keep the interpreter alive on exit.
    """

    def _loop() -> None:
        try:
            for line in stream:
                logger.log(level, "td_transporter: %s", line.rstrip())
        finally:
            stream.close()

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    return t


def _run(cmd: Sequence[str]) -> int:
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            text=True,
        )
    except FileNotFoundError as exc:
        raise TransporterNotFoundError(
            f"{TRANSPORTER_BINARY!r} not found on PATH"
        ) from exc

    assert proc.stdout is not None
    assert proc.stderr is not None
    stdout_reader = _stream_reader(proc.stdout, logging.INFO)
    stderr_reader = _stream_reader(proc.stderr, logging.WARNING)

    rc = proc.wait()
    stdout_reader.join()
    stderr_reader.join()
    return rc

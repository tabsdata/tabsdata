#
# Copyright 2026 Tabs Data Inc.
#

"""Subprocess wrapper for the `td_transporter` binary.

Per Q8 of the migration plan, ``td_transporter`` is a black box that
ships inside the function-runner's venv (installed via the same pip
flow as the rest of the runner). This module exec's it with
``--request <PATH>`` and ``--report <PATH>`` and forwards every
stdout/stderr line through the ``tabsdatak`` logger as it streams,
unformatted so the binary's output keeps its own format.

Binary resolution: we resolve the binary against the active
interpreter's scripts dir (``sysconfig.get_path('scripts')``) rather
than relying on ``PATH`` lookup. The contract is that the pod-runner
activates the function-runner venv before invoking us, so the binary
is always in ``<venv>/bin/`` (or ``<venv>/Scripts/`` on Windows).
Pre-resolving via the venv's scripts dir avoids two failure modes
that a bare ``PATH`` lookup would hide: a missing-activation incident
(PATH doesn't include the venv) and the silently-shadowed-binary
case (a same-named ``td_transporter`` elsewhere on PATH).

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
import os
import subprocess
import threading
from pathlib import Path
from typing import IO, Mapping, Sequence

from tabsdatak.common._interpreter.python.error import BinaryNotFoundError
from tabsdatak.common._interpreter.python.venv import PythonEnvironment

logger = logging.getLogger(__name__)

TRANSPORTER_BINARY_NAME = "td_transporter"

SECRETS_MOUNT_OPTION_PREFIX = "SECRETS__STORAGE__MOUNTS__"
OPTIONS_INFIX = "__OPTIONS__"

OPTION_KEY_TO_TRANSPORTER_FIELD = {
    "AWS_ACCESS_KEY_ID": "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION": "AWS_REGION",
    "AZURE_STORAGE_ACCOUNT_NAME": "AZURE_ACCOUNT_NAME",
    "AZURE_STORAGE_ACCOUNT_KEY": "AZURE_ACCOUNT_KEY",
    "GOOGLE_SERVICE_ACCOUNT_KEY": "GCP_SERVICE_ACCOUNT_KEY",
}


def transporter_environment(source: Mapping[str, str]) -> dict[str, str]:
    environment = dict(source)
    for name, value in source.items():
        if not name.startswith(SECRETS_MOUNT_OPTION_PREFIX):
            continue
        remainder = name[len(SECRETS_MOUNT_OPTION_PREFIX) :]
        separator = remainder.find(OPTIONS_INFIX)
        if separator == -1:
            continue
        mount_id = remainder[:separator]
        option_key = remainder[separator + len(OPTIONS_INFIX) :]
        transporter_field = OPTION_KEY_TO_TRANSPORTER_FIELD.get(option_key)
        if transporter_field is None:
            logger.warning(
                f"No transporter field mapping for storage option '{option_key}' "
                f"on mount '{mount_id}'; leaving it unmapped."
            )
            continue
        environment.setdefault(f"{mount_id}_{transporter_field}", value)
    return environment


class TransporterNotFoundError(FileNotFoundError):
    """The ``td_transporter`` binary is not in the active venv's scripts dir."""


def run_transporter(request_yaml: Path, report: Path) -> int:
    """Exec ``td_transporter --request <request_yaml> --report <report>``.

    Returns the binary's exit code. The binary writes its execution
    report to ``report``; that file must not already exist.

    Every stdout/stderr line the binary emits is forwarded through the
    module logger unformatted — which both captures it for diagnostics,
    preserving the binary's own output format, and ticks any installed
    liveness handler.

    Raises:
        TransporterNotFoundError: if ``td_transporter`` is not in the
            active interpreter's scripts dir.
    """
    try:
        binary = PythonEnvironment().binary(TRANSPORTER_BINARY_NAME)
    except BinaryNotFoundError as error:
        raise TransporterNotFoundError() from error
    # The report path's parent dir may not exist yet under the
    # publisher init-container chain — the `shared/shuttle/response/`
    # subdir of the shared emptyDir starts out empty (the
    # image-template-provided dir is overlaid by the emptyDir mount).
    report.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        str(binary),
        "--request",
        str(request_yaml),
        "--report",
        str(report),
    ]
    return _run(cmd)


def _stream_reader(stream: IO[str], level: int) -> threading.Thread:
    """Spawn a daemon thread that forwards each line of ``stream``
    through the module logger at ``level``, preserving the binary's own
    format. stdout is forwarded at INFO, stderr at WARNING.

    Each stream needs its own reader thread because without one, the
    subprocess could block writing to a full pipe and hang. Daemon =
    True so a stuck reader doesn't keep the interpreter alive on exit.
    """

    def _loop() -> None:
        try:
            for line in stream:
                logger.log(level, line.rstrip(), extra={"noformat": True})
        finally:
            stream.close()

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    return t


def _run(cmd: Sequence[str]) -> int:
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
        text=True,
        env=transporter_environment(os.environ),
    )

    assert proc.stdout is not None
    assert proc.stderr is not None
    stdout_reader = _stream_reader(proc.stdout, logging.INFO)
    stderr_reader = _stream_reader(proc.stderr, logging.WARNING)

    rc = proc.wait()
    stdout_reader.join()
    stderr_reader.join()
    return rc

#
# Copyright 2026 Tabs Data Inc.
#

"""Subprocess wrapper for the `td_transporter` binary.

Per Q8 of the migration plan, `td_transporter` is on `PATH` and is a
black box. This module just exec's it with `--request <PATH>` and
`--report <PATH>` and forwards stdout/stderr; the caller decides what
to do with the returned exit code.

The binary refuses to overwrite an existing `--report` file (per its
`non_existing_file` value parser), so `report` must not already exist.
Each caller threads its own report path through — typically a fresh
file in the same temp / state directory it controls.

Used by the `run-transporter` command (steps 4b and 7b of the publisher
chain) to actually copy parquet files.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import ContextManager, Sequence


def _binary_name() -> str:
    """Return the transporter binary name for the current platform."""
    if sys.platform == "win32":
        return "td_transporter.exe"
    return "td_transporter"


TRANSPORTER_BINARY = _binary_name()


class TransporterNotFoundError(FileNotFoundError):
    """The `td_transporter` binary is not on `PATH`."""


def run_transporter(
    request_yaml: Path,
    report: Path,
    liveness: ContextManager[object] | None = None,
) -> int:
    """Exec `td_transporter --request <request_yaml> --report <report>`.

    Returns the binary's exit code.

    The binary is expected on `PATH`. stdout/stderr are inherited by the
    parent. `report` is a file path the binary writes its execution
    report to; it must not already exist. If `liveness` is provided,
    it's used as a context manager around the subprocess call (started
    before, stopped after — including on exception).

    Raises:
        TransporterNotFoundError: if `td_transporter` is not on `PATH`.
    """
    cmd = [
        TRANSPORTER_BINARY,
        "--request",
        str(request_yaml),
        "--report",
        str(report),
    ]
    if liveness is None:
        return _run(cmd)
    with liveness:
        return _run(cmd)


def _run(cmd: Sequence[str]) -> int:
    try:
        result = subprocess.run(cmd)
    except FileNotFoundError as exc:
        raise TransporterNotFoundError(
            f"{TRANSPORTER_BINARY!r} not found on PATH"
        ) from exc
    return result.returncode

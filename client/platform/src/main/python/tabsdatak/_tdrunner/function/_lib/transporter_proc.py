#
# Copyright 2026 Tabs Data Inc.
#

"""Subprocess wrapper for the `td_transporter` binary.

Per Q8 of the migration plan, `td_transporter` is on `PATH` and is a
black box. This module just exec's it with `--request <PATH>` and
forwards stdout/stderr; the caller decides what to do with the returned
exit code.

Used by the `run-transporter` command (steps 4b and 7b of the publisher
chain) to actually copy parquet files.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import ContextManager, Sequence

TRANSPORTER_BINARY = "td_transporter"


class TransporterNotFoundError(FileNotFoundError):
    """The `td_transporter` binary is not on `PATH`."""


def run_transporter(
    request_yaml: Path,
    liveness: ContextManager[object] | None = None,
) -> int:
    """Exec `td_transporter --request <request_yaml>` and return its exit code.

    The binary is expected on `PATH`. stdout/stderr are inherited by the
    parent. If `liveness` is provided, it's used as a context manager
    around the subprocess call (started before, stopped after — including
    on exception).

    Raises:
        TransporterNotFoundError: if `td_transporter` is not on `PATH`.
    """
    cmd = [TRANSPORTER_BINARY, "--request", str(request_yaml)]
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

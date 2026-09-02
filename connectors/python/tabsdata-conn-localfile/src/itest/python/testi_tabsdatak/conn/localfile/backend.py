#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from domain fixture to connector connections and local scratch trees"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain
from tabsdatax.itest.fixture.monikers import moniker

from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter.handler.handler2 import TRANSPORTER_BINARY_NAME
from tabsdatak.conn.localfile import LocalFileDestConn, LocalFileSrcConn

# The plugins shell out to this binary; resolving it at import time fails loudly
# on an unhydrated environment instead of mid-test.
TRANSPORTER_BINARY = PythonEnvironment().binary(TRANSPORTER_BINARY_NAME)


def src_conn(base_path: Path) -> LocalFileSrcConn:
    return LocalFileSrcConn(base_path=_absolute(base_path))


def dest_conn(base_path: Path) -> LocalFileDestConn:
    return LocalFileDestConn(base_path=_absolute(base_path))


def scratch(
    domain: FsDomain, request: pytest.FixtureRequest
) -> tuple[Path, Path, Path]:
    """A per-test ``(root, base, work)`` trio under the domain's own directory:
    ``base`` is what a connection is rooted at, ``work`` the plugin's work dir and
    ``root`` holds anything that must stay outside the connection."""
    root = domain.base_path / moniker(request)
    base = root / "store"
    work = root / "work"
    base.mkdir(parents=True)
    work.mkdir(parents=True)
    return root, base, work


def stage_parquet(base: Path, name: str, when: datetime | None = None) -> Path:
    """A one-row parquet at ``base/name`` whose single ``name`` column repeats that
    relative name, so a test can tell which file an imported frame came from.
    ``when``, if given, becomes the file's last-modified time -- what the
    incremental tests measure against ``initial_last_modified``."""
    path = base / name
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"name": name}]).write_parquet(path, use_pyarrow=True)
    if when is not None:
        stamp = when.astimezone(timezone.utc).timestamp()
        os.utime(path, (stamp, stamp))
    return path


def _absolute(base_path: Path) -> str:
    text = Path(base_path).as_posix()
    return text if re.match(r"[A-Za-z]:/", text) else "/" + text.strip("/")

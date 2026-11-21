#
#  Copyright 2026 Tabsdata Inc.
#

"""One folder per harvested session, named ``<UTC stamp>-<pid>`` under the
base folder, so concurrent sessions never share or delete each other's data."""

from __future__ import annotations

import os
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path

import psutil

from tabsdatax.pytesting.plugins.harvester.internal.constants import (
    TDX_HARVESTER_FOLDER_DEF,
    TDX_HARVESTER_FOLDER_ENV,
)

SESSION_PATTERN = re.compile(r"^\d{8}T\d{6}Z-(\d+)$")


def base_folder(folder: str | None = None) -> Path:
    chosen = (
        folder or os.environ.get(TDX_HARVESTER_FOLDER_ENV) or TDX_HARVESTER_FOLDER_DEF
    )
    return Path(chosen).absolute()


def new_session_id() -> str:
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{os.getpid()}"


def create_session(base: Path) -> Path:
    folder = base / new_session_id()
    folder.mkdir(parents=True)
    return folder


def list_sessions(base: Path) -> list[str]:
    if not base.is_dir():
        return []
    return sorted(
        entry.name
        for entry in base.iterdir()
        if entry.is_dir() and SESSION_PATTERN.match(entry.name)
    )


def session_pid(session: str) -> int:
    match = SESSION_PATTERN.match(session)
    return int(match.group(1)) if match else 0


def prune_sessions(base: Path, keep: int, current: str) -> None:
    sessions = [name for name in list_sessions(base) if name != current]
    for name in sessions[: max(0, len(sessions) - keep)]:
        if psutil.pid_exists(session_pid(name)):
            continue
        shutil.rmtree(base / name, ignore_errors=True)

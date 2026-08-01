#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os

TD_BEFORE_PATH = "TD_BEFORE_PATH"
TD_AFTER_PATH = "TD_AFTER_PATH"

PATH = "PATH"


def extend_path() -> None:
    extended = extended_path(
        os.environ.get(PATH),
        os.environ.get(TD_BEFORE_PATH),
        os.environ.get(TD_AFTER_PATH),
    )
    if extended is not None:
        os.environ[PATH] = extended


def extended_path(
    path: str | None,
    before: str | None,
    after: str | None,
) -> str | None:
    leading = entries(before)
    trailing = entries(after)
    if not leading and not trailing:
        return None
    extended: list[str] = []
    for entry in leading + entries(path) + trailing:
        if entry not in extended:
            extended.append(entry)
    return os.pathsep.join(extended)


def entries(value: str | None) -> list[str]:
    if not value:
        return []
    return [entry for entry in value.split(os.pathsep) if entry]

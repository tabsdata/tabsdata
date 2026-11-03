#
# Copyright 2026 Tabsdata Inc.
#

"""The files a source takes on a run, out of those listed in its directory.

Matching, on file names only:

- ``*`` matches any run of characters;
- ``?`` matches one character;
- any other character matches itself.

With a cursor, matched files are walked oldest first (ties by name):

- selected: those the cursor takes, up to ``max_files``;
- purgeable: those it turns down that are not older than its initial time,
  consumed by an earlier run.

Without a cursor:

- selected: the oldest ``max_files`` matches;
- purgeable: none.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime

from tabsdatak.conn.common._cursor import LastModifiedCursor

PATH_SEPARATOR = "/"


class FindError(ValueError):
    pass


@dataclass(frozen=True)
class Listed:
    name: str
    last_modified: datetime


@dataclass(frozen=True)
class Found:
    selected: list[Listed] = field(default_factory=list)
    purgeable: list[Listed] = field(default_factory=list)


def split_pattern(path: str) -> tuple[str, str]:
    directory, _, pattern = path.rpartition(PATH_SEPARATOR)
    return directory, pattern


def wildcard(pattern: str) -> re.Pattern[str]:
    if not pattern:
        raise FindError("The path names no file")
    translated = "".join(
        ".*" if char == "*" else "." if char == "?" else re.escape(char)
        for char in pattern
    )
    return re.compile(translated, re.DOTALL)


def find(
    listed: Iterable[Listed],
    pattern: str,
    cursor: LastModifiedCursor | None,
    max_files: int | None = None,
) -> Found:
    matcher = wildcard(pattern)
    matched = sorted(
        (file for file in listed if matcher.fullmatch(file.name)),
        key=lambda file: (file.last_modified, file.name),
    )
    if cursor is None:
        return Found(selected=matched if max_files is None else matched[:max_files])
    found = Found()
    for file in matched:
        if max_files is not None and len(found.selected) >= max_files:
            break
        if cursor.take(pattern, file.name, file.last_modified):
            found.selected.append(file)
        elif cursor.at_or_after_initial(file.last_modified):
            found.purgeable.append(file)
    return found

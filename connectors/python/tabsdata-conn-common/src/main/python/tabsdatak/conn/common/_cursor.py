#
# Copyright 2026 Tabsdata Inc.
#

"""An incremental cursor over the files a pattern matches, by last-modified
time.

Per pattern, it keeps the latest last-modified time taken and the names taken
at that time: a file is taken when it is newer than that time, or as old and
not taken yet, so files sharing a time are each taken once. Before a pattern
has taken anything, a file is taken from the cursor's initial time on.

It is stored as a base64 JSON document (``{"V1": {"initial_last_modified":
..., "entries": {pattern: [time, [names]]}}}``), the format the file-based
connectors have kept their incremental state in, so a state stored by any of
them reads back unchanged.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

VERSION = "V1"
INITIAL_KEY = "initial_last_modified"
ENTRIES_KEY = "entries"
UTC_SUFFIX = "Z"
FRACTION_DIGITS = 6


@dataclass
class Taken:
    last_modified: datetime
    names: list[str]


@dataclass
class LastModifiedCursor:
    initial: datetime
    entries: dict[str, Taken] = field(default_factory=dict)

    @classmethod
    def decode(cls, value: str | None, initial: datetime) -> LastModifiedCursor:
        if value is None:
            return cls(initial)
        padded = value + "=" * (-len(value) % 4)
        state = json.loads(base64.b64decode(padded))[VERSION]
        return cls(
            instant(state[INITIAL_KEY]),
            {
                pattern: Taken(instant(last_modified), list(names))
                for pattern, (last_modified, names) in state[ENTRIES_KEY].items()
            },
        )

    def encode(self) -> str:
        state = {
            VERSION: {
                INITIAL_KEY: text(self.initial),
                ENTRIES_KEY: {
                    pattern: [text(taken.last_modified), taken.names]
                    for pattern, taken in self.entries.items()
                },
            },
        }
        encoded = base64.b64encode(json.dumps(state).encode())
        return encoded.decode().rstrip("=")

    def take(self, pattern: str, name: str, last_modified: datetime) -> bool:
        taken = self.entries.get(pattern)
        if taken is None:
            if last_modified < self.initial:
                return False
            self.entries[pattern] = Taken(last_modified, [name])
            return True
        if last_modified < taken.last_modified:
            return False
        if last_modified > taken.last_modified:
            self.entries[pattern] = Taken(last_modified, [name])
            return True
        if name in taken.names:
            return False
        taken.names.append(name)
        return True


# A stored time may carry nanoseconds; a datetime holds microseconds, as store
# listings report them, so the extra digits are dropped.
def instant(value: str) -> datetime:
    stamp = value.removesuffix(UTC_SUFFIX)
    if "." in stamp:
        seconds, fraction = stamp.split(".", 1)
        offset = ""
        for sign in ("+", "-"):
            if sign in fraction:
                fraction, offset = fraction.split(sign, 1)
                offset = f"{sign}{offset}"
        stamp = f"{seconds}.{fraction[:FRACTION_DIGITS]:0<{FRACTION_DIGITS}}{offset}"
    parsed = datetime.fromisoformat(stamp)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def text(value: datetime) -> str:
    utc = value.astimezone(timezone.utc).replace(tzinfo=None)
    return f"{utc.isoformat(timespec='microseconds')}{UTC_SUFFIX}"

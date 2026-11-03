#
# Copyright 2026 Tabsdata Inc.
#

"""An incremental cursor over the files a pattern matches, by last-modified
time.

Per pattern it keeps:

- the latest last-modified time taken;
- the names taken at that time.

A file is taken when:

- the pattern has taken nothing yet and the file is not older than the
  cursor's initial time;
- it is newer than the latest time taken;
- it is as old as the latest time taken and its name was not taken yet, so
  files sharing a time are each taken once.

Storage, as unpadded base64 JSON:

- written: the format released sources keep their state in::

    {
      "initial": "<time>",
      "entries": {
        "<pattern>": {
          "lastmod": "<time>",
          "names": ["<name>", ...]
        }
      }
    }

- also read: the V1 format::

    {
      "V1": {
        "initial_last_modified": "<time>",
        "entries": {
          "<pattern>": ["<time>", ["<name>", ...]]
        }
      }
    }
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

INITIAL_KEY = "initial"
ENTRIES_KEY = "entries"
LASTMOD_KEY = "lastmod"
NAMES_KEY = "names"
V1_KEY = "V1"
V1_INITIAL_KEY = "initial_last_modified"
UTC_SUFFIX = "Z"
FRACTION_DIGITS = 6
JSON_SEPARATORS = (",", ":")


class CursorError(ValueError):
    pass


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
        stripped = value.rstrip("=")
        try:
            document = base64.b64decode(
                stripped + "=" * (-len(stripped) % 4), validate=True
            )
        except (binascii.Error, ValueError) as error:
            raise CursorError(f"The stored cursor is not base64: {error}") from error
        try:
            state = json.loads(document)
            if V1_KEY in state:
                v1 = state[V1_KEY]
                return cls(
                    instant(v1[V1_INITIAL_KEY]),
                    {
                        pattern: Taken(instant(last_modified), list(names))
                        for pattern, (last_modified, names) in v1[ENTRIES_KEY].items()
                    },
                )
            return cls(
                instant(state[INITIAL_KEY]),
                {
                    pattern: Taken(instant(entry[LASTMOD_KEY]), list(entry[NAMES_KEY]))
                    for pattern, entry in state[ENTRIES_KEY].items()
                },
            )
        except (KeyError, TypeError, ValueError) as error:
            raise CursorError(f"The stored cursor cannot be read: {error}") from error

    def encode(self) -> str:
        state = {
            INITIAL_KEY: text(self.initial),
            ENTRIES_KEY: {
                pattern: {
                    LASTMOD_KEY: text(taken.last_modified),
                    NAMES_KEY: taken.names,
                }
                for pattern, taken in self.entries.items()
            },
        }
        document = json.dumps(state, separators=JSON_SEPARATORS).encode()
        return base64.b64encode(document).decode().rstrip("=")

    def at_or_after_initial(self, last_modified: datetime) -> bool:
        return last_modified >= self.initial

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


# - Nanoseconds are cut to microseconds: a datetime holds no more, nor do store
#   listings.
# - A time without an offset is refused.
def instant(value: str) -> datetime:
    stamp = value.removesuffix(UTC_SUFFIX)
    offset = "+00:00" if value.endswith(UTC_SUFFIX) else ""
    if "." in stamp:
        seconds, fraction = stamp.split(".", 1)
        for sign in ("+", "-"):
            if sign in fraction:
                fraction, rest = fraction.split(sign, 1)
                offset = f"{sign}{rest}"
        stamp = f"{seconds}.{fraction[:FRACTION_DIGITS]:0<{FRACTION_DIGITS}}"
    parsed = datetime.fromisoformat(f"{stamp}{offset}")
    if parsed.tzinfo is None:
        raise ValueError(f"the time {value!r} has no offset")
    return parsed.astimezone(timezone.utc)


# As released sources write a UTC time:
# - whole seconds: no fraction;
# - otherwise: 3 digits when they hold it, else 6.
def text(value: datetime) -> str:
    utc = value.astimezone(timezone.utc)
    seconds = utc.strftime("%Y-%m-%dT%H:%M:%S")
    micros = utc.microsecond
    if micros == 0:
        fraction = ""
    elif micros % 1000 == 0:
        fraction = f".{micros // 1000:03d}"
    else:
        fraction = f".{micros:06d}"
    return f"{seconds}{fraction}{UTC_SUFFIX}"

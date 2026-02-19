#
# Copyright 2026 Tabs Data Inc.
#

import base64
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Entry:
    name: str
    time: str
    files: list[str]


class LastModInfo:
    def __init__(self, lastmod_info: str):
        lastmod_dict = json.loads(base64.b64decode(lastmod_info + "=="))  # Adding
        # padding just in case there was none. There is no need to verify the length
        # of the string to do this, as b64decode takes care of shaving any excess
        # of padding that exists.
        self.data = lastmod_dict

    @property
    def info(self) -> dict:
        return self.data.get("V1", {})

    @property
    def initial_last_modified(self) -> str | None:
        return self.info.get("initial_last_modified")

    @property
    def entries(self) -> list[Entry]:
        raw_entries = self.info.get("entries", {})
        entries = []
        for key, value in raw_entries.items():
            entries.append(Entry(name=key, time=value[0], files=value[1]))
        return entries

    def __repr__(self):
        return f"{self.__class__.__name__}({self.data})"

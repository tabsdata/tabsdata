#
# Copyright 2026 Tabsdata Inc.
#

import base64
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Entry:
    name: str
    time: str
    files: list[str]


VERSION = "V1"


class LastModInfo:
    def __init__(self, lastmod_info: str):
        try:
            lastmod_dict = json.loads(base64.b64decode(lastmod_info + "=="))  # Adding
            # padding just in case there was none. There is no need to verify the length
            # of the string to do this, as b64decode takes care of shaving any excess
            # of padding that exists.
        except ValueError as e:
            raise ValueError(f"Could not decode the last modified info: {e}") from e
        # An envelope this class cannot read used to answer "no entries", which the
        # caller reads as "no previous run" and resolves to the registration time --
        # a silent re-import of everything. The transporter2 cursor is shaped
        # {"initial", "entries"} rather than {"V1": ...} and would land exactly
        # there, so an unknown envelope has to fail instead of going quiet.
        if not isinstance(lastmod_dict, dict):
            raise ValueError(
                "Unreadable last modified info: expected a JSON object, found "
                f"{type(lastmod_dict).__name__}"
            )
        if VERSION not in lastmod_dict:
            raise ValueError(
                f"Unreadable last modified info: expected a {VERSION!r} envelope, "
                f"found the keys {sorted(lastmod_dict)}"
            )
        self.data = lastmod_dict

    @property
    def info(self) -> dict:
        return self.data.get(VERSION, {})

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

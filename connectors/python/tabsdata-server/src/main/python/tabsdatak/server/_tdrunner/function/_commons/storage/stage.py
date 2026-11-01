#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import obstore
from obstore.store import ObjectStore

from tabsdatak.common._storage.transfer import OBJECT_STORES
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)

PATH_SEPARATOR = "/"
ENCODING = "utf-8"

MANIFESTS_FOLDER = "manifests"
FILES_FOLDER = "files"
STREAM_STATE_FILE = "stream-state.json"


class Stage:
    location: Location
    uri: str
    store: ObjectStore

    def __init__(self, location: Location) -> None:
        self.location = location
        self.uri = location.uri.rstrip(PATH_SEPARATOR)
        options = location.mount_declaration().storage_options(self.uri)
        self.store = OBJECT_STORES.store(self.uri, options)

    def locate(self, name: str) -> Location:
        return Location(
            uri=f"{self.uri}{PATH_SEPARATOR}{name}",
            env_prefix=self.location.env_prefix,
        )

    def write(self, name: str, text: str) -> None:
        obstore.put(self.store, name, text.encode(ENCODING))

    def read(self, name: str) -> str | None:
        try:
            result = obstore.get(self.store, name)
        except FileNotFoundError:
            return None
        return bytes(result.bytes()).decode(ENCODING)

    def upload(self, source: Path, name: str) -> None:
        obstore.put(self.store, name, source)

    def discard(self, name: str) -> None:
        try:
            obstore.delete(self.store, name)
        except FileNotFoundError:
            return

    def names(self, folder: str) -> list[str]:
        return sorted(
            item["path"]
            for batch in obstore.list(self.store, prefix=f"{folder}{PATH_SEPARATOR}")
            for item in batch
        )

    def folders(self, folder: str) -> list[str]:
        listing = obstore.list_with_delimiter(self.store, prefix=folder)
        return sorted(
            prefix.rstrip(PATH_SEPARATOR) for prefix in listing["common_prefixes"]
        )

    def next(self, folder: str, offset: str | None) -> str | None:
        names = self.names(folder)
        following = [name for name in names if offset is None or name > offset]
        if following:
            return following[0]
        return None

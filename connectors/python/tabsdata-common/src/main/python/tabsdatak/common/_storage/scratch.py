#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import Protocol

import obstore
from obstore.store import ObjectStore

from tabsdatak.common._storage.ranged import Ranged
from tabsdatak.common._storage.transfer import (
    OBJECT_STORES,
    PATH_SEPARATOR,
    StorageLocation,
)

CHUNK = 8 * 1024 * 1024


class Readable(Protocol):
    def read(self, size: int = ..., /) -> bytes: ...


class ScratchFiles:
    def __init__(self, root: str, options: dict[str, str]) -> None:
        self._root = root.rstrip(PATH_SEPARATOR)
        self._options = options

    def write(self, name: str, source: Readable) -> None:
        if self._exists(name):
            raise FileExistsError(f"The scratch file '{name}' already exists")
        self._put(name, source)

    def replace(self, name: str, source: Readable) -> None:
        self._put(name, source)

    def read(self, name: str) -> Ranged:
        return Ranged(
            StorageLocation(f"{self._root}{PATH_SEPARATOR}{name}", self._options)
        )

    def delete(self, name: str) -> None:
        obstore.delete(self._store(), name)

    def clear(self) -> None:
        store = self._store()
        names = [item["path"] for batch in obstore.list(store) for item in batch]
        if names:
            obstore.delete(store, names)

    def _store(self) -> ObjectStore:
        return OBJECT_STORES.store(self._root, self._options)

    def _exists(self, name: str) -> bool:
        try:
            obstore.head(self._store(), name)
        except FileNotFoundError:
            return False
        return True

    def _put(self, name: str, source: Readable) -> None:
        with obstore.open_writer(self._store(), name) as writer:
            while data := source.read(CHUNK):
                writer.write(data)

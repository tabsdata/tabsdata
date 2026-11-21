#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import errno
import os
import sys
from collections.abc import Iterable
from pathlib import Path

import obstore
from obstore.store import LocalStore, ObjectStore

from tabsdatak.common._storage.transfer import OBJECT_STORES, local_path
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)

PATH_SEPARATOR = "/"
ENCODING = "utf-8"

MANIFESTS_FOLDER = "manifests"
FILES_FOLDER = "files"
STREAM_STATE_FILE = "stream-state.json"

# Windows cannot open a folder to flush it. Flushing a file commits NTFS's log
# up to its last change, entries in its folders included.
FLUSH_FOLDERS = sys.platform != "win32"
# A filesystem that cannot flush a folder says so with one of these; the files
# in it are still flushed.
_UNFLUSHABLE_FOLDER = frozenset({errno.EINVAL, errno.ENOTSUP, errno.EOPNOTSUPP})


class Stage:
    location: Location
    uri: str
    store: ObjectStore
    root: Path | None

    def __init__(self, location: Location) -> None:
        self.location = location
        self.uri = location.uri.rstrip(PATH_SEPARATOR)
        options = location.mount_declaration().storage_options(self.uri)
        self.store = OBJECT_STORES.store(self.uri, options)
        self.root = local_path(self.uri) if isinstance(self.store, LocalStore) else None

    def locate(self, name: str) -> Location:
        return Location(
            uri=f"{self.uri}{PATH_SEPARATOR}{name}",
            env_prefix=self.location.env_prefix,
        )

    def write(self, name: str, text: str) -> None:
        obstore.put(self.store, name, text.encode(ENCODING))
        self.sync([name])

    def sync(self, names: Iterable[str]) -> None:
        """Make what the names hold survive a crash of the host, before the
        source is told it may drop it. An object store keeps every write it
        acknowledges; a local filesystem keeps it in memory until flushed."""
        root = self.root
        if root is None:
            return
        folders: set[Path] = set()
        for name in names:
            path = root.joinpath(*name.split(PATH_SEPARATOR))
            flush(path)
            folders.update(path.parents[: len(path.relative_to(root).parts) + 1])
        if not FLUSH_FOLDERS:
            return
        for folder in sorted(folders, key=lambda folder: len(folder.parts)):
            try:
                flush(folder)
            except OSError as error:
                if error.errno not in _UNFLUSHABLE_FOLDER:
                    raise

    def read(self, name: str) -> str | None:
        try:
            result = obstore.get(self.store, name)
        except FileNotFoundError:
            return None
        return bytes(result.bytes()).decode(ENCODING)

    def upload(self, source: Path, name: str) -> None:
        obstore.put(self.store, name, source)
        self.sync([name])

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


def flush(path: Path) -> None:
    # Windows flushes only through a handle open for writing.
    flags = os.O_RDWR | os.O_BINARY if sys.platform == "win32" else os.O_RDONLY
    descriptor = os.open(path, flags)
    try:
        if sys.platform == "darwin":
            # fsync on macOS leaves the data in the drive's own cache.
            import fcntl

            try:
                fcntl.fcntl(descriptor, fcntl.F_FULLFSYNC)
                return
            except OSError:
                pass
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

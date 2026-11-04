#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import tarfile
import zipfile

from tabsdatak.conn.common.file._read._blocks import Readable, Seekable, close
from tabsdatak.conn.common.file._select import archive
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.file.error import FileCommonErrorCode

TAR = ".tar"
ZIP = ".zip"
TAR_STREAM_MODE = "r|"


def refused(path: str, reason: str) -> Exception:
    return FileCommonErrorCode.FILE_COMMON_37.exception(path=path, reason=reason)


class TarMemberStream:
    def __init__(self, path: str, source: Readable) -> None:
        self.path = path
        self.source = source
        try:
            self.archive = tarfile.open(fileobj=source, mode=TAR_STREAM_MODE)
            first = self.archive.next()
        except tarfile.TarError as error:
            close(source)
            raise refused(path, f"it is not a readable tar archive ({error})")
        if first is None or not first.isfile():
            self.close()
            raise refused(
                path,
                "it is empty"
                if first is None
                else f"its entry '{first.name}' is not a file",
            )
        self.member = self.archive.extractfile(first)
        self.checked = False

    def read(self, size: int = -1) -> bytes:
        data = self.member.read(size)
        if not data and not self.checked:
            self.checked = True
            if self.archive.next() is not None:
                raise refused(self.path, "it holds more than one entry")
        return data

    def close(self) -> None:
        self.archive.close()
        close(self.source)


class ZipMemberStream:
    def __init__(self, path: str, source: Seekable) -> None:
        self.source = source
        try:
            self.archive = zipfile.ZipFile(source)
        except zipfile.BadZipFile as error:
            close(source)
            raise refused(path, f"it is not a readable zip archive ({error})")
        entries = self.archive.infolist()
        if len(entries) != 1 or entries[0].is_dir():
            self.archive.close()
            close(source)
            raise refused(
                path,
                "it is empty"
                if not entries
                else f"it holds {len(entries)} entries"
                if len(entries) > 1
                else f"its entry '{entries[0].filename}' is not a file",
            )
        self.member = self.archive.open(entries[0])

    def read(self, size: int = -1) -> bytes:
        return self.member.read(size)

    def close(self) -> None:
        self.member.close()
        self.archive.close()
        close(self.source)


def member(store: FileStore, path: str) -> Readable:
    match archive(path):
        case ".tar":
            return TarMemberStream(path, store.open(path))
        case ".zip":
            return ZipMemberStream(path, store.ranged(path))
        case found:
            raise ValueError(f"The archive extension '{found}' is unknown")

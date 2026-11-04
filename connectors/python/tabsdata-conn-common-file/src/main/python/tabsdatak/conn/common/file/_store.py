#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

import obstore
import polars as pl
from obstore.store import ObjectStore

from tabsdatak.common._storage.frames import FrameLocator
from tabsdatak.common._storage.transfer import (
    FALSE,
    OBJECT_STORES,
    PATH_SEPARATOR,
    TRUE,
    StorageLocation,
    StoreKind,
)
from tabsdatak.conn.common._find import Listed

OBSTORE_PREFIX = "obstore."
RANGE_BLOCK = 4 * 1024 * 1024


def forwarded(root: str, conn_cfg: Mapping[str, Any] | None) -> dict[str, str]:
    kind = StoreKind.of(root)
    return {
        kind.canonical(key.removeprefix(OBSTORE_PREFIX)): text(value)
        for key, value in (conn_cfg or {}).items()
        if key.startswith(OBSTORE_PREFIX)
    }


def text(value: Any) -> str:
    if isinstance(value, bool):
        return TRUE if value else FALSE
    return str(value)


class Stream:
    def __init__(self, chunks: Iterator[memoryview]) -> None:
        self.chunks = chunks
        self.buffer = bytearray()
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        while size < 0 or len(self.buffer) < size:
            chunk = next(self.chunks, None)
            if chunk is None:
                break
            self.buffer += chunk
        if size < 0 or size >= len(self.buffer):
            data = bytes(self.buffer)
            self.buffer.clear()
            return data
        data = bytes(self.buffer[:size])
        del self.buffer[:size]
        return data

    def close(self) -> None:
        closer = getattr(self.chunks, "close", None)
        if closer is not None:
            closer()
        self.closed = True


class Ranged:
    def __init__(self, location: StorageLocation, block: int = RANGE_BLOCK) -> None:
        self.location = location
        self.block = block
        self.size = location.size()
        self.position = 0
        self.cached_at = -1
        self.cached = b""
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        end = self.size if size < 0 else min(self.position + size, self.size)
        parts = []
        while self.position < end:
            start = self.position - self.position % self.block
            if start != self.cached_at:
                self.cached = self.location.read_range(
                    start, min(start + self.block, self.size)
                )
                self.cached_at = start
            offset = self.position - start
            part = self.cached[offset : offset + end - self.position]
            parts.append(part)
            self.position += len(part)
        return b"".join(parts)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        match whence:
            case io.SEEK_SET:
                position = offset
            case io.SEEK_CUR:
                position = self.position + offset
            case io.SEEK_END:
                position = self.size + offset
            case _:
                raise ValueError(f"The seek origin {whence} is unknown")
        if position < 0:
            raise ValueError(f"The seek position {position} is before the start")
        self.position = position
        return position

    def tell(self) -> int:
        return self.position

    def close(self) -> None:
        self.cached = b""
        self.closed = True


@dataclass(frozen=True)
class FileStore:
    root: str
    options: dict[str, str] = field(default_factory=dict, repr=False)

    def store(self) -> ObjectStore:
        return OBJECT_STORES.store(self.root, self.options)

    def uri(self, path: str) -> str:
        if self.root.endswith(PATH_SEPARATOR):
            return f"{self.root}{path}"
        return f"{self.root}{PATH_SEPARATOR}{path}"

    def listed(self, directory: str) -> list[Listed]:
        listing = obstore.list_with_delimiter(self.store(), prefix=directory or None)
        return [
            Listed(item["path"].rpartition(PATH_SEPARATOR)[2], item["last_modified"])
            for item in listing["objects"]
        ]

    def open(self, path: str) -> Stream:
        return Stream(StorageLocation(self.uri(path), self.options).chunks())

    def ranged(self, path: str) -> Ranged:
        return Ranged(StorageLocation(self.uri(path), self.options))

    def scan_parquet(self, path: str) -> pl.LazyFrame:
        return FrameLocator(self.uri(path), self.options).scan()

    def delete(self, paths: list[str]) -> None:
        if paths:
            obstore.delete(self.store(), paths)

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io

from tabsdatak.common._storage.transfer import StorageLocation

RANGE_BLOCK = 4 * 1024 * 1024


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

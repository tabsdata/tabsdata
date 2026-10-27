#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl

from tabsdatak.common._storage import frames
from tabsdatak.common._storage.mounts import (
    AnonymousMountDeclaration,
    MountDeclaration,
)
from tabsdatak.server._tdrunner.function._commons.document.request import Location
from tabsdatak.server._tdrunner.function._commons.manifests.input import InputRecord


class TableFile:
    def __init__(
        self,
        uri: str,
        declaration: MountDeclaration = AnonymousMountDeclaration(),
    ) -> None:
        self.uri = uri
        self.declaration = declaration

    @classmethod
    def stored(cls, location: Location) -> TableFile:
        return cls(location.uri, location.mount_declaration())

    @classmethod
    def local(cls, path: Path) -> TableFile:
        return cls(path.absolute().as_uri())

    @classmethod
    def of(cls, record: InputRecord) -> TableFile | None:
        if record.location is not None:
            return cls.stored(record.location)
        if record.file is not None:
            return cls.local(Path(record.file))
        return None

    def scan(self) -> pl.LazyFrame:
        frame = frames.scan_parquet(frames.target(self.uri, self.declaration))
        frame.collect_schema()
        return frame

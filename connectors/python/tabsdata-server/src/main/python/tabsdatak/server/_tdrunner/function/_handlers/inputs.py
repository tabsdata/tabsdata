#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl

from tabsdatak.common._storage import frames
from tabsdatak.common._storage.mounts import (
    AnonymousMountDeclaration,
    MountDeclaration,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import InputRecord


class InputSource:
    def __init__(self, uri: str, declaration: MountDeclaration) -> None:
        self.uri = uri
        self.declaration = declaration

    @classmethod
    def of(cls, record: InputRecord) -> InputSource | None:
        if record.location is not None:
            location = record.location
            return cls(location.uri, location.mount_declaration())
        if record.file is not None:
            return cls(record.file, AnonymousMountDeclaration())
        return None

    def scan(self) -> pl.LazyFrame:
        # [TODO: tabsdatak.tableframe scan and sink migration] revisit when
        # tabsdatak.tableframe ships scan and sink methods.
        frame = frames.scan_parquet(frames.target(self.uri, self.declaration))
        frame.collect_schema()
        return frame

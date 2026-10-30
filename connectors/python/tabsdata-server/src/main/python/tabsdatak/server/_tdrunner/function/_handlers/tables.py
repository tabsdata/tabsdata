#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import hashlib
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from tabsdatak.common._storage import frames, transfer
from tabsdatak.common._storage.frames import FrameLocator
from tabsdatak.common._storage.mounts import (
    AnonymousMountDeclaration,
    MountDeclaration,
)
from tabsdatak.common._storage.transfer import StorageLocation
from tabsdatak.server._tdrunner.function._commons.document.request import Location
from tabsdatak.server._tdrunner.function._commons.manifests.input import InputRecord
from tabsdatak.server._tdrunner.function._commons.manifests.output import TableStats

PARQUET_MAGIC = b"PAR1"
FOOTER_LENGTH_SIZE = 4
TRAILER_SIZE = FOOTER_LENGTH_SIZE + len(PARQUET_MAGIC)
SCHEMA_HASH_PREFIX = "sha256:"


class TableFileError(ValueError):
    pass


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

    def locator(self) -> FrameLocator:
        return frames.locator(self.uri, self.declaration)

    def scan(self) -> pl.LazyFrame:
        frame = frames.scan_parquet(self.locator())
        frame.collect_schema()
        return frame

    def read(self) -> pl.DataFrame:
        return self.scan().collect()

    def sink(
        self,
        frame: pl.LazyFrame,
        metadata: dict[str, str] | None = None,
    ) -> pl.LazyFrame:
        locator = self.locator()
        return frame.sink_parquet(
            locator.uri,
            storage_options=locator.storage_options,
            metadata=metadata,
            engine="streaming",
            lazy=True,
        )

    def copy(self, target: TableFile) -> pl.LazyFrame:
        return target.sink(self.scan())

    def stats(self) -> TableStats:
        location = StorageLocation(
            uri=self.uri, options=self.declaration.storage_options(self.uri)
        )
        size = transfer.size(location)
        if size < len(PARQUET_MAGIC) + TRAILER_SIZE:
            raise TableFileError(f"Table file {self.uri} is too short for a parquet")
        trailer = transfer.read_range(location, size - TRAILER_SIZE, size)
        if trailer[FOOTER_LENGTH_SIZE:] != PARQUET_MAGIC:
            raise TableFileError(f"Table file {self.uri} does not end as a parquet")
        footer_length = int.from_bytes(trailer[:FOOTER_LENGTH_SIZE], "little")
        if footer_length > size - TRAILER_SIZE - len(PARQUET_MAGIC):
            raise TableFileError(f"Table file {self.uri} has a truncated footer")
        footer = transfer.read_range(
            location, size - TRAILER_SIZE - footer_length, size
        )
        try:
            metadata = pq.read_metadata(pa.BufferReader(PARQUET_MAGIC + footer))
        except (OSError, pa.ArrowException) as error:
            raise TableFileError(
                f"Table file {self.uri} has an unreadable footer: {error}"
            ) from error
        schema = metadata.schema.to_arrow_schema()
        digest = hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
        return TableStats(
            column_count=len(schema),
            row_count=metadata.num_rows,
            schema_hash=SCHEMA_HASH_PREFIX + digest,
            data_bytes=size,
        )

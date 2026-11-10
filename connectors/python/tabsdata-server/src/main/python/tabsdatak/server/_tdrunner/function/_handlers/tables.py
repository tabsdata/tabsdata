#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import hashlib
import io
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from polars.datatypes import DataTypeClass

from tabsdatak.common._storage.frames import FrameLocator
from tabsdatak.common._storage.mounts import (
    AnonymousMountDeclaration,
    MountDeclaration,
)
from tabsdatak.common._storage.transfer import StorageLocation
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import InputRecord
from tabsdatak.server._tdrunner.function._commons.manifests.output import TableStats

PARQUET_MAGIC = b"PAR1"
FOOTER_LENGTH_SIZE = 4
TRAILER_SIZE = FOOTER_LENGTH_SIZE + len(PARQUET_MAGIC)
SCHEMA_HASH_PREFIX = "sha256:"


class TableFileError(ValueError):
    pass


def storable(frame: pl.LazyFrame) -> pl.LazyFrame:
    columns = [
        _storage(name, pl.col(name), dtype).alias(name)
        for name, dtype in frame.collect_schema().items()
        if _extended(dtype)
    ]
    return frame.with_columns(columns) if columns else frame


def _extended(dtype: pl.DataType | DataTypeClass) -> bool:
    if isinstance(dtype, pl.BaseExtension):
        return True
    if isinstance(dtype, (pl.List, pl.Array)):
        return _extended(dtype.inner)
    if isinstance(dtype, pl.Struct):
        return any(_extended(field.dtype) for field in dtype.fields)
    if isinstance(dtype, pl.Map):
        return _extended(dtype.key) or _extended(dtype.value)
    return False


def _storage(column: str, expr: pl.Expr, dtype: pl.DataType | DataTypeClass) -> pl.Expr:
    if not _extended(dtype):
        return expr
    if isinstance(dtype, pl.BaseExtension):
        return _storage(column, expr.ext.storage(), dtype.ext_storage())
    if isinstance(dtype, pl.List):
        return expr.list.eval(_storage(column, pl.element(), dtype.inner))
    if isinstance(dtype, pl.Array):
        return expr.arr.eval(_storage(column, pl.element(), dtype.inner))
    if isinstance(dtype, pl.Struct):
        return expr.struct.with_fields([
            _storage(column, pl.field(field.name), field.dtype).alias(field.name)
            for field in dtype.fields
            if _extended(field.dtype)
        ])
    raise TableFileError(
        f"Column {column!r} cannot be stored: its type {dtype} holds an "
        "extension type where tabsdata cannot replace it with its storage type"
    )


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
        return None

    def locator(self) -> FrameLocator:
        return FrameLocator.of(self.uri, self.declaration)

    def scan(self) -> pl.LazyFrame:
        frame = self.locator().scan()
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
        return frame.pipe(storable).sink_parquet(
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
        size = location.size()
        if size < len(PARQUET_MAGIC) + TRAILER_SIZE:
            raise TableFileError(f"Table file {self.uri} is too short for a parquet")
        trailer = location.read_range(size - TRAILER_SIZE, size)
        if trailer[FOOTER_LENGTH_SIZE:] != PARQUET_MAGIC:
            raise TableFileError(f"Table file {self.uri} does not end as a parquet")
        footer_length = int.from_bytes(trailer[:FOOTER_LENGTH_SIZE], "little")
        if footer_length > size - TRAILER_SIZE - len(PARQUET_MAGIC):
            raise TableFileError(f"Table file {self.uri} has a truncated footer")
        footer = location.read_range(size - TRAILER_SIZE - footer_length, size)
        try:
            metadata = pq.read_metadata(io.BytesIO(PARQUET_MAGIC + footer))
        except (OSError, ValueError) as error:
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

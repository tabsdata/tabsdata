#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import hashlib
import io
import json
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
from tabsdatak.extensions.tableframe._engine.column.registry import registry
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import InputRecord
from tabsdatak.server._tdrunner.function._commons.manifests.output import TableStats
from tabsdatak.tableframe._engine.taxonomy.column import ColumnKind

PARQUET_MAGIC = b"PAR1"
FOOTER_LENGTH_SIZE = 4
TRAILER_SIZE = FOOTER_LENGTH_SIZE + len(PARQUET_MAGIC)
SCHEMA_HASH_PREFIX = "sha256:"


class TableFileError(ValueError):
    pass


# The schema hash outlives any library version: it is the digest of a canonical
# JSON of the user columns, sorted by name, with their polars types spelled in
# this vocabulary, never in how polars or pyarrow encode them. A spelling here
# must never change; a type without one fails the hash.
_SCALAR_NAMES: dict[type[pl.DataType], str] = {
    pl.Boolean: "boolean",
    pl.Int8: "int8",
    pl.Int16: "int16",
    pl.Int32: "int32",
    pl.Int64: "int64",
    pl.Int128: "int128",
    pl.UInt8: "uint8",
    pl.UInt16: "uint16",
    pl.UInt32: "uint32",
    pl.UInt64: "uint64",
    pl.UInt128: "uint128",
    pl.Float16: "float16",
    pl.Float32: "float32",
    pl.Float64: "float64",
    pl.String: "string",
    pl.Binary: "binary",
    pl.Date: "date",
    pl.Time: "time",
    pl.Null: "null",
}


def user_schema(schema: pl.Schema) -> pl.Schema:
    kind_of = registry().kind_of
    return pl.Schema({
        name: dtype
        for name, dtype in sorted(schema.items(), key=lambda column: column[0])
        if kind_of(name) is ColumnKind.USER
    })


def schema_hash(schema: pl.Schema) -> str:
    canonical = json.dumps(
        [[name, _canonical(dtype)] for name, dtype in user_schema(schema).items()],
        ensure_ascii=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return SCHEMA_HASH_PREFIX + hashlib.sha256(canonical.encode("ascii")).hexdigest()


def _canonical(dtype: pl.DataType | DataTypeClass) -> str | list:
    if isinstance(dtype, type):
        dtype = dtype()
    if isinstance(dtype, pl.Datetime):
        return ["datetime", dtype.time_unit, dtype.time_zone]
    if isinstance(dtype, pl.Duration):
        return ["duration", dtype.time_unit]
    if isinstance(dtype, pl.Decimal):
        return ["decimal", dtype.precision, dtype.scale]
    if isinstance(dtype, pl.Categorical):
        categories = dtype.categories
        return [
            "categorical",
            categories.name(),
            categories.namespace(),
            _canonical(categories.physical()),
        ]
    if isinstance(dtype, pl.Enum):
        return ["enum", dtype.categories.to_list()]
    if isinstance(dtype, pl.List):
        return ["list", _canonical(dtype.inner)]
    if isinstance(dtype, pl.Array):
        return ["array", _canonical(dtype.inner), dtype.size]
    if isinstance(dtype, pl.Struct):
        return [
            "struct",
            [[field.name, _canonical(field.dtype)] for field in dtype.fields],
        ]
    if isinstance(dtype, pl.Map):
        return ["map", _canonical(dtype.key), _canonical(dtype.value)]
    name = _SCALAR_NAMES.get(type(dtype))
    if name is None:
        raise TableFileError(f"The type {dtype} has no schema hash spelling")
    return name


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
        schema = pl.read_parquet_schema(io.BytesIO(PARQUET_MAGIC + footer))
        return TableStats(
            column_count=len(user_schema(schema)),
            row_count=metadata.num_rows,
            schema_hash=schema_hash(schema),
            data_bytes=size,
        )

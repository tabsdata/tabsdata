#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

import polars as pl

from tabsdatak.common._storage.frames import FrameLocator
from tabsdatak.spi import (
    CsvParseOptions,
    FrameMetadata,
    Layout,
    LazyFrameInput,
    ParseOptions,
    SourceFormat,
)
from tabsdatak.tableframe._engine.constant.column.prefixes import (
    TD_VOLATILE_SYSTEM_COLUMN_PREFIX,
)

LANDING_SUFFIX = ".t"
METADATA_SUFFIX = ".m"
OVERFLOW_COLUMN = f"{TD_VOLATILE_SYSTEM_COLUMN_PREFIX}overflow"

KIND_COLUMN = "kind"
CONTENT_COLUMN = "content"


class MetadataKind(Enum):
    SCHEMA = "schema"
    LAYOUT = "layout"
    FORMAT = "format"
    TYPED = "typed"
    PARSING = "parsing"
    COLUMNS = "columns"
    KNOWN = "known"


SERIALIZED_FORMATS = (SourceFormat.NDJSON,)


class StoredMetadataError(ValueError):
    pass


SCALAR_TYPES: dict[str, type[pl.DataType]] = {
    dtype.__name__: dtype
    for dtype in (
        pl.Null,
        pl.Boolean,
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.Int128,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.UInt128,
        pl.Float16,
        pl.Float32,
        pl.Float64,
        pl.String,
        pl.Binary,
        pl.Date,
        pl.Time,
    )
}


def encode_dtype(dtype: pl.DataType | type[pl.DataType]) -> Any:
    if isinstance(dtype, type):
        dtype = dtype()
    if isinstance(dtype, pl.Struct):
        return {
            "Struct": {field.name: encode_dtype(field.dtype) for field in dtype.fields}
        }
    if isinstance(dtype, pl.List):
        return {"List": encode_dtype(dtype.inner)}
    if isinstance(dtype, pl.Array):
        return {
            "Array": {"inner": encode_dtype(dtype.inner), "shape": list(dtype.shape)}
        }
    if isinstance(dtype, pl.Datetime):
        return {
            "Datetime": {"time_unit": dtype.time_unit, "time_zone": dtype.time_zone},
        }
    if isinstance(dtype, pl.Duration):
        return {"Duration": {"time_unit": dtype.time_unit}}
    if isinstance(dtype, pl.Decimal):
        return {"Decimal": {"precision": dtype.precision, "scale": dtype.scale}}
    if isinstance(dtype, pl.Enum):
        return {"Enum": dtype.categories.to_list()}
    name = type(dtype).__name__
    if SCALAR_TYPES.get(name) is type(dtype):
        return name
    raise StoredMetadataError(f"The type {dtype} cannot be stored")


def decode_dtype(encoded: Any) -> pl.DataType:
    if isinstance(encoded, str) and encoded in SCALAR_TYPES:
        return SCALAR_TYPES[encoded]()
    if isinstance(encoded, dict) and len(encoded) == 1:
        [(name, content)] = encoded.items()
        if name == "Struct":
            return pl.Struct({
                field: decode_dtype(dtype) for field, dtype in content.items()
            })
        if name == "List":
            return pl.List(decode_dtype(content))
        if name == "Array":
            return pl.Array(decode_dtype(content["inner"]), tuple(content["shape"]))
        if name == "Datetime":
            return pl.Datetime(**content)
        if name == "Duration":
            return pl.Duration(**content)
        if name == "Decimal":
            return pl.Decimal(**content)
        if name == "Enum":
            return pl.Enum(content)
    raise StoredMetadataError(f"The stored type {encoded!r} is unknown")


class StoredMetadata:
    @staticmethod
    def of(metadata: FrameMetadata) -> pl.DataFrame:
        entries: list[tuple[MetadataKind, Any]] = []
        if metadata.schema is not None:
            entries.append((
                MetadataKind.SCHEMA,
                {name: encode_dtype(dtype) for name, dtype in metadata.schema.items()},
            ))
        entries.append((MetadataKind.LAYOUT, metadata.layout.value))
        if metadata.format is not None:
            entries.append((MetadataKind.FORMAT, metadata.format.value))
        entries.append((MetadataKind.TYPED, metadata.typed))
        options = parse_options(metadata)
        if options is not None:
            entries.append((MetadataKind.PARSING, asdict(options)))
        if metadata.columns is not None:
            entries.append((MetadataKind.COLUMNS, metadata.columns))
        entries.append((MetadataKind.KNOWN, metadata.known))
        return pl.DataFrame(
            {
                KIND_COLUMN: [kind.value for kind, _ in entries],
                CONTENT_COLUMN: [json.dumps(content) for _, content in entries],
            },
            schema={KIND_COLUMN: pl.String, CONTENT_COLUMN: pl.String},
        )

    @staticmethod
    def read(stored: pl.DataFrame) -> FrameMetadata:
        entries: dict[MetadataKind, Any] = {}
        for kind, content in stored.select(KIND_COLUMN, CONTENT_COLUMN).iter_rows():
            try:
                entries[MetadataKind(kind)] = json.loads(content)
            except ValueError as error:
                raise StoredMetadataError(
                    f"The stored metadata entry '{kind}' is unreadable: {error}",
                ) from error
        schema = entries.get(MetadataKind.SCHEMA)
        source = entries.get(MetadataKind.FORMAT)
        typed = entries.get(MetadataKind.TYPED, True)
        parsing = entries.get(MetadataKind.PARSING)
        known = entries.get(MetadataKind.KNOWN, True)
        try:
            if not isinstance(typed, bool):
                raise ValueError(f"typed is {typed!r}, not a boolean")
            if not isinstance(known, bool):
                raise ValueError(f"known is {known!r}, not a boolean")
            source_format = None if source is None else SourceFormat(source)
            if parsing is not None and source_format is None:
                raise ValueError("parse options are stored without a format")
            return FrameMetadata(
                schema=None
                if schema is None
                else pl.Schema({
                    name: decode_dtype(dtype) for name, dtype in schema.items()
                }),
                layout=Layout(entries.get(MetadataKind.LAYOUT, Layout.COLUMNAR.value)),
                format=source_format,
                typed=typed,
                parsing=None
                if parsing is None
                else source_format.parse_options_class(**parsing),
                columns=entries.get(MetadataKind.COLUMNS),
                known=known,
            )
        except (TypeError, ValueError) as error:
            raise StoredMetadataError(
                f"The stored metadata is unreadable: {error}",
            ) from error


def mismatch(actual: pl.Schema, metadata: FrameMetadata) -> str | None:
    if metadata.layout is Layout.SERIALIZED:
        if metadata.format not in SERIALIZED_FORMATS:
            return f"serialized {metadata.format.name} rows cannot be split when served"
        if len(actual) != 1 or actual.dtypes() != [pl.String]:
            return (
                f"serialized data lands as one String column, but the data has "
                f"{dict(actual)}"
            )
        if metadata.schema is None:
            return "serialized data is served with a schema, but there is none"
        return None
    if metadata.schema is None:
        return None
    if list(actual) != landed_columns(actual, metadata.schema):
        return (
            f"the schema names columns {list(metadata.schema)} but the data has "
            f"{list(actual)}"
        )
    return None


def landed_columns(actual: pl.Schema, schema: pl.Schema) -> list[str]:
    header, extras = split_overflow(actual, schema)
    if not extras and OVERFLOW_COLUMN not in actual:
        return header
    if list(schema) != header + extras:
        return list(schema)
    return [*header, OVERFLOW_COLUMN]


def split_overflow(actual: pl.Schema, schema: pl.Schema) -> tuple[list[str], list[str]]:
    if OVERFLOW_COLUMN not in actual:
        return list(schema), []
    header = [name for name in schema if name in actual]
    extras = [name for name in schema if name not in actual]
    return header, extras


def parse_options(metadata: FrameMetadata) -> ParseOptions | None:
    if metadata.parsing is not None or metadata.format is None:
        return metadata.parsing
    return metadata.format.parse_options_class()


def serve(frame: pl.LazyFrame, metadata: FrameMetadata) -> pl.LazyFrame:
    schema = metadata.schema
    if schema is None:
        return frame
    actual = frame.collect_schema()
    if metadata.layout is Layout.SERIALIZED:
        [line] = actual.names()
        return frame.select(
            pl.col(line).str.json_decode(pl.Struct(schema)).struct.unnest(),
        )
    options = parse_options(metadata)
    decimal_comma = isinstance(options, CsvParseOptions) and options.decimal_comma
    header, extras = split_overflow(actual, schema)
    return frame.select(
        *(
            pl.col(name)
            if actual[name] == schema[name]
            else parse(name, schema[name], decimal_comma=decimal_comma)
            if actual[name] == pl.String
            else pl.col(name).cast(schema[name], strict=True)
            for name in header
        ),
        *(
            parse(
                name,
                schema[name],
                decimal_comma=decimal_comma,
                text=pl.col(OVERFLOW_COLUMN).list.get(position, null_on_oob=True),
            )
            for position, name in enumerate(extras)
        ),
    )


@dataclass(frozen=True)
class LandedPair:
    data: str
    metadata: str | None

    @classmethod
    def of(cls, name: str) -> LandedPair:
        if name.endswith(LANDING_SUFFIX):
            return cls(name, f"{name.removesuffix(LANDING_SUFFIX)}{METADATA_SUFFIX}")
        return cls(name, None)

    @property
    def names(self) -> list[str]:
        if self.metadata is None:
            return [self.data]
        return [self.data, self.metadata]


@dataclass(frozen=True)
class LandedTable:
    data: FrameLocator
    metadata: FrameLocator

    def read_data(self) -> pl.LazyFrame:
        return self.data.scan()

    def read_metadata(self) -> FrameMetadata:
        return StoredMetadata.read(self.metadata.scan().collect())

    def scan(self) -> tuple[pl.LazyFrame, FrameMetadata]:
        metadata = self.read_metadata()
        return serve(self.read_data(), metadata), metadata

    def input(self) -> LazyFrameInput:
        return LazyFrameInput(self.read_data(), metadata=self.read_metadata())


def parse(
    name: str,
    dtype: pl.DataType,
    *,
    decimal_comma: bool = False,
    text: pl.Expr | None = None,
) -> pl.Expr:
    text = pl.col(name) if text is None else text
    if dtype == pl.String:
        return text.alias(name)
    if dtype == pl.Boolean:
        parsed = text.str.to_lowercase().replace_strict(
            {"true": True, "false": False},
            return_dtype=pl.Boolean,
        )
    elif dtype == pl.Date:
        parsed = text.str.to_date(strict=True)
    elif isinstance(dtype, pl.Datetime):
        parsed = text.str.to_datetime(
            time_unit=dtype.time_unit,
            time_zone=dtype.time_zone,
            strict=True,
        )
    elif dtype == pl.Time:
        parsed = text.str.to_time(strict=True)
    elif decimal_comma and dtype.is_float():
        parsed = text.str.replace(",", ".", literal=True).cast(dtype, strict=True)
    else:
        parsed = text.cast(dtype, strict=True)
    return parsed.alias(name)

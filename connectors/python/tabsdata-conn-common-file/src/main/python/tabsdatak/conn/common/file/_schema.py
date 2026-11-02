#
# Copyright 2026 Tabsdata Inc.
#

"""File schemas at metadata time, read from the file's head only: a parquet
footer, an avro header, or the first lines of a CSV or newline-delimited JSON
file (their types inferred from those lines).

Per prefix and format, the newest :data:`FILES_PER_GROUP` files are read; when
they agree, the rest of the group gets that schema with ``schema_from`` naming
the file it came from. When they disagree, the files hold different things
(one table per file), so every other file of the group is read for its own
schema. At most :data:`MAX_SCHEMA_READS` files are read per metadata call,
:data:`READ_WORKERS` at a time, and none are started past
:data:`READ_BUDGET_S`. A file that cannot be read keeps no columns.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import re
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager
from dataclasses import replace
from typing import IO, Any

from tabsdatak.conn.common._explorer import Column
from tabsdatak.conn.common.file._explorer import FileStorageEntity

logger = logging.getLogger(__name__)

FILES_PER_GROUP = 2
MAX_SCHEMA_READS = 200
MAX_COLUMNS = 500
READ_WORKERS = 8
READ_BUDGET_S = 30.0
HEAD_BYTES = 64 * 1024
TYPE_ROWS = 20

PARQUET = frozenset({"parquet"})
AVRO = frozenset({"avro"})
NDJSON = frozenset({"json", "ndjson", "jsonl"})
CSV = frozenset({"csv", "tsv", "txt"})
READABLE = PARQUET | AVRO | NDJSON | CSV

Opener = Callable[[FileStorageEntity], AbstractContextManager[IO[bytes]]]

_INT = re.compile(r"^[+-]?\d+$")
_FLOAT = re.compile(r"^[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DATETIME = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$"
)
_BOOL = frozenset({"true", "false"})


def attach_schemas(
    entities: list[FileStorageEntity],
    opener: Opener,
    clock: Callable[[], float] = time.monotonic,
) -> list[FileStorageEntity]:
    """``entities``, in order, with ``columns`` (and ``schema_from``) set on
    those whose schema is read or shared, as described in the module
    docstring."""
    groups: dict[tuple[str, str, str], list[int]] = defaultdict(list)
    for index, entity in enumerate(entities):
        fmt = _format(entity)
        if fmt in READABLE:
            groups[(entity.bucket, entity.prefix, fmt)].append(index)
    ordered = []
    for group in groups.values():
        group.sort(
            key=lambda i: (entities[i].last_modified or "", entities[i].fqn()),
            reverse=True,
        )
        ordered.append(group)
    deadline = clock() + READ_BUDGET_S
    schemas: dict[int, list[Column] | None] = {}

    def read(index: int) -> list[Column] | None:
        entity = entities[index]
        if clock() >= deadline:
            return None
        try:
            with opener(entity) as stream:
                return read_schema(_format(entity), stream)
        except Exception as error:  # noqa: BLE001 - one unreadable file keeps no schema
            logger.info("No schema for %s: %s", entity.fqn(), error)
            return None

    def read_all(indexes: list[int]) -> None:
        indexes = indexes[: MAX_SCHEMA_READS - len(schemas)]
        if not indexes:
            return
        with ThreadPoolExecutor(max_workers=READ_WORKERS) as pool:
            schemas.update(zip(indexes, pool.map(read, indexes)))

    read_all([i for group in ordered for i in group[:FILES_PER_GROUP]])
    result = list(entities)
    mixed = []
    for group in ordered:
        read_columns = [schemas[i] for i in group if i in schemas]
        if not read_columns:
            continue
        if all(read_columns) and all(_same(c, read_columns[0]) for c in read_columns):
            source = entities[group[0]]
            for index in group[len(read_columns) :]:
                result[index] = replace(
                    entities[index],
                    columns=list(read_columns[0]),
                    schema_from=source.fqn(),
                )
        else:
            mixed.extend(group[len(read_columns) :])
    read_all(mixed)
    for index, columns in schemas.items():
        if columns:
            result[index] = replace(entities[index], columns=columns)
    return result


def read_schema(fmt: str, stream: IO[bytes]) -> list[Column]:
    """The columns of a file of format ``fmt`` (one of :data:`READABLE`),
    read from the head of ``stream``."""
    if fmt in PARQUET:
        columns = _parquet(stream)
    elif fmt in AVRO:
        columns = _avro(stream)
    elif fmt in NDJSON:
        columns = _ndjson(_head_lines(stream))
    else:
        columns = _csv(_head_lines(stream), "\t" if fmt == "tsv" else ",")
    return columns[:MAX_COLUMNS]


def _format(entity: FileStorageEntity) -> str:
    return (entity.file.format if entity.file is not None else "").lower()


def _same(a: list[Column], b: list[Column]) -> bool:
    return [(c.name, c.type) for c in a] == [(c.name, c.type) for c in b]


def _head_lines(stream: IO[bytes]) -> list[str]:
    head = stream.read(HEAD_BYTES) or b""
    lines = head.split(b"\n")
    if len(head) >= HEAD_BYTES:
        lines = lines[:-1]
    return [
        line.decode("utf-8", errors="replace").rstrip("\r")
        for line in lines[: TYPE_ROWS + 1]
    ]


def _parquet(stream: IO[bytes]) -> list[Column]:
    import pyarrow.parquet as pq

    schema = pq.ParquetFile(stream).schema_arrow
    return [Column(name=f.name, type=str(f.type), nullable=f.nullable) for f in schema]


def _avro(stream: IO[bytes]) -> list[Column]:
    import fastavro

    schema = fastavro.reader(stream).writer_schema or {}
    return [
        Column(name=str(f.get("name", "")), type=_avro_type(f.get("type")))
        for f in schema.get("fields") or []
    ]


def _avro_type(avro: Any) -> str:
    if isinstance(avro, list):
        kinds = [_avro_type(t) for t in avro if t != "null"]
        return "|".join(kinds) or "null"
    if isinstance(avro, dict):
        return str(avro.get("logicalType") or avro.get("type") or "")
    return str(avro)


def _ndjson(lines: Iterable[str]) -> list[Column]:
    types: dict[str, set[str]] = {}
    for line in lines:
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if not isinstance(record, dict):
            continue
        for name, value in record.items():
            seen = types.setdefault(str(name), set())
            if value is not None:
                seen.add(_json_type(value))
    return [Column(name=name, type=_merge(seen)) for name, seen in types.items()]


def _json_type(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "list"
    return _text_type(str(value))


def _csv(lines: list[str], delimiter: str) -> list[Column]:
    rows = list(csv.reader(io.StringIO("\n".join(lines)), delimiter=delimiter))
    if not rows:
        return []
    header, body = rows[0], rows[1:]
    columns = []
    for index, name in enumerate(header):
        seen = {
            _text_type(row[index])
            for row in body
            if index < len(row) and row[index].strip()
        }
        columns.append(Column(name=name.strip(), type=_merge(seen)))
    return columns


def _text_type(value: str) -> str:
    value = value.strip()
    if value.lower() in _BOOL:
        return "bool"
    if _INT.match(value):
        return "int"
    if _FLOAT.match(value):
        return "float"
    if _DATE.match(value):
        return "date"
    if _DATETIME.match(value):
        return "datetime"
    return "string"


def _merge(seen: set[str]) -> str:
    if not seen:
        return "string"
    if len(seen) == 1:
        return next(iter(seen))
    if seen <= {"int", "float"}:
        return "float"
    if seen <= {"date", "datetime"}:
        return "datetime"
    return "string"

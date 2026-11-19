#
# Copyright 2026 Tabsdata Inc.
#

"""The explorer of an object-store connection, over the same ``FileStore``
(root, credentials and forwarded ``conn_cfg`` options) its plugin reads and
writes with, so a check, a listing or a preview reaches the store a run does.

``check_connect`` lists the conn's ``base_path``; ``metadata`` lists every
object under it, with their schemas read from the objects' heads
(``_schema.attach_schemas``); ``data`` previews the first ``limit`` rows of one
object. Every read is by range requests of a bounded block, never the whole
object.
"""

from __future__ import annotations

import csv
import io
import json
import os
from abc import abstractmethod
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from typing import Any, ClassVar
from urllib.parse import urlparse

from tabsdatak.common._storage.ranged import Ranged
from tabsdatak.common._storage.transfer import PATH_SEPARATOR
from tabsdatak.conn.common.file._explorer import (
    FilePattern,
    FileStorageConnMetaModel,
    FileStorageEntity,
    preview_format,
    read_excel_rows,
)
from tabsdatak.conn.common.file._read._format import Format
from tabsdatak.conn.common.file._schema import HEAD_BYTES, attach_schemas
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.spi import Conn, ConnMetaModel, Entity, Explorer

LINES_CHUNK = 8192
PREVIEW_BLOCK = 256 * 1024


@dataclass(frozen=True)
class Located:
    """A connection's store, split into its bucket and the key prefix its
    ``base_path`` names: entities carry keys from the bucket's root."""

    scheme: str
    bucket: str
    base: str
    options: dict[str, str]

    @classmethod
    def of(cls, store: FileStore) -> Located:
        parsed = urlparse(store.root)
        return cls(
            scheme=parsed.scheme,
            bucket=parsed.netloc,
            base=parsed.path.strip(PATH_SEPARATOR),
            options=store.options,
        )

    def store(self, bucket: str = "") -> FileStore:
        return FileStore(f"{self.scheme}://{bucket or self.bucket}", self.options)


class FileStoreExplorer[C: Conn](Explorer[C]):
    """Verify connectivity to an object store and preview its objects."""

    meta_model: ClassVar[ConnMetaModel] = FileStorageConnMetaModel()
    entity_type: ClassVar[type[Entity]] = FileStorageEntity

    @abstractmethod
    def store(self, conn: C) -> FileStore:
        """The store the conn's plugin reads and writes with."""

    def check_connect(self, conn: C) -> None:
        located = Located.of(self.store(conn))
        next(located.store().walk(located.base), None)

    def metadata(self, conn: C) -> list[FileStorageEntity]:
        located = Located.of(self.store(conn))
        store = located.store()
        entities = [self.entity(located, found) for found in store.walk(located.base)]
        return attach_schemas(
            entities,
            lambda entity: closing(store.ranged(entity.object_key(), HEAD_BYTES)),
        )

    def data(self, conn: C, entity: FileStorageEntity, limit: int = 100) -> list[dict]:
        if entity.file is None:
            return []
        store = Located.of(self.store(conn)).store(entity.bucket)

        def open_() -> Ranged:
            return store.ranged(entity.object_key(), PREVIEW_BLOCK)

        match preview_format(entity):
            case Format.AVRO:
                return read_avro(open_(), limit)
            case Format.EXCEL:
                return read_excel_rows(open_, limit)
            case Format.NDJSON:
                return read_ndjson(open_(), limit)
            case Format.PARQUET:
                return read_parquet(open_(), limit)
            case _:
                return read_csv(open_(), limit)

    def entity(self, located: Located, found: Mapping[str, Any]) -> FileStorageEntity:
        prefix, _, filename = found["path"].rpartition(PATH_SEPARATOR)
        _, extension = os.path.splitext(filename)
        last_modified = found.get("last_modified")
        return FileStorageEntity(
            connection_type=self.meta_model,
            scheme=located.scheme,
            bucket=located.bucket,
            prefix=prefix,
            file=FilePattern(pattern=filename, format=extension[1:].lower()),
            size=found.get("size"),
            last_modified=last_modified.isoformat() if last_modified else None,
        )


def head_lines(source: Ranged, count: int) -> list[str]:
    lines: list[bytes] = []
    rest = b""
    with closing(source):
        while len(lines) < count:
            chunk = source.read(LINES_CHUNK)
            if not chunk:
                if rest:
                    lines.append(rest)
                break
            parts = (rest + chunk).split(b"\n")
            rest = parts.pop()
            lines.extend(parts)
    return [line.decode("utf-8") for line in lines[:count]]


def read_avro(source: Ranged, limit: int) -> list[dict]:
    import fastavro

    rows: list[dict] = []
    with closing(source):
        for index, record in enumerate(fastavro.reader(source)):
            if index >= limit:
                break
            rows.append(dict(record))
    return rows


def read_csv(source: Ranged, limit: int) -> list[dict]:
    rows = head_lines(source, limit + 1)
    return list(csv.DictReader(io.StringIO("\n".join(rows))))


def read_ndjson(source: Ranged, limit: int) -> list[dict]:
    return [json.loads(line) for line in head_lines(source, limit) if line.strip()]


def read_parquet(source: Ranged, limit: int) -> list[dict]:
    import pyarrow.parquet as pq

    with closing(source):
        parquet = pq.ParquetFile(source)
        try:
            batch = next(parquet.iter_batches(batch_size=limit))
        except StopIteration:
            return []
        return batch.to_pylist()

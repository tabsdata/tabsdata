#
# Copyright 2026 Tabsdata Inc.
#

"""Local-file connection explorer.

``check_connect`` proves the connection's ``base_path`` exists and is a
directory; ``metadata`` walks it recursively and lists the files found;
``data`` previews the first ``limit`` rows of a single file, reading only
the leading bytes it needs and dispatching by format: parquet via pyarrow,
avro via fastavro, json / ndjson as newline-delimited JSON, and every other
format (csv, log, ...) via CSV parsing.

Mirrors the object-storage explorers (S3 / GCS / Azure Blob): the same
:class:`FileStorageConnMetaModel` and :class:`FileStorageEntity`, with a
``file`` scheme instead of a cloud one and no bucket.
"""

import csv
import io
import json
import os
from typing import ClassVar, TypeAlias

from tabsdatak.conn.common.file._explorer import (
    FilePattern,
    FileStorageConnMetaModel,
    FileStorageEntity,
)
from tabsdatak.conn.localfile import LocalFileDestConn, LocalFileSrcConn
from tabsdatak.spi import ConnMetaModel, Entity, Explorer

# Either local-file connection flavour -- source or destination. Both carry
# only an absolute ``base_path`` (no credentials).
LocalFileConn: TypeAlias = LocalFileSrcConn | LocalFileDestConn

_CHUNK = 8192


class LocalFileExplorer(Explorer):
    """List and preview files under a local-file connection's base path."""

    meta_model: ClassVar[ConnMetaModel] = FileStorageConnMetaModel()
    entity_type: ClassVar[type[Entity]] = FileStorageEntity

    def check_connect(self, conn: LocalFileConn) -> None:
        base = conn._base_path()
        if not os.path.isdir(base):
            raise NotADirectoryError(f"base_path is not a directory: {base}")

    def metadata(self, conn: LocalFileConn) -> list[FileStorageEntity]:
        base = conn._base_path()
        entities: list[FileStorageEntity] = []
        for dirpath, _dirnames, filenames in os.walk(base):
            for filename in filenames:
                _, ext = os.path.splitext(filename)
                try:
                    stat = os.stat(os.path.join(dirpath, filename))
                except OSError:
                    # Listed but gone (concurrent removal); skip it.
                    continue
                entities.append(
                    FileStorageEntity(
                        connection_type=self.meta_model,
                        scheme="file",
                        prefix=dirpath,
                        file=FilePattern(pattern=filename, format=ext[1:].lower()),
                        size=stat.st_size,
                        last_modified=_isoformat(stat.st_mtime),
                    )
                )
        return entities

    def data(
        self, conn: LocalFileConn, entity: FileStorageEntity, limit: int = 100
    ) -> list[dict]:
        if entity.file is None:
            return []
        fmt = (entity.file.format or "").lower()
        path = (
            os.path.join(entity.prefix, entity.file.pattern)
            if entity.prefix
            else entity.file.pattern
        )
        if fmt == "parquet":
            return _read_parquet(path, limit)
        if fmt == "avro":
            return _read_avro(path, limit)
        if fmt in ("json", "ndjson", "jsonl"):
            return _read_ndjson(path, limit)
        # csv and every other format fall back to CSV parsing.
        return _read_csv(path, limit)


def _isoformat(mtime: float) -> str:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()


def _stream_lines(path: str, n: int) -> list[str]:
    """First ``n`` newline-delimited lines of a file, read in 8 KB chunks and
    stopped as soon as they're found -- so only the leading bytes are read,
    never the whole file."""
    lines: list[bytes] = []
    buf = b""
    with open(path, "rb") as handle:
        while len(lines) < n:
            chunk = handle.read(_CHUNK)
            if not chunk:
                if buf:
                    lines.append(buf)
                break
            buf += chunk
            parts = buf.split(b"\n")
            buf = parts.pop()  # trailing partial line (or b"")
            lines.extend(parts)
    return [line.decode("utf-8") for line in lines[:n]]


def _read_csv(path: str, limit: int) -> list[dict]:
    # Header line + the first ``limit`` data rows -> dicts keyed by header.
    rows = _stream_lines(path, limit + 1)
    return list(csv.DictReader(io.StringIO("\n".join(rows))))


def _read_ndjson(path: str, limit: int) -> list[dict]:
    return [json.loads(line) for line in _stream_lines(path, limit) if line.strip()]


def _read_avro(path: str, limit: int) -> list[dict]:
    # fastavro is imported lazily so importing this module stays cheap.
    import fastavro

    rows: list[dict] = []
    with open(path, "rb") as handle:
        for i, record in enumerate(fastavro.reader(handle)):
            if i >= limit:
                break
            rows.append(dict(record))
    return rows


def _read_parquet(path: str, limit: int) -> list[dict]:
    # pyarrow is imported lazily; only the footer and the first row group are
    # read.
    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(path)
    try:
        batch = next(parquet_file.iter_batches(batch_size=limit))
    except StopIteration:
        return []
    return batch.to_pylist()

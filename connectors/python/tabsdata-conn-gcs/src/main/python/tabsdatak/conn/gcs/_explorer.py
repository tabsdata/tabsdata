#
#  Copyright 2026. Tabs Data Inc.
#

"""GCS connection explorer.

``check_connect`` proves auth + bucket existence; ``metadata`` lists the
objects under the conn's ``base_path``; ``data`` previews the first ``limit``
rows of a single object, fetching only the bytes it needs (never the whole
object) and dispatching by format: parquet via pyarrow, avro via fastavro,
json / ndjson as newline-delimited JSON, and every other format (csv, log,
...) via CSV parsing.
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

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert, normalize_dir_path
from tabsdatak.conn.gcs import GCSDestConn, GCSSrcConn
from tabsdatak.spi import ConnMetaModel, Entity, Explorer

# Either GCS connection flavour -- source or destination.
GCSConn: TypeAlias = GCSSrcConn | GCSDestConn

_CHUNK = 8192


class GCSConnectionExplorer(Explorer):
    """Verify connectivity to a GCS bucket and preview its objects."""

    meta_model: ClassVar[ConnMetaModel] = FileStorageConnMetaModel()
    entity_type: ClassVar[type[Entity]] = FileStorageEntity

    def check_connect(self, conn: GCSConn) -> None:
        client = _storage_client(conn)
        # A bounded object list proves auth + bucket reachability using only
        # object-level access (``storage.objects.list``).
        blobs = client.list_blobs(conn._bucket(), max_results=1)
        next(iter(blobs), None)

    def metadata(self, conn: GCSConn) -> list[FileStorageEntity]:
        bucket = conn._bucket()
        # base_path is a FullPathSpec (leading "/"); the GCS list prefix
        # wants no leading "/" and a trailing "/", so normalize it
        # (root "/" -> "" -> list the whole bucket).
        prefix = normalize_dir_path(
            conn._base_path(), lead_separator=False, back_separator=True
        )
        # ``bucket`` holds the raw bucket name and ``prefix`` the object's
        # full key directory (which already includes base_path), so the
        # fqn resolves to ``gs://<bucket>/<prefix>/<file>``.
        client = _storage_client(conn)
        prefix_arg = prefix or None

        entities: list[FileStorageEntity] = []
        for blob in client.list_blobs(bucket, prefix=prefix_arg):
            key = blob.name
            prefix, _, filename = key.rpartition("/")
            _, ext = os.path.splitext(filename)
            updated = blob.updated
            entities.append(
                FileStorageEntity(
                    connection_type=self.meta_model,
                    scheme="gs",
                    bucket=bucket,
                    prefix=prefix,
                    file=FilePattern(pattern=filename, format=ext[1:].lower()),
                    size=blob.size,
                    last_modified=updated.isoformat() if updated else None,
                )
            )
        return entities

    def data(
        self, conn: GCSConn, entity: FileStorageEntity, limit: int = 100
    ) -> list[dict]:
        if entity.file is None:
            return []
        fmt = (entity.file.format or "").lower()
        bucket = entity.bucket or conn._bucket()
        key = (
            f"{entity.prefix}/{entity.file.pattern}"
            if entity.prefix
            else entity.file.pattern
        )
        blob = _storage_client(conn).bucket(bucket).blob(key)
        if fmt == "parquet":
            return _read_parquet(blob, limit)
        if fmt == "avro":
            return _read_avro(blob, limit)
        if fmt in ("json", "ndjson", "jsonl"):
            return _read_ndjson(blob, limit)
        # csv and every other format fall back to CSV parsing.
        return _read_csv(blob, limit)


def _storage_client(conn: GCSConn):
    # google-cloud-storage is imported lazily so merely importing this
    # module (e.g. a registry scan) doesn't pay for the SDK.
    from google.cloud import storage
    from google.oauth2 import service_account

    creds = None
    if conn.credentials is not None:
        raw = _convert(conn.credentials.json, str)
        payload = json.loads(raw) if raw else {}
        if payload:
            creds = service_account.Credentials.from_service_account_info(payload)
    project = conn._project()
    return storage.Client(project=project or None, credentials=creds)


def _stream_lines(blob, n: int) -> list[str]:
    """First ``n`` newline-delimited lines of a blob, read in 8 KB steps off a
    range-backed ``BlobReader`` (capped at 256 KB per GET) and stopped as soon
    as they're found -- so only the leading bytes are fetched."""
    reader = blob.open("rb", chunk_size=256 * 1024)
    lines: list[bytes] = []
    buf = b""
    try:
        while len(lines) < n:
            chunk = reader.read(_CHUNK)
            if not chunk:
                if buf:
                    lines.append(buf)
                break
            buf += chunk
            parts = buf.split(b"\n")
            buf = parts.pop()  # trailing partial line (or b"")
            lines.extend(parts)
    finally:
        reader.close()
    return [line.decode("utf-8") for line in lines[:n]]


def _read_csv(blob, limit: int) -> list[dict]:
    # Header line + the first ``limit`` data rows -> dicts keyed by header.
    rows = _stream_lines(blob, limit + 1)
    return list(csv.DictReader(io.StringIO("\n".join(rows))))


def _read_ndjson(blob, limit: int) -> list[dict]:
    return [json.loads(line) for line in _stream_lines(blob, limit) if line.strip()]


def _read_avro(blob, limit: int) -> list[dict]:
    # fastavro reads the container block-by-block off the range-backed reader.
    import fastavro

    rows: list[dict] = []
    with blob.open("rb", chunk_size=1024 * 1024) as stream:
        for i, record in enumerate(fastavro.reader(stream)):
            if i >= limit:
                break
            rows.append(dict(record))
    return rows


def _read_parquet(blob, limit: int) -> list[dict]:
    # BlobReader is seekable, so ParquetFile reads only the footer and then
    # iter_batches pulls only the first row group -- never the whole object.
    import pyarrow.parquet as pq

    with blob.open("rb", chunk_size=1024 * 1024) as handle:
        parquet_file = pq.ParquetFile(handle)
        try:
            batch = next(parquet_file.iter_batches(batch_size=limit))
        except StopIteration:
            return []
        return batch.to_pylist()

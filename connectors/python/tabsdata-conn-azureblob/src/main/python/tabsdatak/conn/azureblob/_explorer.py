#
#  Copyright 2026. Tabs Data Inc.
#

"""Azure Blob Storage connection explorer.

``check_connect`` proves auth + container existence; ``metadata`` lists the
blobs under the conn's ``base_path``; ``data`` previews the first ``limit``
rows of a single blob, fetching only the bytes it needs (never the whole
blob) and dispatching by format: parquet via pyarrow, avro via fastavro,
json / ndjson as newline-delimited JSON, and every other format (csv, log,
...) via CSV parsing.
"""

import csv
import io
import json
import os
from typing import ClassVar, TypeAlias

from tabsdatak.conn.azureblob import AzureBlobDestConn, AzureBlobSrcConn
from tabsdatak.conn.common.file._explorer import (
    FilePattern,
    FileStorageConnMetaModel,
    FileStorageEntity,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import normalize_dir_path
from tabsdatak.spi import ConnMetaModel, Entity, Explorer

# Either Azure Blob connection flavour -- source or destination.
AzureBlobConn: TypeAlias = AzureBlobSrcConn | AzureBlobDestConn

_CHUNK = 8192


class AzureBlobConnectionExplorer(Explorer):
    """Verify connectivity to an Azure Blob Storage container and preview it."""

    meta_model: ClassVar[ConnMetaModel] = FileStorageConnMetaModel()
    entity_type: ClassVar[type[Entity]] = FileStorageEntity

    def check_connect(self, conn: AzureBlobConn) -> None:
        container = _container_client(conn)
        # get_container_properties confirms the credentials can reach this
        # specific container without enumerating anything else.
        container.get_container_properties()

    def metadata(self, conn: AzureBlobConn) -> list[FileStorageEntity]:
        container = conn._container()
        # base_path is a FullPathSpec (leading "/"); the blob
        # name_starts_with wants no leading "/" and a trailing "/", so
        # normalize it (root "/" -> "" -> list the whole container).
        prefix = normalize_dir_path(
            conn._base_path(), lead_separator=False, back_separator=True
        )
        # ``bucket`` holds the raw container name and ``prefix`` the blob's
        # full key directory (which already includes base_path), so the
        # fqn resolves to ``az://<container>/<prefix>/<file>``.
        client = _container_client(conn)
        name_starts_with = prefix or None

        entities: list[FileStorageEntity] = []
        for blob in client.list_blobs(name_starts_with=name_starts_with):
            key = blob.name
            prefix, _, filename = key.rpartition("/")
            _, ext = os.path.splitext(filename)
            last_modified = blob.last_modified
            entities.append(
                FileStorageEntity(
                    connection_type=self.meta_model,
                    scheme="az",
                    bucket=container,
                    prefix=prefix,
                    file=FilePattern(pattern=filename, format=ext[1:].lower()),
                    size=blob.size,
                    last_modified=(
                        last_modified.isoformat() if last_modified else None
                    ),
                )
            )
        return entities

    def data(
        self, conn: AzureBlobConn, entity: FileStorageEntity, limit: int = 100
    ) -> list[dict]:
        if entity.file is None:
            return []
        fmt = (entity.file.format or "").lower()
        key = (
            f"{entity.prefix}/{entity.file.pattern}"
            if entity.prefix
            else entity.file.pattern
        )
        if fmt == "parquet":
            return _read_parquet(conn, key, limit)
        container = _container_client(conn)
        if fmt == "avro":
            return _read_avro(container, key, limit)
        if fmt in ("json", "ndjson", "jsonl"):
            return _read_ndjson(container, key, limit)
        # csv and every other format fall back to CSV parsing.
        return _read_csv(container, key, limit)


def _container_client(conn: AzureBlobConn):
    # azure-storage-blob is imported lazily so merely importing this
    # module (e.g. a registry scan) doesn't pay for the SDK.
    from azure.storage.blob import BlobServiceClient

    account_name = conn.credentials.account_name.value()
    account_key = conn.credentials.account_key.value()
    account_url = f"https://{account_name}.blob.core.windows.net"
    # Cap the get sizes so a `download_blob` never buffers more than ~1 MB per
    # GET (the default max_single_get_size is 32 MB, which would pull a whole
    # medium blob before we read a byte).
    service = BlobServiceClient(
        account_url=account_url,
        credential=account_key,
        max_single_get_size=1024 * 1024,
        max_chunk_get_size=1024 * 1024,
    )
    return service.get_container_client(conn._container())


def _stream_lines(container, key: str, n: int) -> list[str]:
    """First ``n`` newline-delimited lines of a blob, read in 8 KB steps off
    the streaming downloader (capped at 1 MB per GET by the client) and
    stopped as soon as they're found -- so only the leading bytes are
    fetched."""
    downloader = container.download_blob(key)
    lines: list[bytes] = []
    buf = b""
    while len(lines) < n:
        chunk = downloader.read(_CHUNK)
        if not chunk:
            if buf:
                lines.append(buf)
            break
        buf += chunk
        parts = buf.split(b"\n")
        buf = parts.pop()  # trailing partial line (or b"")
        lines.extend(parts)
    return [line.decode("utf-8") for line in lines[:n]]


def _read_csv(container, key: str, limit: int) -> list[dict]:
    # Header line + the first ``limit`` data rows -> dicts keyed by header.
    rows = _stream_lines(container, key, limit + 1)
    return list(csv.DictReader(io.StringIO("\n".join(rows))))


def _read_ndjson(container, key: str, limit: int) -> list[dict]:
    return [
        json.loads(line)
        for line in _stream_lines(container, key, limit)
        if line.strip()
    ]


def _read_avro(container, key: str, limit: int) -> list[dict]:
    # fastavro reads the container block-by-block off the streaming downloader.
    import fastavro

    downloader = container.download_blob(key)
    rows: list[dict] = []
    for i, record in enumerate(fastavro.reader(downloader)):
        if i >= limit:
            break
        rows.append(dict(record))
    return rows


def _read_parquet(conn: AzureBlobConn, key: str, limit: int) -> list[dict]:
    # AzureFileSystem serves range requests, so ParquetFile reads only the
    # footer and iter_batches only the first row group -- never the whole blob.
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    account_name = conn.credentials.account_name.value()
    account_key = conn.credentials.account_key.value()
    container = conn._container()
    fs = pafs.AzureFileSystem(account_name=account_name, account_key=account_key)
    with fs.open_input_file(f"{container}/{key}") as handle:
        parquet_file = pq.ParquetFile(handle)
        try:
            batch = next(parquet_file.iter_batches(batch_size=limit))
        except StopIteration:
            return []
        return batch.to_pylist()

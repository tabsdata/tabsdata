#
#  Copyright 2026 Tabsdata Inc.
#

"""Azure Blob Storage connection explorer.

``check_connect`` proves auth + container existence; ``metadata`` lists the
blobs under the conn's ``base_path``, with their schemas read from the blobs'
heads by range requests (``_schema.attach_schemas``); ``data`` previews the
first ``limit`` rows of a single blob, fetching only the bytes it needs (never
the whole blob) and dispatching by format: parquet via pyarrow, avro via
fastavro, xlsx via the Excel reader, json / ndjson as newline-delimited JSON,
and every other format (csv, log, ...) via CSV parsing.
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
    preview_format,
    read_excel_rows,
)
from tabsdatak.conn.common.file._read._format import Format
from tabsdatak.conn.common.file._schema import attach_schemas

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
        # base_path is a CloudRelativePathSpec (leading "/"); the blob
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
        fs = _filesystem(conn)
        return attach_schemas(
            entities,
            lambda entity: fs.open_input_file(f"{container}/{entity.object_key()}"),
        )

    def data(
        self, conn: AzureBlobConn, entity: FileStorageEntity, limit: int = 100
    ) -> list[dict]:
        if entity.file is None:
            return []
        key = entity.object_key()
        match preview_format(entity):
            case Format.AVRO:
                return _read_avro(_container_client(conn), key, limit)
            case Format.CSV:
                return _read_csv(_container_client(conn), key, limit)
            case Format.EXCEL:
                return _read_excel(conn, key, limit)
            case Format.NDJSON:
                return _read_ndjson(_container_client(conn), key, limit)
            case Format.PARQUET:
                return _read_parquet(conn, key, limit)
            case _:
                return _read_csv(_container_client(conn), key, limit)


def _filesystem(conn: AzureBlobConn):
    # pyarrow is imported lazily; AzureFileSystem serves range requests.
    import pyarrow.fs as pafs

    return pafs.AzureFileSystem(
        account_name=conn.credentials.account_name.value(),
        account_key=conn.credentials.account_key.value(),
    )


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


def _read_csv(container, key: str, limit: int) -> list[dict]:
    # Header line + the first ``limit`` data rows -> dicts keyed by header.
    rows = _stream_lines(container, key, limit + 1)
    return list(csv.DictReader(io.StringIO("\n".join(rows))))


def _read_excel(conn: AzureBlobConn, key: str, limit: int) -> list[dict]:
    return read_excel_rows(
        lambda: _filesystem(conn).open_input_file(f"{conn._container()}/{key}"),
        limit,
    )


def _read_ndjson(container, key: str, limit: int) -> list[dict]:
    return [
        json.loads(line)
        for line in _stream_lines(container, key, limit)
        if line.strip()
    ]


def _read_parquet(conn: AzureBlobConn, key: str, limit: int) -> list[dict]:
    # Range requests: ParquetFile reads only the footer and iter_batches only
    # the first row group -- never the whole blob.
    import pyarrow.parquet as pq

    with _filesystem(conn).open_input_file(f"{conn._container()}/{key}") as handle:
        parquet_file = pq.ParquetFile(handle)
        try:
            batch = next(parquet_file.iter_batches(batch_size=limit))
        except StopIteration:
            return []
        return batch.to_pylist()

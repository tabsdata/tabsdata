#
#  Copyright 2026. Tabs Data Inc.
#

"""S3 connection explorer.

``check_connect`` proves auth + bucket existence (an authenticated
``head_bucket``); ``metadata`` lists the objects under the conn's
``base_path``; ``data`` previews the first ``limit`` rows of a single object,
fetching only the bytes it needs (never the whole object) and dispatching by
format: parquet via pyarrow range requests, avro via fastavro, json / ndjson
as newline-delimited JSON, and every other format (csv, log, ...) via CSV
parsing.
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
from tabsdatak.conn.common.types import normalize_dir_path
from tabsdatak.conn.s3 import S3DestConn, S3SrcConn
from tabsdatak.spi import ConnMetaModel, Entity, Explorer

# Either S3 connection flavour -- source or destination.
S3Conn: TypeAlias = S3SrcConn | S3DestConn

_CHUNK = 8192


class S3ConnectionExplorer(Explorer):
    """Verify connectivity to an S3 bucket and preview its objects."""

    meta_model: ClassVar[ConnMetaModel] = FileStorageConnMetaModel()
    entity_type: ClassVar[type[Entity]] = FileStorageEntity

    def check_connect(self, conn: S3Conn) -> None:
        client = _s3_client(conn)
        client.head_bucket(Bucket=conn._bucket())

    def metadata(self, conn: S3Conn) -> list[FileStorageEntity]:
        bucket = conn._bucket()
        # base_path is a FullPathSpec (leading "/"); the S3 list_objects_v2
        # Prefix wants no leading "/" and a trailing "/", so normalize it
        # (root "/" -> "" -> list the whole bucket).
        prefix = normalize_dir_path(
            conn._base_path(), lead_separator=False, back_separator=True
        )
        # ``bucket`` holds the raw bucket name and ``prefix`` the object's
        # full key directory (which already includes base_path), so the
        # fqn resolves to ``s3://<bucket>/<prefix>/<file>``.
        client = _s3_client(conn)
        list_kwargs: dict = {"Bucket": bucket}
        if prefix:
            list_kwargs["Prefix"] = prefix

        entities: list[FileStorageEntity] = []
        for page in client.get_paginator("list_objects_v2").paginate(**list_kwargs):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                prefix, _, filename = key.rpartition("/")
                _, ext = os.path.splitext(filename)
                last_modified = obj.get("LastModified")
                entities.append(
                    FileStorageEntity(
                        connection_type=self.meta_model,
                        scheme="s3",
                        bucket=bucket,
                        prefix=prefix,
                        file=FilePattern(pattern=filename, format=ext[1:].lower()),
                        size=obj.get("Size"),
                        last_modified=(
                            last_modified.isoformat() if last_modified else None
                        ),
                    )
                )
        return entities

    def data(
        self, conn: S3Conn, entity: FileStorageEntity, limit: int = 100
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
        if fmt == "parquet":
            return _read_parquet(conn, bucket, key, limit)
        if fmt == "avro":
            return _read_avro(_s3_client(conn), bucket, key, limit)
        if fmt in ("json", "ndjson", "jsonl"):
            return _read_ndjson(_s3_client(conn), bucket, key, limit)
        # csv and every other format fall back to CSV parsing.
        return _read_csv(_s3_client(conn), bucket, key, limit)


def _s3_client(conn: S3Conn):
    # boto3 is imported lazily so merely importing this module (e.g. a
    # registry scan) doesn't pay for the SDK.
    import boto3

    access_key = secret_key = None
    if conn.credentials is not None:
        access_key = conn.credentials.access_key_id.value() or None
        secret_key = conn.credentials.secret_access_key.value() or None
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=conn._region() or None,
    )


def _stream_lines(client, bucket: str, key: str, n: int) -> list[str]:
    """First ``n`` newline-delimited lines of an object, read in 8 KB chunks
    and stopped as soon as they're found -- so only the leading bytes are
    fetched, never the whole object. Works on a botocore ``StreamingBody`` or
    a raw urllib3 ``HTTPResponse`` (both expose ``read`` / ``close``)."""
    body = client.get_object(Bucket=bucket, Key=key)["Body"]
    lines: list[bytes] = []
    buf = b""
    try:
        while len(lines) < n:
            chunk = body.read(_CHUNK)
            if not chunk:
                if buf:
                    lines.append(buf)
                break
            buf += chunk
            parts = buf.split(b"\n")
            buf = parts.pop()  # trailing partial line (or b"")
            lines.extend(parts)
    finally:
        body.close()
    return [line.decode("utf-8") for line in lines[:n]]


def _read_csv(client, bucket: str, key: str, limit: int) -> list[dict]:
    # Header line + the first ``limit`` data rows -> dicts keyed by header.
    rows = _stream_lines(client, bucket, key, limit + 1)
    return list(csv.DictReader(io.StringIO("\n".join(rows))))


def _read_ndjson(client, bucket: str, key: str, limit: int) -> list[dict]:
    return [
        json.loads(line)
        for line in _stream_lines(client, bucket, key, limit)
        if line.strip()
    ]


def _read_avro(client, bucket: str, key: str, limit: int) -> list[dict]:
    # fastavro is imported lazily so importing this module stays cheap; it
    # reads the avro container block-by-block off the streaming body.
    import fastavro

    body = client.get_object(Bucket=bucket, Key=key)["Body"]
    rows: list[dict] = []
    try:
        for i, record in enumerate(fastavro.reader(body)):
            if i >= limit:
                break
            rows.append(dict(record))
    finally:
        body.close()
    return rows


def _read_parquet(conn: S3Conn, bucket: str, key: str, limit: int) -> list[dict]:
    # pyarrow is imported lazily; the S3 filesystem reads via range requests,
    # so only the footer and the first row group are fetched.
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    access_key = secret_key = None
    if conn.credentials is not None:
        access_key = conn.credentials.access_key_id.value() or None
        secret_key = conn.credentials.secret_access_key.value() or None
    fs = pafs.S3FileSystem(
        access_key=access_key,
        secret_key=secret_key,
        region=conn._region() or None,
    )
    with fs.open_input_file(f"{bucket}/{key}") as handle:
        parquet_file = pq.ParquetFile(handle)
        try:
            batch = next(parquet_file.iter_batches(batch_size=limit))
        except StopIteration:
            return []
        return batch.to_pylist()

#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from system / domain fixture to connector connections and bucket access"""

from __future__ import annotations

import uuid
from pathlib import Path

from tabsdatax.itest.fixture.backends.s3 import S3Domain, S3System
from tabsdatax.itest.fixture.functions.files import ObjectStore

from tabsdatak.api import Secret
from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter.handler.handler2 import TRANSPORTER_BINARY_NAME
from tabsdatak.conn.common.types import AwsAccessSecretKey
from tabsdatak.conn.s3 import S3DestConn, S3SrcConn

# The plugins shell out to this binary; resolving it at import time fails loudly
# on an unhydrated environment instead of mid-test.
TRANSPORTER_BINARY = PythonEnvironment().binary(TRANSPORTER_BINARY_NAME)

# Either backend flavour -- both carry bucket / region / access_key / secret_key.
S3Backend = S3System | S3Domain


def credentials(backend: S3Backend) -> AwsAccessSecretKey:
    return AwsAccessSecretKey(
        access_key_id=Secret(backend.access_key),
        secret_access_key=Secret(backend.secret_key),
    )


def src_conn(backend: S3Backend, base_path: str) -> S3SrcConn:
    return S3SrcConn(
        bucket=backend.bucket,
        region=backend.region,
        credentials=credentials(backend),
        base_path=_absolute(base_path),
    )


def dest_conn(backend: S3Backend, base_path: str) -> S3DestConn:
    return S3DestConn(
        bucket=backend.bucket,
        region=backend.region,
        credentials=credentials(backend),
        base_path=_absolute(base_path),
    )


def run_prefix(root: str | Path, area: str) -> str:
    """A unique object prefix under ``root``, so a run is isolated and never
    collides with provisioned model data."""
    parent = str(root).strip("/")
    suffix = f"{area}/{uuid.uuid4().hex}"
    return f"{parent}/{suffix}" if parent else suffix


def store(backend: S3Backend) -> ObjectStore:
    """A store to stage / read back what a test wrote, over the backend's own
    credentials."""
    return S3ObjectStore(backend)


class S3ObjectStore(ObjectStore):
    """boto3-backed :class:`ObjectStore`: stage source objects and download
    emitted objects to verify the bytes the transporter wrote (the src plugin
    would re-transcode to parquet, hiding the on-object format)."""

    def __init__(self, backend: S3Backend) -> None:
        import boto3

        self._bucket = backend.bucket
        self._client = boto3.client(
            "s3",
            region_name=backend.region or None,
            aws_access_key_id=backend.access_key or None,
            aws_secret_access_key=backend.secret_key or None,
        )

    def exists(self, key: str) -> bool:
        resp = self._client.list_objects_v2(Bucket=self._bucket, Prefix=key, MaxKeys=1)
        return any(o["Key"] == key for o in resp.get("Contents", []))

    def download(self, key: str, dest: Path) -> None:
        self._client.download_file(self._bucket, key, str(dest))

    def upload(self, local: Path, key: str) -> None:
        self._client.upload_file(str(local), self._bucket, key)


def _absolute(base_path: str) -> str:
    return "/" + str(base_path).strip("/")

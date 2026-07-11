#
# Copyright 2026 Tabs Data Inc.
#

"""Shared S3 test fixtures for this package's integration and e2e suites: a
boto3-backed :class:`ObjectStore` over whichever S3 backend the test declared
(its ``system`` for integration, its ``domain`` for e2e -- both expose
``bucket`` / ``region`` / ``access_key`` / ``secret_key``)."""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.functions.files import ObjectStore


class _S3ObjectStore(ObjectStore):
    """boto3-backed :class:`ObjectStore`: stage source objects and download
    emitted objects to verify the bytes the transporter wrote (the src plugin
    would re-transcode to parquet, hiding the on-object format)."""

    def __init__(self, backend) -> None:
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


@pytest.fixture
def object_store():
    """Factory building an :class:`ObjectStore` over the S3 backend the test
    declared -- call it with ``system_s3`` (integration) or ``domain_s3`` (e2e)."""
    return _S3ObjectStore

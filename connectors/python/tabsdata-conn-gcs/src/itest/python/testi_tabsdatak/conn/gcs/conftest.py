#
# Copyright 2026 Tabsdata Inc.
#

"""Shared GCS test fixtures for this package's integration and e2e suites: a
google-cloud-storage-backed :class:`ObjectStore` over whichever GCS backend the
test declared (its ``system`` for integration, its ``domain`` for e2e -- both
expose ``bucket`` / ``service_account_key``)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tabsdatax.itest.fixture.functions.files import ObjectStore


class _GcsObjectStore(ObjectStore):
    """google-cloud-storage-backed :class:`ObjectStore`: stage source objects and
    download emitted objects to verify the bytes the transporter wrote (the src
    plugin would re-transcode to parquet, hiding the on-object format)."""

    def __init__(self, backend) -> None:
        from google.cloud import storage

        self._bucket = storage.Client.from_service_account_info(
            json.loads(backend.service_account_key)
        ).bucket(backend.bucket)

    def exists(self, key: str) -> bool:
        return self._bucket.blob(key).exists()

    def download(self, key: str, dest: Path) -> None:
        self._bucket.blob(key).download_to_filename(str(dest))

    def upload(self, local: Path, key: str) -> None:
        self._bucket.blob(key).upload_from_filename(str(local))


@pytest.fixture
def object_store():
    """Factory building an :class:`ObjectStore` over the GCS backend the test
    declared -- call it with ``system_gcs`` (integration) or ``domain_gcs`` (e2e)."""
    return _GcsObjectStore

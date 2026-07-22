#
# Copyright 2026 Tabsdata Inc.
#

"""Shared Azure Blob test fixtures for this package's integration and e2e suites:
an azure-storage-blob-backed :class:`ObjectStore` over whichever Azure backend the
test declared (its ``system`` for integration, its ``domain`` for e2e -- both
expose ``container`` / ``account_name`` / ``account_key``)."""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.functions.files import ObjectStore


class _AzureObjectStore(ObjectStore):
    """azure-storage-blob-backed :class:`ObjectStore`: stage source blobs and
    download emitted blobs to verify the bytes the transporter wrote (the src
    plugin would re-transcode to parquet, hiding the on-object format)."""

    def __init__(self, backend) -> None:
        from azure.storage.blob import BlobServiceClient

        self._container = backend.container
        self._service = BlobServiceClient(
            account_url=f"https://{backend.account_name}.blob.core.windows.net",
            credential=backend.account_key,
        )

    def _blob(self, key: str):
        return self._service.get_blob_client(container=self._container, blob=key)

    def exists(self, key: str) -> bool:
        return self._blob(key).exists()

    def download(self, key: str, dest: Path) -> None:
        with open(dest, "wb") as fh:
            fh.write(self._blob(key).download_blob().readall())

    def upload(self, local: Path, key: str) -> None:
        with open(local, "rb") as fh:
            self._blob(key).upload_blob(fh, overwrite=True)


@pytest.fixture
def object_store():
    """Factory building an :class:`ObjectStore` over the Azure backend the test
    declared -- call it with ``system_azureblob`` (integration) or
    ``domain_azureblob`` (e2e)."""
    return _AzureObjectStore

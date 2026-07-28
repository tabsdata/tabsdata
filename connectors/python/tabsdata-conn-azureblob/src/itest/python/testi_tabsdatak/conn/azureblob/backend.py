#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from system / domain fixture to connector connections and blob access"""

from __future__ import annotations

import uuid
from pathlib import Path

from tabsdatax.itest.fixture.backends.azureblob import AzureblobDomain, AzureblobSystem
from tabsdatax.itest.fixture.functions.files import ObjectStore

from tabsdatak.api import Secret
from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter.handler.handler2 import TRANSPORTER_BINARY_NAME
from tabsdatak.conn.azureblob import AzureBlobDestConn, AzureBlobSrcConn
from tabsdatak.conn.common.types import AzureAccountKey

# The plugins shell out to this binary; resolving it at import time fails loudly
# on an unhydrated environment instead of mid-test.
TRANSPORTER_BINARY = PythonEnvironment().binary(TRANSPORTER_BINARY_NAME)

# Either backend flavour -- both carry container / account_name / account_key.
AzureblobBackend = AzureblobSystem | AzureblobDomain


def credentials(backend: AzureblobBackend) -> AzureAccountKey:
    return AzureAccountKey(
        account_name=Secret(backend.account_name),
        account_key=Secret(backend.account_key),
    )


def src_conn(backend: AzureblobBackend, base_path: str) -> AzureBlobSrcConn:
    return AzureBlobSrcConn(
        container=backend.container,
        credentials=credentials(backend),
        base_path=_absolute(base_path),
    )


def dest_conn(backend: AzureblobBackend, base_path: str) -> AzureBlobDestConn:
    return AzureBlobDestConn(
        container=backend.container,
        credentials=credentials(backend),
        base_path=_absolute(base_path),
    )


def run_prefix(root: str | Path, area: str) -> str:
    """A unique blob prefix under ``root``, so a run is isolated and never
    collides with provisioned model data."""
    parent = str(root).strip("/")
    suffix = f"{area}/{uuid.uuid4().hex}"
    return f"{parent}/{suffix}" if parent else suffix


def store(backend: AzureblobBackend) -> ObjectStore:
    """A store to stage / read back what a test wrote, over the backend's own
    credentials."""
    return AzureObjectStore(backend)


class AzureObjectStore(ObjectStore):
    """azure-storage-blob-backed :class:`ObjectStore`: stage source blobs and
    download emitted blobs to verify the bytes the transporter wrote (the src
    plugin would re-transcode to parquet, hiding the on-object format)."""

    def __init__(self, backend: AzureblobBackend) -> None:
        from azure.storage.blob import BlobServiceClient

        self._container = backend.container
        self._service = BlobServiceClient(
            account_url=f"https://{backend.account_name}.blob.core.windows.net",
            credential=backend.account_key,
        )

    def exists(self, key: str) -> bool:
        return self._blob(key).exists()

    def download(self, key: str, dest: Path) -> None:
        with open(dest, "wb") as fh:
            fh.write(self._blob(key).download_blob().readall())

    def upload(self, local: Path, key: str) -> None:
        with open(local, "rb") as fh:
            self._blob(key).upload_blob(fh, overwrite=True)

    def _blob(self, key: str):
        return self._service.get_blob_client(container=self._container, blob=key)


def _absolute(base_path: str) -> str:
    return "/" + str(base_path).strip("/")

#
# Copyright 2026 Tabsdata Inc.
#

"""Adapters from system / domain fixture to connector connections and bucket access"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from tabsdatax.itest.fixture.backends.gcs import GCSDomain, GCSSystem
from tabsdatax.itest.fixture.functions.files import ObjectStore

from tabsdatak.api import Secret
from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter.handler.handler2 import TRANSPORTER_BINARY_NAME
from tabsdatak.conn.common.types import GCPJson
from tabsdatak.conn.gcs import GCSDestConn, GCSSrcConn

# The plugins shell out to this binary; resolving it at import time fails loudly
# on an unhydrated environment instead of mid-test.
TRANSPORTER_BINARY = PythonEnvironment().binary(TRANSPORTER_BINARY_NAME)

# Either backend flavour -- both carry bucket / service_account_key.
GCSBackend = GCSSystem | GCSDomain


def credentials(backend: GCSBackend) -> GCPJson:
    return GCPJson(json=Secret(backend.service_account_key))


def src_conn(backend: GCSBackend, base_path: str) -> GCSSrcConn:
    return GCSSrcConn(
        bucket=backend.bucket,
        credentials=credentials(backend),
        base_path=_absolute(base_path),
    )


def dest_conn(backend: GCSBackend, base_path: str) -> GCSDestConn:
    return GCSDestConn(
        bucket=backend.bucket,
        credentials=credentials(backend),
        base_path=_absolute(base_path),
    )


def run_prefix(root: str | Path, area: str) -> str:
    """A unique object prefix under ``root``, so a run is isolated and never
    collides with provisioned model data."""
    parent = str(root).strip("/")
    suffix = f"{area}/{uuid.uuid4().hex}"
    return f"{parent}/{suffix}" if parent else suffix


def store(backend: GCSBackend) -> ObjectStore:
    """A store to stage / read back what a test wrote, over the backend's own
    credentials."""
    return GCSObjectStore(backend)


class GCSObjectStore(ObjectStore):
    """google-cloud-storage-backed :class:`ObjectStore`: stage source objects and
    download emitted objects to verify the bytes the transporter wrote (the src
    plugin would re-transcode to parquet, hiding the on-object format)."""

    def __init__(self, backend: GCSBackend) -> None:
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


def _absolute(base_path: str) -> str:
    return "/" + str(base_path).strip("/")

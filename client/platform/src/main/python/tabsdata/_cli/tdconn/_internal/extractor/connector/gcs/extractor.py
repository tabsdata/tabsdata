#
#  Copyright 2026. Tabs Data Inc.
#
"""Google Cloud Storage connection extractor.

Storage systems have no schema or row-level sampling notion, so
:meth:`metadata` and :meth:`data` are intentional no-ops.
"""

import json
from typing import ClassVar

from google.cloud import storage
from google.oauth2 import service_account

from tabsdata._cli.tdconn._internal.extractor.api import (
    ConnectionExtractor,
    ConnMetaModel,
    Entity,
    FileStorageConnMetaModel,
    FileStorageEntity,
)
from tabsdata._io.connections.file_connections.gcp_connection import GCSConn


class GCSConnectionExtractor(ConnectionExtractor):
    """Verify connectivity to a GCS bucket.

    The ``info`` dict matches ``GCSConn``'s JSON shape:
        bucket_uri: ``gs://bucket_name`` (no path)
        credentials: {service_account_key}  (inline service-account JSON)

    Extractor-level options (not part of GCSConn):
        project: GCP project override; defaults to the one the key carries.
        endpoint_url: override for GCS-compatible emulators (fake-gcs-server).
    """

    meta_model: ClassVar[ConnMetaModel] = FileStorageConnMetaModel()
    entity_type: ClassVar[type[Entity]] = FileStorageEntity

    def check_connect(self, info: dict) -> None:
        conn = GCSConn._load_from_dict(_without_extras(info))
        client = _storage_client(conn, info.get("project"))
        # bucket.reload() triggers a metadata fetch — proves auth + existence.
        client.bucket(conn.bucket_name).reload()

    def metadata(self, info: dict) -> list[FileStorageEntity]:
        return []


def _storage_client(conn: GCSConn, project: str | None) -> storage.Client:
    creds = None
    key = ""
    if conn.credentials is not None:
        key = conn.credentials.service_account_key.secret_value or ""
    if key:
        payload = json.loads(key) if isinstance(key, str) else key
        if payload:
            creds = service_account.Credentials.from_service_account_info(payload)
    return storage.Client(project=project or None, credentials=creds)


def _without_extras(info: dict) -> dict:
    extras = {"project", "endpoint_url"}
    return {k: v for k, v in info.items() if k not in extras}

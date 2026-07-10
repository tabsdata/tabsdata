#
#  Copyright 2026. Tabs Data Inc.
#

"""GCS connection explorer.

Object storage has no schema or row-level sampling notion, so ``data``
is a no-op; ``check_connect`` proves auth + bucket existence and
``metadata`` lists the objects under the conn's ``base_path``.
"""

import os
from typing import ClassVar, TypeAlias

from tabsdatak.conn.common.file._explorer import (
    FilePattern,
    FileStorageConnMetaModel,
    FileStorageEntity,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.gcs import GCSDestConn, GCSSrcConn
from tabsdatak.spi import ConnMetaModel, Entity, Explorer

# Either GCS connection flavour -- source or destination.
GCSConn: TypeAlias = GCSSrcConn | GCSDestConn


class GCSConnectionExplorer(Explorer):
    """Verify connectivity to a GCS bucket."""

    meta_model: ClassVar[ConnMetaModel] = FileStorageConnMetaModel()
    entity_type: ClassVar[type[Entity]] = FileStorageEntity

    def check_connect(self, conn: GCSConn) -> None:
        client = _storage_client(conn)
        # A bounded object list proves auth + bucket reachability using only
        # object-level access (``storage.objects.list``).
        blobs = client.list_blobs(_convert(conn.bucket, str), max_results=1)
        next(iter(blobs), None)

    def metadata(self, conn: GCSConn) -> list[FileStorageEntity]:
        bucket = _convert(conn.bucket, str)
        base_path = _convert(conn.base_path, str).strip("/") if conn.base_path else ""
        # ``bucket`` holds the raw bucket name and ``prefix`` the object's
        # full key directory (which already includes base_path), so the
        # fqn resolves to ``gs://<bucket>/<prefix>/<file>``.
        client = _storage_client(conn)
        prefix_arg = base_path + "/" if base_path else None

        entities: list[FileStorageEntity] = []
        for blob in client.list_blobs(bucket, prefix=prefix_arg):
            key = blob.name
            prefix, _, filename = key.rpartition("/")
            _, ext = os.path.splitext(filename)
            entities.append(
                FileStorageEntity(
                    connection_type=self.meta_model,
                    scheme="gs",
                    bucket=bucket,
                    prefix=prefix,
                    file=FilePattern(pattern=filename, format=ext[1:].lower()),
                )
            )
        return entities


def _storage_client(conn: GCSConn):
    # google-cloud-storage is imported lazily so merely importing this
    # module (e.g. a registry scan) doesn't pay for the SDK.
    import json

    from google.cloud import storage
    from google.oauth2 import service_account

    creds = None
    if conn.credentials is not None:
        raw = _convert(conn.credentials.json, str)
        payload = json.loads(raw) if raw else {}
        if payload:
            creds = service_account.Credentials.from_service_account_info(payload)
    project = _convert(conn.project, str) if conn.project is not None else None
    return storage.Client(project=project or None, credentials=creds)

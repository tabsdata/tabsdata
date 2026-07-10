#
#  Copyright 2026. Tabs Data Inc.
#

"""Azure Blob Storage connection explorer.

Object storage has no schema or row-level sampling notion, so ``data``
is a no-op; ``check_connect`` proves auth + container existence and
``metadata`` lists the blobs under the conn's ``base_path``.
"""

import os
from typing import ClassVar, TypeAlias

from tabsdatak.conn.azureblob import AzureBlobDestConn, AzureBlobSrcConn
from tabsdatak.conn.common.file.explorer import (
    FilePattern,
    FileStorageConnMetaModel,
    FileStorageEntity,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.spi import ConnMetaModel, Entity, Explorer

# Either Azure Blob connection flavour -- source or destination.
AzureBlobConn: TypeAlias = AzureBlobSrcConn | AzureBlobDestConn


class AzureBlobConnectionExplorer(Explorer):
    """Verify connectivity to an Azure Blob Storage container."""

    meta_model: ClassVar[ConnMetaModel] = FileStorageConnMetaModel()
    entity_type: ClassVar[type[Entity]] = FileStorageEntity

    def check_connect(self, conn: AzureBlobConn) -> None:
        container = _container_client(conn)
        # get_container_properties confirms the credentials can reach this
        # specific container without enumerating anything else.
        container.get_container_properties()

    def metadata(self, conn: AzureBlobConn) -> list[FileStorageEntity]:
        container = _convert(conn.container, str)
        base_path = _convert(conn.base_path, str).strip("/") if conn.base_path else ""
        # ``bucket`` holds the raw container name and ``prefix`` the blob's
        # full key directory (which already includes base_path), so the
        # fqn resolves to ``az://<container>/<prefix>/<file>``.
        client = _container_client(conn)
        name_starts_with = base_path + "/" if base_path else None

        entities: list[FileStorageEntity] = []
        for blob in client.list_blobs(name_starts_with=name_starts_with):
            key = blob.name
            prefix, _, filename = key.rpartition("/")
            _, ext = os.path.splitext(filename)
            entities.append(
                FileStorageEntity(
                    connection_type=self.meta_model,
                    scheme="az",
                    bucket=container,
                    prefix=prefix,
                    file=FilePattern(pattern=filename, format=ext[1:].lower()),
                )
            )
        return entities


def _container_client(conn: AzureBlobConn):
    # azure-storage-blob is imported lazily so merely importing this
    # module (e.g. a registry scan) doesn't pay for the SDK.
    from azure.storage.blob import BlobServiceClient

    account_name = conn.credentials.account_name.value()
    account_key = conn.credentials.account_key.value()
    account_url = f"https://{account_name}.blob.core.windows.net"
    service = BlobServiceClient(account_url=account_url, credential=account_key)
    return service.get_container_client(_convert(conn.container, str))

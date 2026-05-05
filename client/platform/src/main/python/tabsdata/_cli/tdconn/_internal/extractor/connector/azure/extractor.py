#
#  Copyright 2026. Tabs Data Inc.
#
"""Azure Blob Storage connection extractor.

Storage systems have no schema or row-level sampling notion, so
:meth:`metadata` and :meth:`data` are intentional no-ops.
"""

from typing import ClassVar

from azure.storage.blob import BlobServiceClient

from tabsdata._cli.tdconn._internal.extractor.api import (
    ConnectionExtractor,
    ConnMetaModel,
    Entity,
    FileStorageConnMetaModel,
    FileStorageEntity,
)
from tabsdata._io.connections.file_connections.azure_connection import AzureConn


class AzureConnectionExtractor(ConnectionExtractor):
    """Verify connectivity to an Azure Blob Storage container.

    The ``info`` dict matches ``AzureConn``'s JSON shape:
        container_uri: ``az://container_name`` (no path)
        credentials: {account_name, account_key}

    Extractor-level option (not part of AzureConn):
        connection_string: full Azure connection string; when set, the
            client is built from it instead of (account_name, account_key).
            Useful for Azurite/emulator URLs.
    """

    meta_model: ClassVar[ConnMetaModel] = FileStorageConnMetaModel()
    entity_type: ClassVar[type[Entity]] = FileStorageEntity

    def check_connect(self, info: dict) -> None:
        conn = AzureConn._load_from_dict(_without_extras(info))
        svc = _blob_service_client(conn, info.get("connection_string"))
        # get_container_properties confirms the credentials can reach this
        # specific container without enumerating anything else.
        svc.get_container_client(conn.container_name).get_container_properties()

    def metadata(self, info: dict) -> list[FileStorageEntity]:
        return []


def _blob_service_client(
    conn: AzureConn, connection_string: str | None
) -> BlobServiceClient:
    if connection_string:
        return BlobServiceClient.from_connection_string(connection_string)
    account_name = conn.credentials.account_name.secret_value
    account_key = conn.credentials.account_key.secret_value
    account_url = f"https://{account_name}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=account_key)


def _without_extras(info: dict) -> dict:
    return {k: v for k, v in info.items() if k != "connection_string"}

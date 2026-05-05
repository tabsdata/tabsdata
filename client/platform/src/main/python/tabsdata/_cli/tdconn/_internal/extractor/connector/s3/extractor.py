#
#  Copyright 2026. Tabs Data Inc.
#
"""S3 connection extractor.

Storage systems have no schema or row-level sampling notion, so
:meth:`metadata` and :meth:`data` are intentional no-ops. Only
:meth:`check_connect` does meaningful work.
"""

from typing import ClassVar

import boto3

from tabsdata._cli.tdconn._internal.extractor.api import (
    ConnectionExtractor,
    ConnMetaModel,
    Entity,
    FileStorageConnMetaModel,
    FileStorageEntity,
)
from tabsdata._io.connections.file_connections.s3_connection import S3Conn


class S3ConnectionExtractor(ConnectionExtractor):
    """Verify connectivity to an S3 bucket.

    The ``info`` dict matches ``S3Conn``'s JSON shape:
        bucket_uri: ``s3://bucket_name`` (no path)
        credentials: {aws_access_key_id, aws_secret_access_key}
        region: AWS region (optional)

    Extractor-level option (not part of S3Conn):
        endpoint_url: override for S3-compatible systems (MinIO, etc.).
    """

    meta_model: ClassVar[ConnMetaModel] = FileStorageConnMetaModel()
    entity_type: ClassVar[type[Entity]] = FileStorageEntity

    def check_connect(self, info: dict) -> None:
        conn = S3Conn._load_from_dict(_without_extras(info))
        client = _s3_client(conn, info.get("endpoint_url"))
        # head_bucket is the cheapest authenticated call that proves both
        # credentials and bucket-existence.
        client.head_bucket(Bucket=conn.bucket_name)

    def metadata(self, info: dict) -> list[FileStorageEntity]:
        return []


def _s3_client(conn: S3Conn, endpoint_url: str | None):
    access_key = secret_key = None
    if conn.credentials is not None:
        access_key = conn.credentials.aws_access_key_id.secret_value or None
        secret_key = conn.credentials.aws_secret_access_key.secret_value or None
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=conn.region or None,
        endpoint_url=endpoint_url or None,
    )


def _without_extras(info: dict) -> dict:
    return {k: v for k, v in info.items() if k != "endpoint_url"}

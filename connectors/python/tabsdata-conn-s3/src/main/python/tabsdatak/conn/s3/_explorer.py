#
#  Copyright 2026. Tabs Data Inc.
#

"""S3 connection explorer.

Object storage has no row-level sampling notion, so ``data`` is a no-op;
``check_connect`` proves auth + bucket existence (an authenticated
``head_bucket``) and ``metadata`` lists the objects under the conn's
``base_path``.
"""

import os
from typing import ClassVar, TypeAlias

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.common.file._explorer import (
    FilePattern,
    FileStorageConnMetaModel,
    FileStorageEntity,
)
from tabsdatak.conn.s3 import S3DestConn, S3SrcConn
from tabsdatak.spi import ConnMetaModel, Entity, Explorer

# Either S3 connection flavour -- source or destination.
S3Conn: TypeAlias = S3SrcConn | S3DestConn


class S3ConnectionExplorer(Explorer):
    """Verify connectivity to an S3 bucket."""

    meta_model: ClassVar[ConnMetaModel] = FileStorageConnMetaModel()
    entity_type: ClassVar[type[Entity]] = FileStorageEntity

    def check_connect(self, conn: S3Conn) -> None:
        client = _s3_client(conn)
        client.head_bucket(Bucket=_convert(conn.bucket, str))

    def metadata(self, conn: S3Conn) -> list[FileStorageEntity]:
        bucket = _convert(conn.bucket, str)
        base_path = _convert(conn.base_path, str).strip("/") if conn.base_path else ""
        # ``bucket`` holds the raw bucket name and ``prefix`` the object's
        # full key directory (which already includes base_path), so the
        # fqn resolves to ``s3://<bucket>/<prefix>/<file>``.
        client = _s3_client(conn)
        list_kwargs: dict = {"Bucket": bucket}
        if base_path:
            list_kwargs["Prefix"] = base_path + "/"

        entities: list[FileStorageEntity] = []
        for page in client.get_paginator("list_objects_v2").paginate(**list_kwargs):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                prefix, _, filename = key.rpartition("/")
                _, ext = os.path.splitext(filename)
                entities.append(
                    FileStorageEntity(
                        connection_type=self.meta_model,
                        scheme="s3",
                        bucket=bucket,
                        prefix=prefix,
                        file=FilePattern(pattern=filename, format=ext[1:].lower()),
                    )
                )
        return entities


def _s3_client(conn: S3Conn):
    # boto3 is imported lazily so merely importing this module (e.g. a
    # registry scan) doesn't pay for the SDK.
    import boto3

    access_key = secret_key = None
    if conn.credentials is not None:
        access_key = conn.credentials.access_key_id.value() or None
        secret_key = conn.credentials.secret_access_key.value() or None
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=_convert(conn.region, str) or None,
    )

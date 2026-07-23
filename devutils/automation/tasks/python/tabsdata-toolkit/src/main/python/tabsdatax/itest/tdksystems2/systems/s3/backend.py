#
# Copyright 2026 Tabsdata Inc.
#

"""Amazon S3 backend (boto3) — object-store domains as bucket prefixes.

Config from the ``S3__`` environment: ``S3__BUCKET``, ``S3__REGION``,
``S3__ACCESS_KEY``, ``S3__SECRET_KEY`` (blank keys fall back to boto3's default
credential chain), ``S3__SYS_PATH`` (staging-root prefix). Only the S3 storage
ops live here — the rest is :class:`CloudFileBackend`.
"""

from __future__ import annotations

from functools import cached_property

from tabsdatax.itest.tdksystems2 import options
from tabsdatax.itest.tdksystems2.systems._common.files import (
    CloudFileBackend,
    normalise_prefix,
    walk_files,
)


class S3(CloudFileBackend):
    slug = "s3"
    label = "S3"
    env_prefix = "S3"
    uri_scheme = "s3"

    def options(self) -> tuple[options.Option, ...]:
        return (
            options.Option("BUCKET", "bucket", options.ENV, "env $S3__BUCKET"),
            options.Option(
                "REGION", "region", options.ENV, "env $S3__REGION", required=False
            ),
            options.Option(
                "ACCESS_KEY",
                "access-key",
                options.ENV,
                "env $S3__ACCESS_KEY (else boto3 default chain)",
                secret=True,
                required=False,
            ),
            options.Option(
                "SECRET_KEY",
                "secret-key",
                options.ENV,
                "env $S3__SECRET_KEY (else boto3 default chain)",
                secret=True,
                required=False,
            ),
            options.Option("SYS_PATH", "sys-path", options.ENV, "env $S3__SYS_PATH"),
        )

    def _bucket(self) -> str:
        return self._cfg("BUCKET")

    @cached_property
    def _client(self):
        import boto3

        return boto3.client(
            "s3",
            region_name=self._cfg("REGION", required=False) or None,
            aws_access_key_id=self._cfg("ACCESS_KEY", required=False) or None,
            aws_secret_access_key=self._cfg("SECRET_KEY", required=False) or None,
        )

    def store_status(self, root: str) -> None:
        self._client.head_bucket(Bucket=self._bucket())

    def store_exists(self, dest: str) -> bool:
        resp = self._client.list_objects_v2(
            Bucket=self._bucket(), Prefix=normalise_prefix(dest), MaxKeys=1
        )
        return resp.get("KeyCount", 0) > 0

    def store_children(self, root: str) -> list[str]:
        p = normalise_prefix(root)
        names: list[str] = []
        for page in self._client.get_paginator("list_objects_v2").paginate(
            Bucket=self._bucket(), Prefix=p, Delimiter="/"
        ):
            for cp in page.get("CommonPrefixes", []) or []:
                child = cp.get("Prefix", "")[len(p) :].rstrip("/")
                if child:
                    names.append(child)
        return sorted(names)

    def store_clear(self, dest: str) -> None:
        p = normalise_prefix(dest)
        bucket = self._bucket()
        for page in self._client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=p
        ):
            objs = [{"Key": o["Key"]} for o in page.get("Contents", []) or []]
            for i in range(0, len(objs), 1000):
                self._client.delete_objects(
                    Bucket=bucket, Delete={"Objects": objs[i : i + 1000]}
                )

    def store_upload(self, dest: str, local_dir: str) -> int:
        p = normalise_prefix(dest)
        bucket = self._bucket()
        n = 0
        for src, rel in walk_files(local_dir):
            self._client.upload_file(str(src), bucket, p + rel)
            n += 1
        return n

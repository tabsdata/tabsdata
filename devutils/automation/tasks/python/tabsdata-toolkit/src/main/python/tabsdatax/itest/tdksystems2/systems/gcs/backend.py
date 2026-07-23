#
# Copyright 2026 Tabsdata Inc.
#

"""Google Cloud Storage backend (google-cloud-storage) — bucket-prefix domains.

Config from the ``GCS__`` environment: ``GCS__BUCKET`` and
``GCS__SERVICE_ACCOUNT_KEY`` (inline JSON — starts with ``{`` — or a file path),
plus ``GCS__SYS_PATH`` (staging-root prefix). The S3-interop HMAC keys
(``GCS__HMAC_ACCESS_KEY`` / ``GCS__HMAC_SECRET_KEY``) are carried in the descriptor
for consumers that reach GCS over the S3 protocol (ClickHouse / StarRocks
staging); the storage ops here use the service-account key. Only the GCS storage
ops live here — the rest is :class:`CloudFileBackend`.
"""

from __future__ import annotations

import json
from functools import cached_property

from tabsdatax.itest.tdksystems2 import options
from tabsdatax.itest.tdksystems2.systems._common.files import (
    CloudFileBackend,
    normalise_prefix,
    walk_files,
)


class Gcs(CloudFileBackend):
    slug = "gcs"
    label = "GCS"
    env_prefix = "GCS"
    uri_scheme = "gs"

    def options(self) -> tuple[options.Option, ...]:
        return (
            options.Option("BUCKET", "bucket", options.ENV, "env $GCS__BUCKET"),
            options.Option(
                "SERVICE_ACCOUNT_KEY",
                "service-account-key",
                options.ENV,
                "env $GCS__SERVICE_ACCOUNT_KEY (inline JSON or a file path)",
                secret=True,
            ),
            # S3-interop (HMAC) credentials, baked into the descriptor for consumers
            # that reach GCS over the S3 protocol (ClickHouse / StarRocks staging).
            # Optional: a GCS system without S3-interop keys still creates.
            options.Option(
                "HMAC_ACCESS_KEY",
                "hmac-access-key",
                options.ENV,
                "env $GCS__HMAC_ACCESS_KEY (S3-interop access key)",
                required=False,
            ),
            options.Option(
                "HMAC_SECRET_KEY",
                "hmac-secret-key",
                options.ENV,
                "env $GCS__HMAC_SECRET_KEY (S3-interop secret key)",
                secret=True,
                required=False,
            ),
            options.Option("SYS_PATH", "sys-path", options.ENV, "env $GCS__SYS_PATH"),
        )

    def _bucket(self) -> str:
        return self._cfg("BUCKET")

    @cached_property
    def _client(self):
        from google.cloud import storage  # type: ignore[import-untyped]

        sa = self._cfg("SERVICE_ACCOUNT_KEY")
        if sa.lstrip().startswith("{"):
            return storage.Client.from_service_account_info(json.loads(sa))
        return storage.Client.from_service_account_json(sa)

    def store_status(self, root: str) -> None:
        # Object-level probe (least-priv SAs may lack buckets.get).
        next(iter(self._client.list_blobs(self._bucket(), max_results=1)), None)

    def store_exists(self, dest: str) -> bool:
        p = normalise_prefix(dest)
        return any(self._client.list_blobs(self._bucket(), prefix=p, max_results=1))

    def store_children(self, root: str) -> list[str]:
        p = normalise_prefix(root)
        it = self._client.list_blobs(self._bucket(), prefix=p, delimiter="/")
        for _ in it:  # exhaust so `prefixes` is populated
            pass
        names = [cp[len(p) :].rstrip("/") for cp in it.prefixes]
        return sorted(n for n in names if n)

    def store_clear(self, dest: str) -> None:
        p = normalise_prefix(dest)
        for blob in list(self._client.list_blobs(self._bucket(), prefix=p)):
            blob.delete()

    def store_upload(self, dest: str, local_dir: str) -> int:
        bucket = self._client.bucket(self._bucket())
        p = normalise_prefix(dest)
        n = 0
        for src, rel in walk_files(local_dir):
            bucket.blob(p + rel).upload_from_filename(str(src))
            n += 1
        return n

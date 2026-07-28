#
# Copyright 2026 Tabsdata Inc.
#

"""Google Cloud Storage system — hooks only; the lifecycle is in CloudBase."""

from __future__ import annotations

from .._cloud import CloudBase
from ..base import Option
from .backend import GCSBackend


class GCSSystem(CloudBase):
    slug = "gcs"
    label = "GCS"
    options = (
        Option("BUCKET", "bucket"),
        Option("SERVICE_ACCOUNT_KEY", "service-account-key", secret=True),
        # S3-interop (HMAC) credentials for an S3-compatible connection.
        Option("HMAC_ACCESS_KEY", "hmac-access-key"),
        Option("HMAC_SECRET_KEY", "hmac-secret-key", secret=True),
    )

    def _backend(self, values: dict[str, str]) -> GCSBackend:
        return GCSBackend(values)

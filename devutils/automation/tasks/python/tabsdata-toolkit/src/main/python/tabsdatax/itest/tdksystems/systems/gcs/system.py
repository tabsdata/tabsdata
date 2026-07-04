"""Google Cloud Storage system — hooks only; the lifecycle is in CloudBase."""
from __future__ import annotations

from .._cloud import CloudBase
from ..base import Option
from .backend import GcsBackend


class GcsSystem(CloudBase):
    slug = "gcs"
    label = "GCS"
    options = (
        Option("BUCKET", "bucket"),
        Option("SERVICE_ACCOUNT_KEY", "service-account-key", secret=True),
    )

    def _backend(self, values: dict[str, str]) -> GcsBackend:
        return GcsBackend(values)

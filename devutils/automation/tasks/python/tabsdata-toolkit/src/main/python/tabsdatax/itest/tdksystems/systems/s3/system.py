"""S3 object-store system — hooks only; the lifecycle is in CloudBase."""
from __future__ import annotations

from .._cloud import CloudBase
from ..base import Option
from .backend import S3Backend


class S3System(CloudBase):
    slug = "s3"
    label = "S3"
    options = (
        Option("BUCKET", "bucket"),
        Option("REGION", "region"),
        Option("ACCESS_KEY", "access-key"),
        Option("SECRET_KEY", "secret-key", secret=True),
    )

    def _backend(self, values: dict[str, str]) -> S3Backend:
        return S3Backend(values)

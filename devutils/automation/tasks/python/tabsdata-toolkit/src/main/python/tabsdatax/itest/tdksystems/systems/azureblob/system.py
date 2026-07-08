"""Azure Blob object-store system — hooks only; the lifecycle is in CloudBase."""

from __future__ import annotations

from .._cloud import CloudBase
from ..base import Option
from .backend import AzureblobBackend


class AzureblobSystem(CloudBase):
    slug = "azureblob"
    label = "Azure Blob"
    env_prefix = "AZ"  # $AZ__CONTAINER / $AZ__ACCOUNT_NAME / $AZ__ACCOUNT_KEY
    options = (
        Option("CONTAINER", "container"),
        Option("ACCOUNT_NAME", "account-name"),
        Option("ACCOUNT_KEY", "account-key", secret=True),
    )

    def _backend(self, values: dict[str, str]) -> AzureblobBackend:
        return AzureblobBackend(values)

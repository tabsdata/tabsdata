"""Azure Blob object-store system — hooks only; the lifecycle is in CloudBase."""

from __future__ import annotations

from .._cloud import CloudBase
from ..base import Option
from .backend import AzureblobBackend


class AzureblobSystem(CloudBase):
    slug = "azureblob"
    label = "Azure Blob"
    options = (
        Option("CONTAINER", "container"),
        Option("ACCOUNT_NAME", "account-name"),
        Option("ACCOUNT_KEY", "account-key", secret=True),
    )

    def _backend(self, values: dict[str, str]) -> AzureblobBackend:
        return AzureblobBackend(values)

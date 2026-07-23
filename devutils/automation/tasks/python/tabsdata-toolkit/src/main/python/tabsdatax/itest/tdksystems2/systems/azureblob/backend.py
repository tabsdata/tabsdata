#
# Copyright 2026 Tabsdata Inc.
#

"""Azure Blob Storage backend (azure-storage-blob) — container-prefix domains.

Config from the ``AZ__`` environment: ``AZ__ACCOUNT_NAME``, ``AZ__ACCOUNT_KEY``,
``AZ__CONTAINER``, plus ``AZ__SYS_PATH`` (staging-root prefix). Auth is the shared
account key over the HTTPS blob endpoint. Only the Azure storage ops live here —
the rest is :class:`CloudFileBackend`.
"""

from __future__ import annotations

from functools import cached_property

from tabsdatax.itest.tdksystems2 import options
from tabsdatax.itest.tdksystems2.systems._common.files import (
    CloudFileBackend,
    normalise_prefix,
    walk_files,
)


class Azureblob(CloudFileBackend):
    slug = "azureblob"
    label = "Azure Blob"
    env_prefix = "AZ"
    uri_scheme = "az"

    def options(self) -> tuple[options.Option, ...]:
        return (
            options.Option("CONTAINER", "container", options.ENV, "env $AZ__CONTAINER"),
            options.Option(
                "ACCOUNT_NAME", "account-name", options.ENV, "env $AZ__ACCOUNT_NAME"
            ),
            options.Option(
                "ACCOUNT_KEY",
                "account-key",
                options.ENV,
                "env $AZ__ACCOUNT_KEY",
                secret=True,
            ),
            options.Option("SYS_PATH", "sys-path", options.ENV, "env $AZ__SYS_PATH"),
        )

    def _bucket(self) -> str:
        return self._cfg("CONTAINER")

    @cached_property
    def _container(self):
        from azure.storage.blob import BlobServiceClient

        account = self._cfg("ACCOUNT_NAME")
        service = BlobServiceClient(
            account_url=f"https://{account}.blob.core.windows.net",
            credential=self._cfg("ACCOUNT_KEY"),
        )
        return service.get_container_client(self._cfg("CONTAINER"))

    def store_status(self, root: str) -> None:
        self._container.get_container_properties()

    def store_exists(self, dest: str) -> bool:
        p = normalise_prefix(dest)
        return (
            next(iter(self._container.list_blobs(name_starts_with=p)), None) is not None
        )

    def store_children(self, root: str) -> list[str]:
        p = normalise_prefix(root)
        names: list[str] = []
        for item in self._container.walk_blobs(name_starts_with=p, delimiter="/"):
            if item.name.endswith("/"):  # a BlobPrefix (child "folder")
                child = item.name[len(p) :].rstrip("/")
                if child:
                    names.append(child)
        return sorted(names)

    def store_clear(self, dest: str) -> None:
        p = normalise_prefix(dest)
        names = [b.name for b in self._container.list_blobs(name_starts_with=p)]
        for i in range(0, len(names), 256):
            self._container.delete_blobs(*names[i : i + 256])

    def store_upload(self, dest: str, local_dir: str) -> int:
        p = normalise_prefix(dest)
        n = 0
        for src, rel in walk_files(local_dir):
            with open(src, "rb") as f:
                self._container.upload_blob(name=p + rel, data=f, overwrite=True)
            n += 1
        return n

"""Azure Blob raw storage ops (azure-storage-blob). Only the four ops the
`System` contract needs: status / exists / clear / upload.

(The `normalise_prefix` / `walk_paths` helpers are imported from the current
`cloud/_backend.py`; in the full reorg they'd move to a `systems/_shared.py`.)"""

from __future__ import annotations

import sys

from .._backend import normalise_prefix, walk_paths


class AzureblobBackend:
    """Built from a plain `{OPTION: value}` dict of resolved connection values
    — no argparse / descriptor coupling, so create-time and run-time callers
    construct it the same way."""

    def __init__(self, values: dict[str, str]) -> None:
        from azure.storage.blob import BlobServiceClient

        self.account = values.get("ACCOUNT_NAME") or ""
        self.key = values.get("ACCOUNT_KEY") or ""
        self.container_name = values.get("CONTAINER") or ""
        url = f"https://{self.account}.blob.core.windows.net"
        self.service = BlobServiceClient(account_url=url, credential=self.key)
        self.container = self.service.get_container_client(self.container_name)

    def status(self):
        self.container.get_container_properties()

    def exists(self, prefix: str) -> bool:
        p = normalise_prefix(prefix)
        for _ in self.container.list_blobs(name_starts_with=p, results_per_page=1):
            return True
        return False

    def clear(self, prefix: str) -> None:
        p = normalise_prefix(prefix)
        names = [b.name for b in self.container.list_blobs(name_starts_with=p)]
        if not names:
            return
        for i in range(0, len(names), 256):
            self.container.delete_blobs(*names[i : i + 256])

    def upload(self, prefix: str, local_paths: list[str]) -> int:
        p = normalise_prefix(prefix)
        n = 0
        for path, rel in walk_paths(local_paths):
            with open(path, "rb") as f:
                self.container.upload_blob(name=p + rel, data=f, overwrite=True)
            print(f"    {rel}", file=sys.stderr)
            n += 1
        return n

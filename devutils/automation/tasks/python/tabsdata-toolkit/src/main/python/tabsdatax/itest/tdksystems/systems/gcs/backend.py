"""GCS raw storage ops (google-cloud-storage). Only the four ops the `System`
contract needs: status / exists / clear / upload.

(The `normalise_prefix` / `walk_paths` helpers are imported from the current
`cloud/_backend.py`; in the full reorg they'd move to a `systems/_shared.py`.)"""
from __future__ import annotations

import sys

from .._backend import normalise_prefix, walk_paths


def _is_inline_json(value: str) -> bool:
    """The service-account key may be a file path or the key JSON itself
    (e.g. resolved from a 1Password field). Inline JSON starts with '{'."""
    return value.lstrip().startswith("{")


class GcsBackend:
    """Built from a plain `{OPTION: value}` dict of resolved connection values
    — no argparse / descriptor coupling, so create-time and run-time callers
    construct it the same way."""

    def __init__(self, values: dict[str, str]) -> None:
        import json

        from google.cloud import storage
        self.bucket_name = values.get("BUCKET") or ""
        sa_key = values.get("SERVICE_ACCOUNT_KEY") or ""
        if _is_inline_json(sa_key):
            self.client = storage.Client.from_service_account_info(json.loads(sa_key))
        else:
            self.client = storage.Client.from_service_account_json(sa_key)
        self.bucket = self.client.bucket(self.bucket_name)

    def status(self):
        # Reachability via an object-level probe (storage.objects.list), not
        # bucket.reload() (storage.buckets.get): datagen only does object
        # ops, and least-privilege service accounts often lack buckets.get.
        next(iter(self.client.list_blobs(self.bucket_name, max_results=1)), None)

    def exists(self, prefix: str) -> bool:
        p = normalise_prefix(prefix)
        it = self.client.list_blobs(self.bucket_name, prefix=p, max_results=1)
        return any(True for _ in it)

    def clear(self, prefix: str) -> None:
        p = normalise_prefix(prefix)
        blobs = list(self.client.list_blobs(self.bucket_name, prefix=p))
        if not blobs:
            return
        with self.client.batch():
            for b in blobs:
                b.delete()

    def upload(self, prefix: str, local_paths: list[str]) -> int:
        p = normalise_prefix(prefix)
        n = 0
        for path, rel in walk_paths(local_paths):
            blob = self.bucket.blob(p + rel)
            blob.upload_from_filename(str(path))
            print(f"    {rel}", file=sys.stderr)
            n += 1
        return n

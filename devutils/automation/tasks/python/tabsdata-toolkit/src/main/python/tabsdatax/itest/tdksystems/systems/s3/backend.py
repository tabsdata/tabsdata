"""S3 raw storage ops (boto3). Only the four ops the `System` contract needs:
status / exists / clear / upload.

(`normalise_prefix` / `walk_paths` are shared object-store helpers in
`systems/_backend.py`.)"""
from __future__ import annotations

import sys

from .._backend import normalise_prefix, walk_paths


class S3Backend:
    """Built from a plain `{OPTION: value}` dict of resolved connection values
    — no argparse / descriptor coupling, so create-time and run-time callers
    construct it the same way."""

    def __init__(self, values: dict[str, str]) -> None:
        import boto3
        self.bucket = values.get("BUCKET", "")
        self.client = boto3.client(
            "s3",
            region_name=values.get("REGION") or None,
            aws_access_key_id=values.get("ACCESS_KEY") or None,
            aws_secret_access_key=values.get("SECRET_KEY") or None,
        )

    def status(self) -> None:
        self.client.head_bucket(Bucket=self.bucket)

    def exists(self, prefix: str) -> bool:
        p = normalise_prefix(prefix)
        resp = self.client.list_objects_v2(Bucket=self.bucket, Prefix=p, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0

    def clear(self, prefix: str) -> None:
        p = normalise_prefix(prefix)
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=p):
            objs = [{"Key": o["Key"]} for o in page.get("Contents", []) or []]
            for i in range(0, len(objs), 1000):
                self.client.delete_objects(
                    Bucket=self.bucket, Delete={"Objects": objs[i:i + 1000]})

    def upload(self, prefix: str, local_paths: list[str]) -> int:
        p = normalise_prefix(prefix)
        n = 0
        for path, rel in walk_paths(local_paths):
            self.client.upload_file(str(path), self.bucket, p + rel)
            print(f"    {rel}", file=sys.stderr)
            n += 1
        return n

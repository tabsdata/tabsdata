"""End-to-end ``S3ConnectionExplorer`` tests against a real S3 bucket.

The explorer talks to S3 directly via ``boto3`` (no transporter binary),
so these are gated only on bucket + credentials. Skipped unless ALL of
these are set:

  - ``S30__S3_URI``        -- ``s3://bucket[/path/prefix]``
  - ``S30__S3_REGION``     -- bucket region (e.g. ``us-east-1``)
  - ``S30__S3_ACCESS_KEY`` -- access key
  - ``S30__S3_SECRET_KEY`` -- secret key

Each run seeds a handful of objects under a unique
``<uri_path>/td-explorer-integ/<uuid>`` prefix (used as the conn's
``base_path``), so ``metadata`` sees exactly those objects and nothing
else already living in the bucket. The seeded objects are removed on
teardown.
"""

from __future__ import annotations

import os
import uuid
from pathlib import PurePosixPath
from urllib.parse import urlparse

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import AwsAccessSecretKey
from tabsdatak.conn.s3 import S3SrcConn

# noinspection PyProtectedMember
import tabsdatak.conn.s3._explorer as s3_explorer
from tabsdatak.conn.s3._explorer import S3ConnectionExplorer

pytestmark = pytest.mark.s3


_URI = os.environ.get("S30__S3_URI")
_REGION = os.environ.get("S30__S3_REGION")
_ACCESS = os.environ.get("S30__S3_ACCESS_KEY")
_SECRET = os.environ.get("S30__S3_SECRET_KEY")

_skip_reason = (
    "S3 explorer integration tests require S30__S3_URI + S30__S3_REGION + "
    "S30__S3_ACCESS_KEY + S30__S3_SECRET_KEY"
)

pytestmark = [
    pytestmark,
    pytest.mark.skipif(not all([_URI, _REGION, _ACCESS, _SECRET]), reason=_skip_reason),
]


# Relative key (under base_path) -> expected FilePattern.format. Mixed
# depths exercise the prefix split; mixed extensions the format sniff.
_SEED_FILES = {
    "users.csv": "csv",
    "sub/orders.parquet": "parquet",
    "report.json": "json",
}


def _join(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if p and p.strip("/"))


def _creds() -> AwsAccessSecretKey:
    return AwsAccessSecretKey(
        access_key_id=Secret(_ACCESS),
        secret_access_key=Secret(_SECRET),
    )


def _conn(base_path: str) -> S3SrcConn:
    bucket = urlparse(_URI).netloc
    return S3SrcConn(
        bucket=bucket,
        region=_REGION,
        credentials=_creds(),
        base_path=base_path,
    )


@pytest.fixture
def seeded_conn():
    """Provision a unique prefix seeded with ``_SEED_FILES`` and tear it
    down afterwards. Yields the ``S3SrcConn`` pointed at that prefix."""
    uri_path = urlparse(_URI).path.lstrip("/")
    base_path = _join(uri_path, "td-explorer-integ", uuid.uuid4().hex)
    conn = _conn(base_path)
    bucket = conn.bucket

    client = s3_explorer._s3_client(conn)
    for rel in _SEED_FILES:
        client.put_object(Bucket=bucket, Key=f"{base_path}/{rel}", Body=b"col\n1\n")
    try:
        yield conn
    finally:
        keys = [
            {"Key": obj["Key"]}
            for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=base_path + "/"
            )
            for obj in page.get("Contents", [])
        ]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys})


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


def test_check_connect_succeeds():
    # Contract: returns None on success and must not raise.
    assert S3ConnectionExplorer().check_connect(_conn("")) is None


def test_check_connect_nonexistent_bucket_raises():
    bad = S3SrcConn(
        bucket=f"td-does-not-exist-{uuid.uuid4().hex}",
        region=_REGION,
        credentials=_creds(),
    )
    with pytest.raises(Exception):
        S3ConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


def test_metadata_lists_only_seeded_objects(seeded_conn):
    entities = S3ConnectionExplorer().metadata(seeded_conn)

    # Isolation: base_path is unique, so exactly the seeded files show up.
    assert {e.file.pattern for e in entities} == {
        PurePosixPath(rel).name for rel in _SEED_FILES
    }
    assert all(e.scheme == "s3" for e in entities)
    assert all(e.bucket == seeded_conn.bucket for e in entities)


def test_metadata_sniffs_format_and_builds_fqn(seeded_conn):
    entities = S3ConnectionExplorer().metadata(seeded_conn)
    by_name = {e.file.pattern: e for e in entities}

    for rel, fmt in _SEED_FILES.items():
        name = PurePosixPath(rel).name
        entity = by_name[name]
        assert entity.file.format == fmt
        # prefix carries the object's full key directory (base_path
        # included), so the fqn is the fully-qualified s3 path.
        assert (
            entity.fqn() == f"s3://{seeded_conn.bucket}/{seeded_conn.base_path}/{rel}"
        )


# ---------------------------------------------------------------------------
# data -- object storage has no row-level preview
# ---------------------------------------------------------------------------


def test_data_returns_none(seeded_conn):
    explorer = S3ConnectionExplorer()
    entity = explorer.metadata(seeded_conn)[0]
    assert explorer.data(seeded_conn, entity) is None

"""End-to-end ``GCSConnectionExplorer`` tests against a real GCS bucket.

The explorer talks to GCS directly via ``google-cloud-storage`` (no
transporter binary), so these are gated only on bucket + credentials.
Skipped unless ALL of these are set:

  - ``GCP0__GCP_STORAGE_URI``         -- ``gs://bucket[/path/prefix]``
  - ``GCP0__GCP_SERVICE_ACCOUNT_KEY`` -- JSON-encoded service-account
                                         credentials (the whole blob)

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
from tabsdatak.conn.common.types import GCPJson
from tabsdatak.conn.gcs import GCSSrcConn

# noinspection PyProtectedMember
import tabsdatak.conn.gcs._explorer as gcs_explorer
from tabsdatak.conn.gcs._explorer import GCSConnectionExplorer

pytestmark = pytest.mark.gcs


_URI = os.environ.get("GCP0__GCP_STORAGE_URI")
_SA_JSON = os.environ.get("GCP0__GCP_SERVICE_ACCOUNT_KEY")

_skip_reason = (
    "GCS explorer integration tests require GCP0__GCP_STORAGE_URI + "
    "GCP0__GCP_SERVICE_ACCOUNT_KEY"
)

pytestmark = [
    pytestmark,
    pytest.mark.skipif(not all([_URI, _SA_JSON]), reason=_skip_reason),
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


def _conn(base_path: str) -> GCSSrcConn:
    bucket = urlparse(_URI).netloc
    return GCSSrcConn(
        bucket=bucket,
        credentials=GCPJson(json=Secret(_SA_JSON)),
        base_path=base_path,
    )


@pytest.fixture
def seeded_conn():
    """Provision a unique prefix seeded with ``_SEED_FILES`` and tear it
    down afterwards. Yields the ``GCSSrcConn`` pointed at that prefix."""
    uri_path = urlparse(_URI).path.lstrip("/")
    base_path = _join(uri_path, "td-explorer-integ", uuid.uuid4().hex)
    conn = _conn(base_path)
    bucket = conn.bucket

    client = gcs_explorer._storage_client(conn)
    bucket_obj = client.bucket(bucket)
    for rel in _SEED_FILES:
        bucket_obj.blob(f"{base_path}/{rel}").upload_from_string(b"col\n1\n")
    try:
        yield conn
    finally:
        for blob in client.list_blobs(bucket, prefix=base_path + "/"):
            blob.delete()


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


def test_check_connect_succeeds():
    # Contract: returns None on success and must not raise.
    assert GCSConnectionExplorer().check_connect(_conn("")) is None


def test_check_connect_nonexistent_bucket_raises():
    bad = GCSSrcConn(
        bucket=f"td-does-not-exist-{uuid.uuid4().hex}",
        credentials=GCPJson(json=Secret(_SA_JSON)),
    )
    with pytest.raises(Exception):
        GCSConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


def test_metadata_lists_only_seeded_objects(seeded_conn):
    entities = GCSConnectionExplorer().metadata(seeded_conn)

    # Isolation: base_path is unique, so exactly the seeded files show up.
    assert {e.file.pattern for e in entities} == {
        PurePosixPath(rel).name for rel in _SEED_FILES
    }
    assert all(e.scheme == "gs" for e in entities)
    assert all(e.bucket == seeded_conn.bucket for e in entities)


def test_metadata_sniffs_format_and_builds_fqn(seeded_conn):
    entities = GCSConnectionExplorer().metadata(seeded_conn)
    by_name = {e.file.pattern: e for e in entities}

    for rel, fmt in _SEED_FILES.items():
        name = PurePosixPath(rel).name
        entity = by_name[name]
        assert entity.file.format == fmt
        # prefix carries the object's full key directory (base_path
        # included), so the fqn is the fully-qualified gs path.
        assert (
            entity.fqn() == f"gs://{seeded_conn.bucket}/{seeded_conn.base_path}/{rel}"
        )


# ---------------------------------------------------------------------------
# data -- object storage has no row-level preview
# ---------------------------------------------------------------------------


def test_data_returns_none(seeded_conn):
    explorer = GCSConnectionExplorer()
    entity = explorer.metadata(seeded_conn)[0]
    assert explorer.data(seeded_conn, entity) is None

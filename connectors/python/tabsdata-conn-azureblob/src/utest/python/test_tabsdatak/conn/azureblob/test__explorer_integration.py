"""End-to-end ``AzureBlobConnectionExplorer`` tests against a real Azure
Blob Storage container.

The explorer talks to Azure directly via ``azure-storage-blob`` (no
transporter binary), so these are gated only on container + credentials.
Skipped unless ALL of these are set:

  - ``AZ0__AZ_URI``          -- ``az://container[/path/prefix]``
  - ``AZ0__AZ_ACCOUNT_NAME`` -- storage account name
  - ``AZ0__AZ_ACCOUNT_KEY``  -- storage account key

Each run seeds a handful of blobs under a unique
``<uri_path>/td-explorer-integ/<uuid>`` prefix (used as the conn's
``base_path``), so ``metadata`` sees exactly those blobs and nothing
else already living in the container. The seeded blobs are removed on
teardown.
"""

from __future__ import annotations

import os
import uuid
from pathlib import PurePosixPath
from urllib.parse import urlparse

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import AzureAccountKey
from tabsdatak.conn.azureblob import AzureBlobSrcConn

# noinspection PyProtectedMember
import tabsdatak.conn.azureblob._explorer as az_explorer
from tabsdatak.conn.azureblob._explorer import AzureBlobConnectionExplorer

pytestmark = pytest.mark.azureblob


_URI = os.environ.get("AZ0__AZ_URI")
_ACCT_NAME = os.environ.get("AZ0__AZ_ACCOUNT_NAME")
_ACCT_KEY = os.environ.get("AZ0__AZ_ACCOUNT_KEY")

_skip_reason = (
    "Azure Blob explorer integration tests require AZ0__AZ_URI + "
    "AZ0__AZ_ACCOUNT_NAME + AZ0__AZ_ACCOUNT_KEY"
)

pytestmark = [
    pytestmark,
    pytest.mark.skipif(not all([_URI, _ACCT_NAME, _ACCT_KEY]), reason=_skip_reason),
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


def _creds() -> AzureAccountKey:
    return AzureAccountKey(
        account_name=Secret(_ACCT_NAME),
        account_key=Secret(_ACCT_KEY),
    )


def _conn(base_path: str) -> AzureBlobSrcConn:
    container = urlparse(_URI).netloc
    return AzureBlobSrcConn(
        container=container,
        credentials=_creds(),
        base_path=base_path,
    )


@pytest.fixture
def seeded_conn():
    """Provision a unique prefix seeded with ``_SEED_FILES`` and tear it
    down afterwards. Yields the ``AzureBlobSrcConn`` at that prefix."""
    uri_path = urlparse(_URI).path.lstrip("/")
    base_path = _join(uri_path, "td-explorer-integ", uuid.uuid4().hex)
    conn = _conn(base_path)

    client = az_explorer._container_client(conn)
    for rel in _SEED_FILES:
        client.upload_blob(name=f"{base_path}/{rel}", data=b"col\n1\n", overwrite=True)
    try:
        yield conn
    finally:
        for blob in client.list_blobs(name_starts_with=base_path + "/"):
            client.delete_blob(blob.name)


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


def test_check_connect_succeeds():
    # Contract: returns None on success and must not raise.
    assert AzureBlobConnectionExplorer().check_connect(_conn("")) is None


def test_check_connect_nonexistent_container_raises():
    bad = AzureBlobSrcConn(
        container=f"td-does-not-exist-{uuid.uuid4().hex}",
        credentials=_creds(),
    )
    with pytest.raises(Exception):
        AzureBlobConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


def test_metadata_lists_only_seeded_blobs(seeded_conn):
    entities = AzureBlobConnectionExplorer().metadata(seeded_conn)

    # Isolation: base_path is unique, so exactly the seeded files show up.
    assert {e.file.pattern for e in entities} == {
        PurePosixPath(rel).name for rel in _SEED_FILES
    }
    assert all(e.scheme == "az" for e in entities)
    assert all(e.bucket == seeded_conn.container for e in entities)


def test_metadata_sniffs_format_and_builds_fqn(seeded_conn):
    entities = AzureBlobConnectionExplorer().metadata(seeded_conn)
    by_name = {e.file.pattern: e for e in entities}

    for rel, fmt in _SEED_FILES.items():
        name = PurePosixPath(rel).name
        entity = by_name[name]
        assert entity.file.format == fmt
        # prefix carries the blob's full key directory (base_path
        # included), so the fqn is the fully-qualified az path.
        assert (
            entity.fqn()
            == f"az://{seeded_conn.container}/{seeded_conn.base_path}/{rel}"
        )


# ---------------------------------------------------------------------------
# data -- object storage has no row-level preview
# ---------------------------------------------------------------------------


def test_data_returns_none(seeded_conn):
    explorer = AzureBlobConnectionExplorer()
    entity = explorer.metadata(seeded_conn)[0]
    assert explorer.data(seeded_conn, entity) is None

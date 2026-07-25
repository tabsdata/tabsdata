#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end ``AzureBlobConnectionExplorer`` tests against a real Azure
Blob Storage container.

The ``azureblob_world_xs`` domain (provisioned by tdx) holds the world dataset
laid out as ``<base_path>/<format>/<table>.<format>`` -- three tables
(``country`` / ``city`` / ``country_language``) in three formats
(``csv`` / ``ndjson`` / ``parquet``). The explorer talks to Azure directly via
``azure-storage-blob``: ``check_connect`` proves auth + container existence,
``metadata`` lists the blobs under ``base_path`` (with size + last-modified),
and ``data`` previews the first rows of a blob (csv via streamed lines, ndjson
via newline-delimited JSON, parquet via pyarrow range requests).
"""

from __future__ import annotations

import re
import uuid

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.azureblob import AzureBlobSrcConn
from tabsdatak.conn.common.file._explorer import FileStorageEntity
from tabsdatak.conn.common.types import AzureAccountKey
from tabsdatak.conn.azureblob._explorer import AzureBlobConnectionExplorer

from tabsdatax.itest.fixture.backends.azureblob import (
    AzureblobDomain,
    AzureblobSystem,
    azureblob_domain,
    azureblob_system,
)

pytestmark = pytest.mark.azureblob

_TABLES = ("country", "city", "country_language")
_FORMATS = ("csv", "ndjson", "parquet")

# The first five city rows (in id order). The staged files include the `id`
# column (native int for ndjson / parquet; string for csv, like every other
# value). ndjson / parquet preserve native types; csv comes back with every
# value as a string.
_WLD_CITY_HEAD = [
    {
        "id": 1,
        "name": "Kabul",
        "country_code": "AFG",
        "district": "Kabol",
        "population": 1780000,
    },
    {
        "id": 2,
        "name": "Qandahar",
        "country_code": "AFG",
        "district": "Qandahar",
        "population": 237500,
    },
    {
        "id": 3,
        "name": "Herat",
        "country_code": "AFG",
        "district": "Herat",
        "population": 186800,
    },
    {
        "id": 4,
        "name": "Mazar-e-Sharif",
        "country_code": "AFG",
        "district": "Balkh",
        "population": 127800,
    },
    {
        "id": 5,
        "name": "Amsterdam",
        "country_code": "NLD",
        "district": "Noord-Holland",
        "population": 731200,
    },
]


def _creds(cfg: AzureblobDomain) -> AzureAccountKey:
    return AzureAccountKey(
        account_name=Secret(cfg.account_name),
        account_key=Secret(cfg.account_key),
    )


def _conn_from(cfg: AzureblobDomain) -> AzureBlobSrcConn:
    return AzureBlobSrcConn(
        container=cfg.container,
        credentials=_creds(cfg),
        base_path="/" + str(cfg.base_path).strip("/"),
    )


_TS_RE = re.compile(r"_\d{8}T\d{6}_\d+(?=\.[^.]+$)")


def _logical(pattern: str) -> str:
    """Staged object name with tdx's ``_<timestamp>`` suffix stripped, so a
    timestamped ``city_20260721T190011_390.csv`` reads back as ``city.csv``."""
    return _TS_RE.sub("", pattern)


def _entity_by_file(
    entities: list[FileStorageEntity], filename: str
) -> FileStorageEntity:
    matches = [
        e
        for e in entities
        if e.file is not None and _logical(e.file.pattern) == filename
    ]
    assert matches, (
        f"no entity for {filename!r} in "
        f"{[_logical(e.file.pattern) for e in entities if e.file]}"
    )
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@azureblob_system("azureblob")
@azureblob_domain("azureblob_world_xs")
def test_check_connect_succeeds(
    system_azureblob: AzureblobSystem, domain_azureblob_world_xs: AzureblobDomain
):
    # Contract: returns None on success and must not raise.
    explorer = AzureBlobConnectionExplorer()
    assert explorer.check_connect(_conn_from(domain_azureblob_world_xs)) is None


@azureblob_system("azureblob")
@azureblob_domain("azureblob_world_xs")
def test_check_connect_nonexistent_container_raises(
    system_azureblob: AzureblobSystem, domain_azureblob_world_xs: AzureblobDomain
):
    bad = AzureBlobSrcConn(
        container=f"td-does-not-exist-{uuid.uuid4().hex}",
        credentials=_creds(domain_azureblob_world_xs),
    )
    with pytest.raises(Exception):
        AzureBlobConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@azureblob_system("azureblob")
@azureblob_domain("azureblob_world_xs")
def test_metadata_lists_domain_objects(
    system_azureblob: AzureblobSystem, domain_azureblob_world_xs: AzureblobDomain
):
    entities = AzureBlobConnectionExplorer().metadata(
        _conn_from(domain_azureblob_world_xs)
    )

    assert entities and all(isinstance(e, FileStorageEntity) for e in entities)
    assert all(e.scheme == "az" for e in entities)
    assert all(e.bucket == domain_azureblob_world_xs.container for e in entities)

    # Every object reports its size (bytes) and last-modified timestamp.
    assert all(isinstance(e.size, int) and e.size > 0 for e in entities)
    assert all(e.last_modified for e in entities)

    # Exactly the world dataset: three tables in three formats.
    assert {_logical(e.file.pattern) for e in entities} == {
        f"{table}.{fmt}" for table in _TABLES for fmt in _FORMATS
    }


@azureblob_system("azureblob")
@azureblob_domain("azureblob_world_xs")
def test_metadata_sniffs_format(
    system_azureblob: AzureblobSystem, domain_azureblob_world_xs: AzureblobDomain
):
    entities = AzureBlobConnectionExplorer().metadata(
        _conn_from(domain_azureblob_world_xs)
    )
    by_name = {_logical(e.file.pattern): e for e in entities}
    for fmt in _FORMATS:
        assert by_name[f"city.{fmt}"].file.format == fmt


# ---------------------------------------------------------------------------
# data -- first-rows preview per format
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fmt", _FORMATS)
@azureblob_system("azureblob")
@azureblob_domain("azureblob_world_xs")
def test_data_returns_first_rows(
    system_azureblob: AzureblobSystem,
    domain_azureblob_world_xs: AzureblobDomain,
    fmt: str,
):
    conn = _conn_from(domain_azureblob_world_xs)
    explorer = AzureBlobConnectionExplorer()
    entity = _entity_by_file(explorer.metadata(conn), f"city.{fmt}")

    rows = explorer.data(conn, entity, limit=5)

    assert isinstance(rows, list)
    # csv comes back as strings; ndjson / parquet keep native types.
    if fmt == "csv":
        expected = [{k: str(v) for k, v in r.items()} for r in _WLD_CITY_HEAD]
    else:
        expected = _WLD_CITY_HEAD
    assert rows == expected

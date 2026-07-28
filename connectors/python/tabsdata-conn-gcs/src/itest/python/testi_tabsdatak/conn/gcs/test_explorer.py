#
# Copyright 2026 Tabsdata Inc.
#

"""GCS explorer integration tests against a provisioned bucket prefix"""

from __future__ import annotations

import re
import uuid

import pytest
from tabsdatax.itest.fixture.backends.gcs import (
    GcsDomain,
    gcs_domain,
)
from testi_tabsdatak.conn.gcs.backend import credentials, src_conn

from tabsdatak.conn.common.file._explorer import FileStorageEntity
from tabsdatak.conn.gcs import GCSSrcConn
from tabsdatak.conn.gcs._explorer import GCSConnectionExplorer

pytestmark = pytest.mark.gcs

TABLES = ("country", "city", "country_language")
FORMATS = ("csv", "ndjson", "parquet")

# The first five city rows (in id order). The staged objects include the `id`
# column (native int for ndjson / parquet; string for csv, like every other
# value). ndjson / parquet preserve native types; csv comes back with every
# value as a string.
CITY_HEAD = [
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


@gcs_domain("gcs_world_xs")
def test_check_connect_succeeds(domain_gcs_world_xs: GcsDomain):
    # Contract: returns None on success and must not raise.
    domain = domain_gcs_world_xs
    conn = src_conn(domain, domain.base_path)
    assert GCSConnectionExplorer().check_connect(conn) is None


@gcs_domain("gcs_world_xs")
def test_check_connect_nonexistent_bucket_raises(domain_gcs_world_xs: GcsDomain):
    bad = GCSSrcConn(
        bucket=f"td-does-not-exist-{uuid.uuid4().hex}",
        credentials=credentials(domain_gcs_world_xs),
    )
    with pytest.raises(Exception):
        GCSConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@gcs_domain("gcs_world_xs")
def test_metadata_lists_domain_objects(domain_gcs_world_xs: GcsDomain):
    domain = domain_gcs_world_xs
    entities = GCSConnectionExplorer().metadata(src_conn(domain, domain.base_path))

    assert entities and all(isinstance(e, FileStorageEntity) for e in entities)
    assert all(e.scheme == "gs" for e in entities)
    assert all(e.bucket == domain.bucket for e in entities)

    # Every object reports its size (bytes) and last-modified timestamp.
    assert all(isinstance(e.size, int) and e.size > 0 for e in entities)
    assert all(e.last_modified for e in entities)

    # Exactly the world dataset: three tables in three formats.
    assert {_logical(e.file.pattern) for e in entities} == {
        f"{table}.{fmt}" for table in TABLES for fmt in FORMATS
    }


@gcs_domain("gcs_world_xs")
def test_metadata_sniffs_format(domain_gcs_world_xs: GcsDomain):
    domain = domain_gcs_world_xs
    entities = GCSConnectionExplorer().metadata(src_conn(domain, domain.base_path))
    by_name = {_logical(e.file.pattern): e for e in entities}
    for fmt in FORMATS:
        assert by_name[f"city.{fmt}"].file.format == fmt


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fmt", FORMATS)
@gcs_domain("gcs_world_xs")
def test_data_returns_first_rows(domain_gcs_world_xs: GcsDomain, fmt: str):
    domain = domain_gcs_world_xs
    conn = src_conn(domain, domain.base_path)
    explorer = GCSConnectionExplorer()
    entity = _entity_by_file(explorer.metadata(conn), f"city.{fmt}")

    rows = explorer.data(conn, entity, limit=5)

    assert isinstance(rows, list)
    # csv comes back as strings; ndjson / parquet keep native types.
    if fmt == "csv":
        expected = [{k: str(v) for k, v in r.items()} for r in CITY_HEAD]
    else:
        expected = CITY_HEAD
    assert rows == expected

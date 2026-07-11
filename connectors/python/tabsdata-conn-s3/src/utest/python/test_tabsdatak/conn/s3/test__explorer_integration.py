"""End-to-end ``S3ConnectionExplorer`` tests against a real S3 bucket.

The ``s3_world_xs`` domain (provisioned by tdx) holds the world dataset laid
out as ``<base_path>/<format>/<table>.<format>`` -- three tables
(``wld_country`` / ``wld_city`` / ``wld_country_language``) in three formats
(``csv`` / ``ndjson`` / ``parquet``). The explorer talks to S3 directly via
``boto3`` (no transporter binary): ``check_connect`` proves auth + bucket
existence, ``metadata`` lists the objects under ``base_path``, and ``data``
previews the first rows of a blob (csv via streamed lines, ndjson via
newline-delimited JSON, parquet via pyarrow range requests).
"""

from __future__ import annotations

import uuid

import pytest

from tabsdatak.api import Secret
from tabsdatak.conn.common.file._explorer import FileStorageEntity
from tabsdatak.conn.common.types import AwsAccessSecretKey
from tabsdatak.conn.s3 import S3SrcConn
from tabsdatak.conn.s3._explorer import S3ConnectionExplorer

from tabsdatax.itest.fixture.backends.s3 import (
    S3Domain,
    S3System,
    s3_domain,
    s3_system,
)

pytestmark = pytest.mark.s3

_TABLES = ("wld_country", "wld_city", "wld_country_language")
_FORMATS = ("csv", "ndjson", "parquet")

# The first five wld_city rows (in id order, as stored). ndjson / parquet
# preserve native types; csv comes back with every value as a string.
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


def _creds(cfg: S3Domain) -> AwsAccessSecretKey:
    return AwsAccessSecretKey(
        access_key_id=Secret(cfg.access_key),
        secret_access_key=Secret(cfg.secret_key),
    )


def _conn_from(cfg: S3Domain) -> S3SrcConn:
    return S3SrcConn(
        bucket=cfg.bucket,
        region=cfg.region,
        credentials=_creds(cfg),
        base_path=str(cfg.base_path),
    )


def _entity_by_file(
    entities: list[FileStorageEntity], filename: str
) -> FileStorageEntity:
    matches = [e for e in entities if e.file is not None and e.file.pattern == filename]
    assert matches, (
        f"no entity for {filename!r} in {[e.file.pattern for e in entities if e.file]}"
    )
    return matches[0]


# ---------------------------------------------------------------------------
# check_connect
# ---------------------------------------------------------------------------


@s3_system("s3")
@s3_domain("s3_world_xs")
def test_check_connect_succeeds(system_s3: S3System, domain_s3_world_xs: S3Domain):
    # Contract: returns None on success and must not raise.
    assert S3ConnectionExplorer().check_connect(_conn_from(domain_s3_world_xs)) is None


@s3_system("s3")
@s3_domain("s3_world_xs")
def test_check_connect_nonexistent_bucket_raises(
    system_s3: S3System, domain_s3_world_xs: S3Domain
):
    bad = S3SrcConn(
        bucket=f"td-does-not-exist-{uuid.uuid4().hex}",
        region=domain_s3_world_xs.region,
        credentials=_creds(domain_s3_world_xs),
    )
    with pytest.raises(Exception):
        S3ConnectionExplorer().check_connect(bad)


# ---------------------------------------------------------------------------
# metadata
# ---------------------------------------------------------------------------


@s3_system("s3")
@s3_domain("s3_world_xs")
def test_metadata_lists_domain_objects(
    system_s3: S3System, domain_s3_world_xs: S3Domain
):
    entities = S3ConnectionExplorer().metadata(_conn_from(domain_s3_world_xs))

    assert entities and all(isinstance(e, FileStorageEntity) for e in entities)
    assert all(e.scheme == "s3" for e in entities)
    assert all(e.bucket == domain_s3_world_xs.bucket for e in entities)

    # Every object reports its size (bytes) and last-modified timestamp.
    assert all(isinstance(e.size, int) and e.size > 0 for e in entities)
    assert all(e.last_modified for e in entities)

    # Exactly the world dataset: three tables in three formats.
    assert {e.file.pattern for e in entities} == {
        f"{table}.{fmt}" for table in _TABLES for fmt in _FORMATS
    }


@s3_system("s3")
@s3_domain("s3_world_xs")
def test_metadata_sniffs_format(system_s3: S3System, domain_s3_world_xs: S3Domain):
    entities = S3ConnectionExplorer().metadata(_conn_from(domain_s3_world_xs))
    by_name = {e.file.pattern: e for e in entities}
    for fmt in _FORMATS:
        assert by_name[f"wld_city.{fmt}"].file.format == fmt


# ---------------------------------------------------------------------------
# data -- first-rows preview per format
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("fmt", _FORMATS)
@s3_system("s3")
@s3_domain("s3_world_xs")
def test_data_returns_first_rows(
    system_s3: S3System, domain_s3_world_xs: S3Domain, fmt: str
):
    conn = _conn_from(domain_s3_world_xs)
    explorer = S3ConnectionExplorer()
    entity = _entity_by_file(explorer.metadata(conn), f"wld_city.{fmt}")

    rows = explorer.data(conn, entity, limit=5)

    assert isinstance(rows, list)
    # csv comes back as strings; ndjson / parquet keep native types.
    if fmt == "csv":
        expected = [{k: str(v) for k, v in r.items()} for r in _WLD_CITY_HEAD]
    else:
        expected = _WLD_CITY_HEAD
    assert rows == expected

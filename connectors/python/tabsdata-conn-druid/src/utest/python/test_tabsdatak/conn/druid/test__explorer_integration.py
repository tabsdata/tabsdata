#
#  Copyright 2026. Tabs Data Inc.
#

"""End-to-end ``DruidConnectionExplorer`` tests against a real Druid
cluster provisioned through the tdx systems / domains machinery.

The ``druid_world_xs`` domain seeds the ``world`` model: the tdx Druid
system stages the generated parquet to S3 and ingests one datasource per
table (``<datasource>__wld_country`` / ``__wld_city`` /
``__wld_country_language``), so the catalog surface of
``DruidConnectionExplorer`` -- ``check_connect`` / ``metadata`` / ``data``
-- is exercised against real datasources with real rows.

Note: the explorer queries Druid's SQL endpoint, which the **Router**
serves at ``/druid/v2/sql``. ``DruidDestConn`` only carries
``overlord_url``, and ``make_url`` builds the SQL URL from the host and
port it finds there -- so the conn is fed the domain's ``router_url`` to
reach the SQL service. The cloud staging area is required to construct the
conn but the explorer never touches it, so a placeholder is used.
"""

from __future__ import annotations

import pytest
from tabsdatax.itest.fixture.backends.druid import (
    DruidDomain,
    druid_domain,
)

from tabsdatak.api import Secret
from tabsdatak.conn.common.sql._explorer import SQLEntity
from tabsdatak.conn.common.types import AwsAccessSecretKey, UserPassword
from tabsdatak.conn.druid import DruidDestConn, S3Location
from tabsdatak.conn.druid.explorer import DruidConnectionExplorer

pytestmark = pytest.mark.druid

# The explorer only issues catalog SQL over the Broker/Router endpoint; the
# staging area is never used, so a placeholder is enough to construct the conn.
_STAGING = S3Location(
    bucket="unused",
    region="us-east-1",
    credentials=AwsAccessSecretKey(
        access_key_id=Secret("unused"), secret_access_key=Secret("unused")
    ),
)

# The world model's tables — each seeded as its own datasource. A subset of each
# table's columns is asserted so the check is robust to Druid's synthetic time
# columns (__time / __td_dummy_time) that ingestion adds.
_WORLD_COLUMNS = {
    "wld_country": {"code", "name", "continent", "population"},
    "wld_city": {"id", "name", "country_code", "population"},
    "wld_country_language": {"country_code", "language", "percentage"},
}


def _conn_from(cfg: DruidDomain) -> DruidDestConn:
    return DruidDestConn(
        # SQL is served by the Router; make_url honors the port carried here.
        overlord_url=cfg.router_url,
        staging=_STAGING,
        credentials=UserPassword(user=Secret(cfg.user), password=Secret(cfg.password)),
    )


def _table_of(datasource: str) -> str:
    """The world table a seeded datasource carries (its ``__<table>`` suffix)."""
    _, _, table = datasource.rpartition("__")
    return table


@druid_domain("druid_world_xs")
def test_check_connect_succeeds(domain_druid_world_xs: DruidDomain):
    conn = _conn_from(domain_druid_world_xs)
    assert DruidConnectionExplorer().check_connect(conn) is None


@druid_domain("druid_world_xs")
def test_metadata_lists_world_datasources(domain_druid_world_xs: DruidDomain):
    entities = DruidConnectionExplorer().metadata(_conn_from(domain_druid_world_xs))
    assert entities
    assert all(isinstance(e, SQLEntity) for e in entities)
    assert all(e.schema == "druid" for e in entities)

    tables = {_table_of(e.table) for e in entities}
    assert _WORLD_COLUMNS.keys() <= tables

    by_table = {_table_of(e.table): e for e in entities}
    for table, expected in _WORLD_COLUMNS.items():
        found = {c.name for c in by_table[table].columns}
        assert expected <= found, f"{table} missing columns: {expected - found}"


@druid_domain("druid_world_xs")
def test_data_reads_world_rows(domain_druid_world_xs: DruidDomain):
    conn = _conn_from(domain_druid_world_xs)
    explorer = DruidConnectionExplorer()
    entities = explorer.metadata(conn)
    by_table = {_table_of(e.table): e for e in entities}

    country = by_table["wld_country"]
    rows = explorer.data(conn, country, limit=5)
    assert rows
    assert len(rows) <= 5
    field_names = {c.name for c in country.columns}
    assert all(set(r) <= field_names for r in rows)
    assert all(_WORLD_COLUMNS["wld_country"] <= set(r) for r in rows)

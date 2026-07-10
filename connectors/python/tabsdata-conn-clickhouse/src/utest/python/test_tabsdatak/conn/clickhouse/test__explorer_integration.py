#
#  Copyright 2026. Tabs Data Inc.
#

"""End-to-end ``ClickHouseConnectionExplorer`` tests against a real
ClickHouse instance.

ClickHouse has no fixed ``users`` / ``orders`` seed, so the
``metadata`` / ``data`` tests use a module-local ``seeded_clickhouse``
fixture that creates a small scratch table (and drops it afterwards).
That fixture is intentionally NOT baked into the shared
``fresh_clickhouse`` fixture -- it's scoped to this module so the other
``fresh_clickhouse`` consumers (e.g. the plugin integration tests) see an
unchanged, empty database. The conn (and its required cloud staging) is
built by the shared ``_conn_from`` helper; the suite skips when
ClickHouse / staging aren't configured.
"""

from __future__ import annotations

import pytest

from tabsdatak.conn.clickhouse._explorer import ClickHouseConnectionExplorer
from tabsdatak.conn.common.sql._explorer import SQLEntity

# noinspection PyProtectedMember
from test_tabsdatak.conn.clickhouse._integration_helpers import (
    _ch_client,
    _conn_from,
    _db_name,
    _drop_table,
)

# Share the serialization domain of the other ``fresh_clickhouse`` consumers.
pytestmark = [
    pytest.mark.clickhouse,
    pytest.mark.xdist_group("clickhouse-plugin_integration_seq"),
]

# Scratch table seeded for the metadata/data tests (dropped afterwards).
_PROBE_TABLE = "itest_explorer_probe"


def _entity_by_table(entities: list[SQLEntity], table: str) -> SQLEntity:
    matches = [e for e in entities if e.table == table]
    assert matches, f"no entity for {table!r} in {[e.table for e in entities]}"
    return matches[0]


@pytest.fixture
def seeded_clickhouse(fresh_clickhouse):
    """Create a small seeded table in the configured ClickHouse database
    so the explorer's ``metadata`` / ``data`` have something to read, and
    drop it once the test finishes.

    Kept local to this module (rather than modifying the shared
    ``fresh_clickhouse`` fixture) so other consumers of that fixture are
    unaffected. Yields ``(fresh_clickhouse, table_name)``.
    """
    db = _db_name(fresh_clickhouse)
    client = _ch_client(fresh_clickhouse)
    try:
        # Drop any leftover from a previously crashed run, then seed.
        _drop_table(client, db, _PROBE_TABLE)
        client.command(
            f"CREATE TABLE `{db}`.`{_PROBE_TABLE}` "
            "(id Int32, name String, age Int32) ENGINE = MergeTree ORDER BY id"
        )
        client.command(
            f"INSERT INTO `{db}`.`{_PROBE_TABLE}` (id, name, age) VALUES "
            "(1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 40)"
        )
        yield fresh_clickhouse, _PROBE_TABLE
    finally:
        _drop_table(client, db, _PROBE_TABLE)
        client.close()


def test_check_connect_succeeds(fresh_clickhouse):
    conn = _conn_from(fresh_clickhouse)
    assert ClickHouseConnectionExplorer().check_connect(conn) is None


def test_metadata_lists_seeded_table(seeded_clickhouse):
    cfg, table = seeded_clickhouse
    conn = _conn_from(cfg)
    entities = ClickHouseConnectionExplorer().metadata(conn)

    assert all(isinstance(e, SQLEntity) for e in entities)
    # The scan is pinned to the configured database; ClickHouse has no
    # schema level, so each entity reports the database in `database`
    # with a blank `schema`.
    assert all(e.schema == "" for e in entities)

    # The seeded table shows up with its reflected columns.
    probe = _entity_by_table(entities, table)
    assert {c.name for c in probe.columns} == {"id", "name", "age"}


def test_data_reads_seeded_rows(seeded_clickhouse):
    cfg, table = seeded_clickhouse
    conn = _conn_from(cfg)
    explorer = ClickHouseConnectionExplorer()
    probe = _entity_by_table(explorer.metadata(conn), table)

    rows = explorer.data(conn, probe, limit=5)

    assert isinstance(rows, list)
    assert len(rows) == 3
    assert all(set(r) == {"id", "name", "age"} for r in rows)
    assert {r["name"] for r in rows} == {"alice", "bob", "carol"}

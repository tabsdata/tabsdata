#
# Copyright 2026 Tabsdata Inc.
#

"""End-to-end ClickHouse plugin tests against a real ClickHouse
instance.

Unlike the SQL connectors (MySQL / Postgres / ...), ClickHouse ingests
data through a cloud-staged bulk loader: parquet files are first
uploaded to S3 / Azure Blob / GCS, then ``INSERT INTO ... SELECT *
FROM <table_fn>`` loads them. A real ClickHouse server is therefore
not enough -- a real cloud-storage location with valid credentials
is also required.

The session-scoped ``clickhouse_config`` fixture (in ``conftest.py``)
spins up a ClickHouse docker container (or attaches to an external
server via ``CLICKHOUSE0__CLICKHOUSE_*`` env vars) and pairs it with
a staging location read from ``CLICKHOUSE0__STAGING_*`` env vars.
When no staging is configured every test below is skipped with a
clear message.

This file covers the bread-and-butter destination scenarios --
create / append / replace -- across a representative sample of
FQN spellings (see the ``_FQN_FORMS_BY_PROVIDER`` block below).
Iceberg schema-evolution coverage lives in
``test__plugin_iceberg_integration.py`` (S3 only, since the
evolution logic is dialect-side and provider-agnostic).

Error-path coverage (fault-injection via mocks) lives in
``test__plugin.py``.
"""

from __future__ import annotations

import os

import pytest

from tabsdatak.conn.clickhouse import ClickHouseDest
# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse import _plugin as plugin
# noinspection PyProtectedMember
from test_tabsdatak.conn.clickhouse._integration_helpers import (
    _ch_client,
    _conn_from,
    _count_rows,
    _dest_ctx,
    _make_parquet,
)


pytestmark = [pytest.mark.clickhouse, pytest.mark.xdist_group("clickhouse-plugin_integration_seq")]


# ---------------------------------------------------------------------------
# FQN coverage -- table name spellings the dialect must normalize
# ---------------------------------------------------------------------------
#
# The dialect parses every user-supplied table name through
# ``CLICKHOUSE_FQN_DEF`` (separator='.', quote='`', escape='`') and
# fills in the connection's database when the leading part is missing.
# These integration tests exercise the same physical table through
# the plugin using each of the accepted spellings, verifying that
# every operation (create / append / replace) resolves to the same
# target regardless of how the user wrote the name.
#
# FQN parsing / escaping happens inside ``ClickHouseDialect`` --
# before any cloud-storage code runs -- so the same five spellings
# behave identically against S3, Azure, and GCS. Re-running all
# five spellings against each provider would only verify "cloud
# I/O still works", which the per-provider scenario suite already
# covers.
#
# We instead distribute the five spellings across the three
# providers (3 + 1 + 1) so the FQN matrix is exhaustively
# exercised exactly once across the combined suite, and each
# provider still pays for the round-trip cost of just a handful
# of cases.
#
#   S3    -> bare-table, db.table, quoted-db.quoted-table   (3)
#   Azure -> quoted-db.table                                (1)
#   GCS   -> db.quoted-table                                (1)
#                                            -- union: all 5 forms.


_PROVIDER = (
    os.environ.get("CLICKHOUSE0__STAGING_PROVIDER", "s3")
    .strip()
    .lower()
)
_FQN_FORMS_BY_PROVIDER: dict[str, list] = {
    "s3": [
        pytest.param("{tbl}", id="bare-table"),
        pytest.param("{db}.{tbl}", id="db.table"),
        pytest.param("`{db}`.`{tbl}`", id="quoted-db.quoted-table"),
    ],
    "azure": [
        pytest.param("`{db}`.{tbl}", id="quoted-db.table"),
    ],
    "gcs": [
        pytest.param("{db}.`{tbl}`", id="db.quoted-table"),
    ],
}
_FQN_FORMS = _FQN_FORMS_BY_PROVIDER.get(
    _PROVIDER, _FQN_FORMS_BY_PROVIDER["s3"]
)


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("template", _FQN_FORMS)
def test_fqn_create_table(
    fresh_clickhouse, work_dir, template, fqn_table,
):
    """First write to a new table -- exercises ``create_table`` +
    ``bulk_load`` for every accepted spelling of the table name."""
    db, tbl = fqn_table
    spelled = template.format(db=db, tbl=tbl)
    src = _make_parquet(
        work_dir / "in.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )
    dest = ClickHouseDest(
        tables=[spelled],
    )
    plugin._ClickHouseDestPlugin().write_out(
        _dest_ctx(work_dir), _conn_from(fresh_clickhouse), dest, [src],
    )
    client = _ch_client(fresh_clickhouse)
    assert _count_rows(client, db, tbl) == 2


@pytest.mark.parametrize("template", _FQN_FORMS)
def test_fqn_append_to_existing_table(
    fresh_clickhouse, work_dir, template, fqn_table,
):
    """Second write to an existing table -- exercises
    ``table_exists`` + ``verify_schema`` + ``bulk_load``."""
    db, tbl = fqn_table
    # Seed: create the table by writing one row with the bare name.
    seed_src = _make_parquet(
        work_dir / "seed.parquet",
        [{"id": 1, "name": "alice"}],
    )
    seed_dest = ClickHouseDest(
        tables=[tbl],
    )
    plugin._ClickHouseDestPlugin().write_out(
        _dest_ctx(work_dir), _conn_from(fresh_clickhouse), seed_dest, [seed_src],
    )
    # Append: address the SAME table via the parametrized spelling.
    spelled = template.format(db=db, tbl=tbl)
    add_src = _make_parquet(
        work_dir / "add.parquet",
        [{"id": 2, "name": "bob"}, {"id": 3, "name": "carol"}],
    )
    dest = ClickHouseDest(
        tables=[spelled],
    )
    plugin._ClickHouseDestPlugin().write_out(
        _dest_ctx(work_dir), _conn_from(fresh_clickhouse), dest, [add_src],
    )
    client = _ch_client(fresh_clickhouse)
    assert _count_rows(client, db, tbl) == 3


@pytest.mark.parametrize("template", _FQN_FORMS)
def test_fqn_replace_recreates_existing(
    fresh_clickhouse, work_dir, template, fqn_table,
):
    """``if_table_exists='replace'`` exercises ``drop_table`` + recreate +
    ``bulk_load``."""
    db, tbl = fqn_table
    seed_src = _make_parquet(
        work_dir / "seed.parquet",
        [{"id": 100, "name": "old"}, {"id": 101, "name": "also_old"}],
    )
    seed_dest = ClickHouseDest(
        tables=[tbl],
    )
    plugin._ClickHouseDestPlugin().write_out(
        _dest_ctx(work_dir), _conn_from(fresh_clickhouse), seed_dest, [seed_src],
    )
    # Replace: address the SAME table via the parametrized spelling.
    spelled = template.format(db=db, tbl=tbl)
    new_src = _make_parquet(
        work_dir / "new.parquet",
        [{"id": 1, "name": "only"}],
    )
    dest = ClickHouseDest(
        tables=[spelled],
        if_table_exists="replace",
    )
    plugin._ClickHouseDestPlugin().write_out(
        _dest_ctx(work_dir), _conn_from(fresh_clickhouse), dest, [new_src],
    )
    client = _ch_client(fresh_clickhouse)
    assert _count_rows(client, db, tbl) == 1
    result = client.query(
        f"SELECT name FROM `{db}`.`{tbl}` WHERE id = 1"
    )
    assert result.result_rows == [("only",)]

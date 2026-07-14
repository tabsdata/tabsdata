#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake connector integration tests: drive ``SnowflakeDestPlugin.write_out``
directly against a tdx-provisioned ``snowflake`` domain (a real Snowflake account)
with a fabricated plugin context -- unlike ``test__plugin_e2e.py``, which goes
through a running tabsdata instance. Snowflake is destination-only (no
``SnowflakeSrc``); it stages internally (PUT + COPY INTO) and creates the target
table from the parquet schema, so tests need no cloud staging and no pre-created
table. Writes land in uniquely-named scratch tables (dropped on teardown) and are
verified with a ``snowflake.connector`` client, never through the connector.

Snowflake folds unquoted identifiers to upper case, so scratch tables use
upper-case names and read-back columns are normalized to lower case before
comparison. Requires the ``snowflake`` domain provisioned with account/PAT creds.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.snowflake import SnowflakeDomain, snowflake_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from tabsdatax.itest.fixture.functions.harness import assert_frames_match
from tabsdatax.itest.fixture.functions.sql import scratch_table, write_parquet

from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import (
    SnowflakeDest,
    SnowflakeDestConn,
    _plugin as plugin,
)

pytestmark = pytest.mark.snowflake


def _dest_conn(domain: SnowflakeDomain) -> SnowflakeDestConn:
    # The tdx schema is a case-sensitive lower-case identifier; quote it so the
    # session's USE SCHEMA preserves case (unquoted folds to upper case and the
    # schema is not found -> "session does not have a current schema").
    return SnowflakeDestConn(
        account=domain.account,
        credentials=UserPassword(
            user=Secret(domain.username), password=Secret(domain.pat)
        ),
        database=domain.database,
        schema=f'"{domain.schema}"',
        warehouse=domain.warehouse,
    )


def _lower(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename({c: c.lower() for c in df.columns})


@snowflake_domain("snowflake")
def test_dest_then_read_round_trip(
    domain_snowflake: SnowflakeDomain, snowflake, tmp_path: Path
):
    db = snowflake(domain_snowflake)
    table = scratch_table().upper()
    work = tmp_path / "work"
    work.mkdir()
    parquet = write_parquet(
        tmp_path / "in.parquet",
        [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}],
    )
    try:
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            _dest_conn(domain_snowflake),
            SnowflakeDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        assert_frames_match(
            _lower(db.read_table(table)),
            pl.DataFrame([{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_append_accumulates(
    domain_snowflake: SnowflakeDomain, snowflake, tmp_path: Path
):
    db = snowflake(domain_snowflake)
    table = scratch_table().upper()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(tmp_path / "a.parquet", [{"id": 1, "name": "alice"}])
    second = write_parquet(tmp_path / "b.parquet", [{"id": 2, "name": "bob"}])
    dest_conn = _dest_conn(domain_snowflake)
    try:
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            SnowflakeDest(tables=[table], if_table_exists="replace"),
            [first],
        )
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            SnowflakeDest(tables=[table], if_table_exists="append"),
            [second],
        )
        assert db.count(table) == 2
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_replace_truncates_existing(
    domain_snowflake: SnowflakeDomain, snowflake, tmp_path: Path
):
    db = snowflake(domain_snowflake)
    table = scratch_table().upper()
    work = tmp_path / "work"
    work.mkdir()
    first = write_parquet(
        tmp_path / "a.parquet", [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    )
    second = write_parquet(tmp_path / "b.parquet", [{"id": 3, "name": "carol"}])
    dest_conn = _dest_conn(domain_snowflake)
    try:
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            SnowflakeDest(tables=[table], if_table_exists="replace"),
            [first],
        )
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            dest_conn,
            SnowflakeDest(tables=[table], if_table_exists="replace"),
            [second],
        )
        got = _lower(db.read_table(table))
        assert got["name"].to_list() == ["carol"]
    finally:
        db.drop_table(table)

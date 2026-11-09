#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.snowflake import SnowflakeDomain, snowflake_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.common.sql.schema.policy import (
    APPEND_CASES,
    ID,
    MERGE,
    MERGE_CASES,
    OP,
    Policy,
    run,
    table_for,
)
from testi_tabsdatak.conn.snowflake.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import MergeCfg, SnowflakeDest, _plugin as plugin

pytestmark = pytest.mark.snowflake


def _policy(domain: SnowflakeDomain, work: Path) -> Policy:
    db = store(domain)
    conn = dest_conn(domain)

    def write(table: str, frame: pl.LazyFrame, mode: str, evolution: str) -> None:
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(
                tables=[table],
                if_table_exists=mode,
                merge_cfg=[MergeCfg(on=[ID], op_column=OP)] if mode == MERGE else None,
                schema_evolution=evolution,
                dest_cfg={"snowflake.schema_evolution_engine": "iceberg"}
                if mode == MERGE and evolution == "update"
                else None,
            ),
            [frame],
        )

    return Policy(write=write, read=db.read_table, drop=db.drop_table)


@pytest.mark.parametrize("case", APPEND_CASES)
@snowflake_domain("snowflake")
def test_schema_policy_append(
    domain_snowflake: SnowflakeDomain, tmp_path: Path, case, request
):
    run(
        case,
        _policy(domain_snowflake, work_dir(tmp_path)),
        table_for(request).upper(),
    )


@pytest.mark.parametrize("case", MERGE_CASES)
@snowflake_domain("snowflake")
def test_schema_policy_merge(
    domain_snowflake: SnowflakeDomain, tmp_path: Path, case, request
):
    run(
        case,
        _policy(domain_snowflake, work_dir(tmp_path)),
        table_for(request).upper(),
    )

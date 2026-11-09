#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.databricks import (
    DatabricksDomain,
    databricks_domain,
)
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.common.sql.schema.policy import (
    APPEND_CASES,
    ID,
    MERGE,
    MERGE_CASES,
    OP,
    REPLACE_CASES,
    Policy,
    run,
    table_for,
)
from testi_tabsdatak.conn.databricks.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.databricks import DatabricksDest, MergeCfg, _plugin as plugin

pytestmark = pytest.mark.databricks


def _policy(domain: DatabricksDomain, work: Path) -> Policy:
    db = store(domain)
    conn = dest_conn(domain)

    def write(table: str, frame: pl.LazyFrame, mode: str, evolution: str) -> None:
        plugin._DatabricksDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            DatabricksDest(
                tables=[table],
                if_table_exists=mode,
                merge_cfg=[MergeCfg(on=[ID], op_column=OP)] if mode == MERGE else None,
                schema_evolution=evolution,
            ),
            [frame],
        )

    return Policy(write=write, read=db.read_table, drop=db.drop_table)


@pytest.mark.parametrize("case", APPEND_CASES + REPLACE_CASES + MERGE_CASES)
@databricks_domain("databricks")
def test_schema_policy(
    domain_databricks: DatabricksDomain, tmp_path: Path, case, request
):
    run(case, _policy(domain_databricks, work_dir(tmp_path)), table_for(request))

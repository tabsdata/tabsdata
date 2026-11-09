#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.bigquery import BigQueryDomain, bigquery_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.bigquery.backend import dest_conn, store, table_fqn
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.common.sql.schema.policy import (
    APPEND_CASES,
    REPLACE_CASES,
    ID,
    MERGE,
    MERGE_CASES,
    OP,
    Policy,
    run,
    table_for,
)

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import BigQueryDest, MergeCfg, _plugin as plugin

pytestmark = pytest.mark.bigquery


def _policy(domain: BigQueryDomain, work: Path) -> Policy:
    db = store(domain)
    conn = dest_conn(domain)

    def write(table: str, frame: pl.LazyFrame, mode: str, evolution: str) -> None:
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(
                tables=[table_fqn(table)],
                if_table_exists=mode,
                merge_cfg=[MergeCfg(on=[ID], op_column=OP)] if mode == MERGE else None,
                schema_evolution=evolution,
                dest_cfg={"bigquery.schema_evolution_engine": "iceberg"}
                if mode == MERGE and evolution == "update"
                else None,
            ),
            [frame],
        )

    return Policy(write=write, read=db.read_table, drop=db.drop_table)


@pytest.mark.parametrize("case", APPEND_CASES + REPLACE_CASES)
@bigquery_domain("bigquery")
def test_schema_policy_bulk(
    domain_bigquery: BigQueryDomain, tmp_path: Path, case, request
):
    run(case, _policy(domain_bigquery, work_dir(tmp_path)), table_for(request))


@pytest.mark.parametrize("case", MERGE_CASES)
@bigquery_domain("bigquery")
def test_schema_policy_merge(
    domain_bigquery: BigQueryDomain, tmp_path: Path, case, request
):
    run(
        case,
        _policy(domain_bigquery, work_dir(tmp_path)),
        table_for(request),
    )

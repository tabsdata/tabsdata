#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.oracle import OracleDomain, oracle_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.common.sql.schema.policy import (
    APPEND_CASES,
    REPLACE_CASES,
    Policy,
    run,
    table_for,
)
from testi_tabsdatak.conn.common.sql.schema.widening import WIDENING_CASES
from testi_tabsdatak.conn.oracle.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.oracle import OracleDest, _plugin as plugin

pytestmark = pytest.mark.oracle


def _policy(domain: OracleDomain, work: Path) -> Policy:
    db = store(domain)
    conn = dest_conn(domain)

    def write(table: str, frame: pl.LazyFrame, mode: str, evolution: str) -> None:
        plugin.OracleDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            OracleDest(
                tables=[table], if_table_exists=mode, schema_evolution=evolution
            ),
            [frame],
        )

    return Policy(write=write, read=db.read_table, drop=db.drop_table)


@pytest.mark.parametrize("case", APPEND_CASES + REPLACE_CASES)
@oracle_domain("oracle")
def test_schema_policy(domain_oracle: OracleDomain, tmp_path: Path, case, request):
    run(case, _policy(domain_oracle, work_dir(tmp_path)), table_for(request))


@pytest.mark.parametrize("case", WIDENING_CASES)
@oracle_domain("oracle")
def test_schema_widening(domain_oracle: OracleDomain, tmp_path: Path, case, request):
    policy = _policy(domain_oracle, work_dir(tmp_path))
    try:
        case(policy, table_for(request), store(domain_oracle)._engine)
    finally:
        policy.drop(table_for(request))

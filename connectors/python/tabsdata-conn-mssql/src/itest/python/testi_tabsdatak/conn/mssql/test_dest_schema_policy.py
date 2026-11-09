#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.mssqlserver import (
    MssqlserverDomain,
    mssqlserver_domain,
)
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
from testi_tabsdatak.conn.mssql.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.mssql import MSSQLDest, _plugin as plugin

pytestmark = pytest.mark.mssql


def _policy(domain: MssqlserverDomain, work: Path) -> Policy:
    db = store(domain)
    conn = dest_conn(domain)

    def write(table: str, frame: pl.LazyFrame, mode: str, evolution: str) -> None:
        plugin.MSSQLDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            MSSQLDest(tables=[table], if_table_exists=mode, schema_evolution=evolution),
            [frame],
        )

    return Policy(write=write, read=db.read_table, drop=db.drop_table)


@pytest.mark.parametrize("case", APPEND_CASES + REPLACE_CASES)
@mssqlserver_domain("mssqlserver")
def test_schema_policy(
    domain_mssqlserver: MssqlserverDomain, tmp_path: Path, case, request
):
    run(case, _policy(domain_mssqlserver, work_dir(tmp_path)), table_for(request))


@pytest.mark.parametrize("case", WIDENING_CASES)
@mssqlserver_domain("mssqlserver")
def test_schema_widening(
    domain_mssqlserver: MssqlserverDomain, tmp_path: Path, case, request
):
    policy = _policy(domain_mssqlserver, work_dir(tmp_path))
    try:
        case(policy, table_for(request), store(domain_mssqlserver)._engine)
    finally:
        policy.drop(table_for(request))

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.db2 import Db2Domain, db2_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.common.sql.schema.policy import (
    APPEND_CASES,
    Policy,
    run,
    table_for,
)
from testi_tabsdatak.conn.common.sql.schema.widening import WIDENING_CASES
from testi_tabsdatak.conn.db2.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.db2 import Db2Dest, _plugin as plugin

pytestmark = pytest.mark.db2


def _policy(domain: Db2Domain, work: Path) -> Policy:
    db = store(domain)
    conn = dest_conn(domain)

    def write(table: str, frame: pl.LazyFrame, mode: str, evolution: str) -> None:
        plugin.Db2DestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            Db2Dest(tables=[table], if_table_exists=mode, schema_evolution=evolution),
            [frame],
        )

    return Policy(write=write, read=db.read_table, drop=db.drop_table)


@pytest.mark.parametrize("case", APPEND_CASES)
@db2_domain("db2")
def test_schema_policy(domain_db2: Db2Domain, tmp_path: Path, case, request):
    run(case, _policy(domain_db2, work_dir(tmp_path)), table_for(request))


@pytest.mark.parametrize("case", WIDENING_CASES)
@db2_domain("db2")
def test_schema_widening(domain_db2: Db2Domain, tmp_path: Path, case, request):
    policy = _policy(domain_db2, work_dir(tmp_path))
    try:
        case(policy, table_for(request), store(domain_db2)._engine)
    finally:
        policy.drop(table_for(request))

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.clickhouse import (
    ClickhouseDomain,
    clickhouse_domain,
)
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.clickhouse.backend import dest_conn, store
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.common.sql.schema.policy import (
    APPEND_CASES,
    REPLACE_CASES,
    Policy,
    run,
    table_for,
)

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse import ClickHouseDest, _plugin as plugin

pytestmark = pytest.mark.clickhouse


def _policy(domain: ClickhouseDomain, staging: S3Domain, work: Path) -> Policy:
    db = store(domain)
    conn = dest_conn(domain, staging)

    def write(table: str, frame: pl.LazyFrame, mode: str, evolution: str) -> None:
        plugin._ClickHouseDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            ClickHouseDest(
                tables=[table],
                if_table_exists=mode,
                schema_evolution=evolution,
            ),
            [frame],
        )

    return Policy(write=write, read=db.read_table, drop=db.drop_table)


@pytest.mark.parametrize("case", APPEND_CASES + REPLACE_CASES)
@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_schema_policy(
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
    case,
    request,
):
    policy = _policy(domain_clickhouse, domain_s3, work_dir(tmp_path))
    run(case, policy, table_for(request))

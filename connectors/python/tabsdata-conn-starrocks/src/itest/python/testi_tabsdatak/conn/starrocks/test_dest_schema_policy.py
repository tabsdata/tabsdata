#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.backends.starrocks import StarrocksDomain, starrocks_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.common.sql.schema.policy import (
    APPEND_CASES,
    REPLACE,
    UPDATE,
    Policy,
    run,
    table_for,
)
from testi_tabsdatak.conn.starrocks.backend import column_spec, dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import StarRocksDest, _plugin as plugin

pytestmark = pytest.mark.starrocks

_EVOLUTION = {"strict": "strict", UPDATE: "iceberg"}


def _policy(domain: StarrocksDomain, staging: S3Domain, work: Path) -> Policy:
    db = store(domain)
    conn = dest_conn(domain, staging)

    def write(table: str, frame: pl.LazyFrame, mode: str, evolution: str) -> None:
        if mode == REPLACE:
            db.create_table(table, column_spec(frame.collect_schema()))
        plugin.StarRocksDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            StarRocksDest(
                tables=[table],
                if_table_exists=mode,
                schema_evolution=_EVOLUTION[evolution],
            ),
            [frame],
        )

    return Policy(write=write, read=db.read_table, drop=db.drop_table)


@pytest.mark.parametrize("case", APPEND_CASES)
@starrocks_domain("starrocks")
@s3_domain("s3")
def test_schema_policy(
    domain_starrocks: StarrocksDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
    case,
    request,
):
    policy = _policy(domain_starrocks, domain_s3, work_dir(tmp_path))
    run(case, policy, table_for(request))

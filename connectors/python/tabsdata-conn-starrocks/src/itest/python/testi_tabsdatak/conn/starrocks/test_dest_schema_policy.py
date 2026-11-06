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
    APPEND,
    APPEND_CASES,
    REPLACE,
    STRICT,
    Policy,
    run,
    table_for,
)
from testi_tabsdatak.conn.starrocks.backend import column_spec, dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import StarRocksDest, _plugin as plugin

pytestmark = pytest.mark.starrocks


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
                schema_evolution=evolution,
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


_NARROW = {"id": "bigint", "n": "int", "code": "char(3)"}
_BATCH = {"id": pl.Int64, "n": pl.Int64, "code": pl.String}


@pytest.mark.parametrize("n,refused", [(5, False), (2**40, True)])
@starrocks_domain("starrocks")
@s3_domain("s3")
def test_strict_takes_into_narrower_columns_only_what_they_hold(
    domain_starrocks: StarrocksDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
    n: int,
    refused: bool,
):
    """StarRocks refuses text its column cannot hold but loads such a number
    as NULL, so strict reads the batch's range before taking it."""
    db = store(domain_starrocks)
    policy = _policy(domain_starrocks, domain_s3, work_dir(tmp_path))
    table = f"td_policy_strict_narrower_{'refused' if refused else 'taken'}"
    db.create_table(table, _NARROW)
    try:
        batch = pl.LazyFrame({"id": [1], "n": [n], "code": ["ESP"]}, schema=_BATCH)
        if refused:
            with pytest.raises(Exception, match="^SQL_COMMON_10"):
                policy.write(table, batch, APPEND, STRICT)
            assert db.read_table(table).height == 0
        else:
            policy.write(table, batch, APPEND, STRICT)
            assert db.read_table(table).rows() == [(1, 5, "ESP")]
    finally:
        policy.drop(table)

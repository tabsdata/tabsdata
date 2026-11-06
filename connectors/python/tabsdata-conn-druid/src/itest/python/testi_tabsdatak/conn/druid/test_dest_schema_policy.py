#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.druid import DruidDomain, druid_domain
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.common.sql.schema.policy import (
    REPLACE,
    SCHEMALESS_CASES,
    Policy,
    run,
    table_for,
)
from testi_tabsdatak.conn.druid.backend import (
    POLL_SECONDS,
    POLL_TIMEOUT_SECONDS,
    dest_conn,
    store,
)

# noinspection PyProtectedMember
from tabsdatak.conn.druid import DruidDest, _plugin as plugin

pytestmark = pytest.mark.druid


def _policy(domain: DruidDomain, staging: S3Domain, work: Path) -> Policy:
    db = store(domain)
    conn = dest_conn(domain, staging)
    rows: dict[str, int] = {}

    def write(table: str, frame: pl.LazyFrame, mode: str, evolution: str) -> None:
        plugin.DruidDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            DruidDest(tables=[table], if_table_exists=mode),
            [frame],
        )
        height = frame.select(pl.len()).collect().item()
        rows[table] = height if mode == REPLACE else rows.get(table, 0) + height

    def drop(table: str) -> None:
        rows.pop(table, None)
        db.drop_table(table)

    @retry(
        retry=retry_if_exception_type(AssertionError),
        wait=wait_fixed(POLL_SECONDS),
        stop=stop_after_delay(POLL_TIMEOUT_SECONDS),
        reraise=True,
    )
    def read(table: str) -> pl.DataFrame:
        # The Broker's SQL catalog learns a new column on its next metadata
        # refresh; the live segments know it at once.
        frame = db.read_table(table, rows[table])
        assert db.dimensions(table) <= set(frame.columns)
        return frame

    return Policy(write=write, read=read, drop=drop)


@pytest.mark.parametrize("case", SCHEMALESS_CASES)
@druid_domain("druid")
@s3_domain("s3")
def test_schema_policy(
    domain_druid: DruidDomain, domain_s3: S3Domain, tmp_path: Path, case, request
):
    run(case, _policy(domain_druid, domain_s3, work_dir(tmp_path)), table_for(request))

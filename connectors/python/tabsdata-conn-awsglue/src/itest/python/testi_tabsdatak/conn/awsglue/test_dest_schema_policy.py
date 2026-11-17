#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.awsglue import AwsGlueDomain, awsglue_domain
from testi_tabsdatak.conn.awsglue.backend import TrxCtx, dest_conn, store
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.common.sql.schema.policy import (
    APPEND_CASES,
    REPLACE_CASES,
    Policy,
    run,
    table_for,
)

# noinspection PyProtectedMember
from tabsdatak.conn.awsglue import AwsGlueDest, _plugin as plugin

pytestmark = pytest.mark.conn_awsglue


def _policy(domain: AwsGlueDomain, work: Path) -> Policy:
    catalog = store(domain)
    conn = dest_conn(domain)

    def write(table: str, frame: pl.LazyFrame, mode: str, evolution: str) -> None:
        plugin.AwsGlueDestPlugin().write_out(
            TrxCtx(work),
            conn,
            AwsGlueDest(
                tables=[table], if_table_exists=mode, schema_evolution=evolution
            ),
            [frame],
        )

    return Policy(write=write, read=catalog.read_table, drop=catalog.drop_table)


@pytest.mark.parametrize("case", APPEND_CASES + REPLACE_CASES)
@awsglue_domain("awsglue")
def test_schema_policy_bulk(
    domain_awsglue: AwsGlueDomain, tmp_path: Path, case, request
):
    run(case, _policy(domain_awsglue, work_dir(tmp_path)), table_for(request))

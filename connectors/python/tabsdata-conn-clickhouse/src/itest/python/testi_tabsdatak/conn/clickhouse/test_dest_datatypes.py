#
# Copyright 2026 Tabsdata Inc.
#

"""ClickHouse data-type coverage on write, staged through S3 -- the column types
the loader creates do not depend on which staging provider carried the parquet."""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.clickhouse import (
    ClickhouseDomain,
    clickhouse_domain,
)
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from testi_tabsdatak.conn.clickhouse.backend import dest_conn, store
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_named_datatype_table,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse import ClickHouseDest, _plugin as plugin

pytestmark = pytest.mark.clickhouse

# Types the ClickHouse write path can't create/load today (surfaced by the matrix):
# the Arrow -> ClickHouse type map has no entry for them, so the target DDL never
# gets built.
_XFAIL: dict[str, str] = {}


def _params():
    for case in DATATYPE_CASES:
        marks = (
            [pytest.mark.xfail(reason=_XFAIL[case.id], strict=True)]
            if case.id in _XFAIL
            else []
        )
        # One fixed table per case, so a run leaves nothing unpredictable behind.
        table = f"td_dt_{case.id}"
        yield pytest.param(case, table, id=case.id, marks=marks)


@pytest.mark.parametrize("case, table", list(_params()))
@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_datatype_create_and_load(
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    case: DataTypeCase,
    table: str,
    tmp_path: Path,
):
    frame = write_named_datatype_table(
        case,
        table=table,
        dest_plugin=plugin._ClickHouseDestPlugin(),
        make_dest=lambda tables: ClickHouseDest(
            tables=tables, if_table_exists="replace"
        ),
        dest_conn=dest_conn(domain_clickhouse, domain_s3),
        sql=store(domain_clickhouse),
        work_dir=work_dir(tmp_path),
    )
    assert frame.height == len(case.values)

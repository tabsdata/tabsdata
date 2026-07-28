#
# Copyright 2026 Tabsdata Inc.
#

"""Databricks data-type coverage on write."""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.databricks import (
    DatabricksDomain,
    databricks_domain,
)
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_named_datatype_table,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.databricks.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.databricks import DatabricksDest, _plugin as plugin

pytestmark = pytest.mark.databricks

# Types the Databricks write path can't create/load today (surfaced by the matrix).
_XFAIL: dict[str, str] = {
    "time": (
        "DATABRICKS_13: Databricks has no time-of-day type and COPY INTO rejects "
        "the parquet TIME type ([PARQUET_TYPE_ILLEGAL] Illegal Parquet type: INT64 "
        "(TIME(NANOS,false))). The destination now refuses a time column up front "
        "rather than letting the load fail after the upload and CREATE -- cast it "
        "to a timestamp or a string before writing"
    ),
}


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
@databricks_domain("databricks")
def test_datatype_create_and_load(
    domain_databricks: DatabricksDomain,
    case: DataTypeCase,
    table: str,
    tmp_path: Path,
):
    frame = write_named_datatype_table(
        case,
        table=table,
        dest_plugin=plugin._DatabricksDestPlugin(),
        make_dest=lambda tables: DatabricksDest(
            tables=tables, if_table_exists="replace"
        ),
        dest_conn=dest_conn(domain_databricks),
        sql=store(domain_databricks),
        work_dir=work_dir(tmp_path),
    )
    assert frame.height == len(case.values)

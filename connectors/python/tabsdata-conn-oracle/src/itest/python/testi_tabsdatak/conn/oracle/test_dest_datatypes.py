#
# Copyright 2026 Tabsdata Inc.
#

"""Oracle data-type coverage on write."""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.oracle import OracleDomain, oracle_domain
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_named_datatype_table,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.oracle.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.oracle import OracleDest, _plugin as plugin

pytestmark = pytest.mark.oracle

# Types the Oracle write path can't create/load today (surfaced by the matrix).
_XFAIL: dict[str, str] = {
    "float32": "ORACLE_9: float columns fail on the Oracle write path",
    "float64": "ORACLE_9: float columns fail on the Oracle write path",
    "time": "ORACLE_9: Oracle has no TIME type, so the create fails",
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
@oracle_domain("oracle")
def test_datatype_create_and_load(
    domain_oracle: OracleDomain,
    case: DataTypeCase,
    table: str,
    tmp_path: Path,
):
    frame = write_named_datatype_table(
        case,
        table=table,
        dest_plugin=plugin.OracleDestPlugin(),
        make_dest=lambda tables: OracleDest(tables=tables, if_table_exists="replace"),
        dest_conn=dest_conn(domain_oracle),
        sql=store(domain_oracle),
        work_dir=work_dir(tmp_path),
    )
    assert frame.height == len(case.values)

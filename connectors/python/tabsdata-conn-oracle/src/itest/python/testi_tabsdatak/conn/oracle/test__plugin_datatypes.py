#
# Copyright 2026 Tabsdata Inc.
#

"""Oracle data-type coverage: drive ``OracleDestPlugin.write_out`` (create-on-write,
replace) for every TableFrame column type against a tdx-provisioned ``oracle``
domain, via the shared matrix in
:mod:`testi_tabsdatak.conn.common.sql.datatype_matrix`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.oracle import OracleDomain, oracle_domain
from tabsdatax.itest.fixture.functions.sql import sql_credentials

# noinspection PyProtectedMember
from tabsdatak.conn.oracle import OracleDest, OracleDestConn, _plugin as plugin
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_datatype_table,
)

pytestmark = pytest.mark.oracle


def _dest_conn(domain: OracleDomain) -> OracleDestConn:
    return OracleDestConn(uri=domain.uri, credentials=sql_credentials(domain))


# Types the Oracle write path can't create/load today (surfaced by the matrix).
_XFAIL: dict[str, str] = {
    "float32": "ORACLE_9: float columns fail on the Oracle write path",
    "float64": "ORACLE_9: float columns fail on the Oracle write path",
    "time": "Oracle has no TIME type; create fails (ORACLE_9)",
}


def _params():
    for case in DATATYPE_CASES:
        marks = (
            [pytest.mark.xfail(reason=_XFAIL[case.id], strict=True)]
            if case.id in _XFAIL
            else []
        )
        yield pytest.param(case, id=case.id, marks=marks)


@pytest.mark.parametrize("case", list(_params()))
@oracle_domain("oracle")
def test_datatype_create_and_load(
    case: DataTypeCase, domain_oracle: OracleDomain, sql, tmp_path: Path
):
    work = tmp_path / "work"
    work.mkdir()
    frame = write_datatype_table(
        case,
        dest_plugin=plugin.OracleDestPlugin(),
        make_dest=lambda tables: OracleDest(tables=tables, if_table_exists="replace"),
        dest_conn=_dest_conn(domain_oracle),
        sql=sql(domain_oracle),
        work_dir=work,
    )
    assert frame.height == len(case.values)

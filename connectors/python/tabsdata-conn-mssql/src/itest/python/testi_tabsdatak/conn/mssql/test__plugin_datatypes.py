#
# Copyright 2026 Tabsdata Inc.
#

"""SQL Server data-type coverage: drive ``MSSQLDestPlugin.write_out``
(create-on-write, replace) for every TableFrame column type against a
tdx-provisioned ``mssqlserver`` domain, via the shared matrix in
:mod:`testi_tabsdatak.conn.common.sql.datatype_matrix`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.mssqlserver import (
    MssqlserverDomain,
    mssqlserver_domain,
)
from tabsdatax.itest.fixture.functions.sql import sql_credentials
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_datatype_table,
)

# noinspection PyProtectedMember
from tabsdatak.conn.mssql import MSSQLDest, MSSQLDestConn, _plugin as plugin

pytestmark = pytest.mark.mssql


def _dest_conn(domain: MssqlserverDomain) -> MSSQLDestConn:
    return MSSQLDestConn(uri=domain.uri, credentials=sql_credentials(domain))


# Types the SQL Server write path can't create/load today (surfaced by the matrix).
_XFAIL: dict[str, str] = {
    "datetime_tz": "MSSQL_9: tz-aware datetime not supported (needs DATETIMEOFFSET)",
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
@mssqlserver_domain("mssqlserver")
def test_datatype_create_and_load(
    case: DataTypeCase, domain_mssqlserver: MssqlserverDomain, sql, tmp_path: Path
):
    work = tmp_path / "work"
    work.mkdir()
    frame = write_datatype_table(
        case,
        dest_plugin=plugin.MSSQLDestPlugin(),
        make_dest=lambda tables: MSSQLDest(tables=tables, if_table_exists="replace"),
        dest_conn=_dest_conn(domain_mssqlserver),
        sql=sql(domain_mssqlserver),
        work_dir=work,
    )
    assert frame.height == len(case.values)

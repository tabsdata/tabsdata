#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL data-type coverage: drive ``MySQLDestPlugin.write_out`` (create-on-write,
replace) for every TableFrame column type against a tdx-provisioned ``mysql``
domain, via the shared matrix in
:mod:`testi_tabsdatak.conn.common.sql.datatype_matrix`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.mysql import MysqlDomain, mysql_domain
from tabsdatax.itest.fixture.functions.sql import sql_credentials
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_datatype_table,
)

# noinspection PyProtectedMember
from tabsdatak.conn.mysql import MySQLDest, MySQLDestConn, _plugin as plugin

pytestmark = pytest.mark.mysql


def _dest_conn(domain: MysqlDomain) -> MySQLDestConn:
    return MySQLDestConn(uri=domain.uri, credentials=sql_credentials(domain))


@pytest.mark.parametrize("case", DATATYPE_CASES, ids=lambda c: c.id)
@mysql_domain("mysql")
def test_datatype_create_and_load(
    case: DataTypeCase, domain_mysql: MysqlDomain, sql, tmp_path: Path
):
    work = tmp_path / "work"
    work.mkdir()
    frame = write_datatype_table(
        case,
        dest_plugin=plugin.MySQLDestPlugin(),
        make_dest=lambda tables: MySQLDest(tables=tables, if_table_exists="replace"),
        dest_conn=_dest_conn(domain_mysql),
        sql=sql(domain_mysql),
        work_dir=work,
    )
    assert frame.height == len(case.values)

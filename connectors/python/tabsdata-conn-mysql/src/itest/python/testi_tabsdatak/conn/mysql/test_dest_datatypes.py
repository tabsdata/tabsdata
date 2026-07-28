#
# Copyright 2026 Tabsdata Inc.
#

"""MySQL data-type coverage on write."""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.mysql import MysqlDomain, mysql_domain
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_named_datatype_table,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.mysql.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.mysql import MySQLDest, _plugin as plugin

pytestmark = pytest.mark.mysql

# Types the MySQL write path can't create/load today (surfaced by the matrix).
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
@mysql_domain("mysql")
def test_datatype_create_and_load(
    domain_mysql: MysqlDomain,
    case: DataTypeCase,
    table: str,
    tmp_path: Path,
):
    frame = write_named_datatype_table(
        case,
        table=table,
        dest_plugin=plugin.MySQLDestPlugin(),
        make_dest=lambda tables: MySQLDest(tables=tables, if_table_exists="replace"),
        dest_conn=dest_conn(domain_mysql),
        sql=store(domain_mysql),
        work_dir=work_dir(tmp_path),
    )
    assert frame.height == len(case.values)

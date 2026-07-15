#
# Copyright 2026 Tabsdata Inc.
#

"""DB2 data-type coverage: drive ``Db2DestPlugin.write_out`` (create-on-write,
replace mode) for every TableFrame column type against a tdx-provisioned ``db2``
domain, using the shared matrix in
:mod:`testi_tabsdatak.conn.common.sql.datatype_matrix`.

A type DB2 needs special handling for surfaces as a failing case here (mark it
``xfail`` with the tracking ticket, mirroring the existing write-path xfails).
Requires a Db2 docker container and the ``db2`` domain provisioned.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.db2 import Db2Domain, db2_domain
from tabsdatax.itest.fixture.functions.sql import sql_credentials

# noinspection PyProtectedMember
from tabsdatak.conn.db2 import Db2Dest, Db2DestConn, _plugin as plugin
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_datatype_table,
)

pytestmark = pytest.mark.db2


def _dest_conn(domain: Db2Domain) -> Db2DestConn:
    return Db2DestConn(uri=domain.uri, credentials=sql_credentials(domain))


@pytest.mark.parametrize("case", DATATYPE_CASES, ids=lambda c: c.id)
@db2_domain("db2")
def test_datatype_create_and_load(
    case: DataTypeCase, domain_db2: Db2Domain, sql, tmp_path: Path
):
    work = tmp_path / "work"
    work.mkdir()
    frame = write_datatype_table(
        case,
        dest_plugin=plugin.Db2DestPlugin(),
        make_dest=lambda tables: Db2Dest(tables=tables, if_table_exists="replace"),
        dest_conn=_dest_conn(domain_db2),
        sql=sql(domain_db2),
        work_dir=work,
    )
    assert frame.height == len(case.values)

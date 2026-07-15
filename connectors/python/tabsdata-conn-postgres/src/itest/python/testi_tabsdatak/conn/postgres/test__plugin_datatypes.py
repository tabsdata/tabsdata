#
# Copyright 2026 Tabsdata Inc.
#

"""PostgreSQL data-type coverage: drive ``PostgresDestPlugin.write_out``
(create-on-write, replace) for every TableFrame column type against a
tdx-provisioned ``postgres`` domain, via the shared matrix in
:mod:`testi_tabsdatak.conn.common.sql.datatype_matrix`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.postgres import PostgresDomain, postgres_domain
from tabsdatax.itest.fixture.functions.sql import sql_credentials
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_datatype_table,
)

# noinspection PyProtectedMember
from tabsdatak.conn.postgres import (
    PostgresDest,
    PostgresDestConn,
    _plugin as plugin,
)

pytestmark = pytest.mark.postgres


def _dest_conn(domain: PostgresDomain) -> PostgresDestConn:
    return PostgresDestConn(uri=domain.uri, credentials=sql_credentials(domain))


@pytest.mark.parametrize("case", DATATYPE_CASES, ids=lambda c: c.id)
@postgres_domain("postgres")
def test_datatype_create_and_load(
    case: DataTypeCase, domain_postgres: PostgresDomain, sql, tmp_path: Path
):
    work = tmp_path / "work"
    work.mkdir()
    frame = write_datatype_table(
        case,
        dest_plugin=plugin.PostgresDestPlugin(),
        make_dest=lambda tables: PostgresDest(tables=tables, if_table_exists="replace"),
        dest_conn=_dest_conn(domain_postgres),
        sql=sql(domain_postgres),
        work_dir=work,
    )
    assert frame.height == len(case.values)

#
# Copyright 2026 Tabsdata Inc.
#

"""Postgres data-type coverage on write."""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.postgres import PostgresDomain, postgres_domain
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_named_datatype_table,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.postgres.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.postgres import PostgresDest, _plugin as plugin

pytestmark = pytest.mark.postgres

# Types the postgres write path can't create/load today (surfaced by the matrix).
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
@postgres_domain("postgres")
def test_datatype_create_and_load(
    domain_postgres: PostgresDomain,
    case: DataTypeCase,
    table: str,
    tmp_path: Path,
):
    frame = write_named_datatype_table(
        case,
        table=table,
        dest_plugin=plugin.PostgresDestPlugin(),
        make_dest=lambda tables: PostgresDest(tables=tables, if_table_exists="replace"),
        dest_conn=dest_conn(domain_postgres),
        sql=store(domain_postgres),
        work_dir=work_dir(tmp_path),
    )
    assert frame.height == len(case.values)

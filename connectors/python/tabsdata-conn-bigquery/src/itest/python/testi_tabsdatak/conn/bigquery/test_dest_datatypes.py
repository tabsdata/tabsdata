#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery data-type coverage on write."""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.bigquery import BigqueryDomain, bigquery_domain
from testi_tabsdatak.conn.bigquery.backend import dest_conn, store, table_fqn
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_named_datatype_table,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import BigQueryDest, _plugin as plugin

pytestmark = pytest.mark.bigquery

# Types the BigQuery write path can't create/load today (surfaced by the matrix).
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
@bigquery_domain("bigquery")
def test_datatype_create_and_load(
    domain_bigquery: BigqueryDomain,
    case: DataTypeCase,
    table: str,
    tmp_path: Path,
):
    frame = write_named_datatype_table(
        case,
        table=table,
        dest_plugin=plugin._BigQueryDestPlugin(),
        make_dest=lambda tables: BigQueryDest(
            tables=[table_fqn(t) for t in tables], if_table_exists="replace"
        ),
        dest_conn=dest_conn(domain_bigquery),
        sql=store(domain_bigquery),
        work_dir=work_dir(tmp_path),
    )
    assert frame.height == len(case.values)

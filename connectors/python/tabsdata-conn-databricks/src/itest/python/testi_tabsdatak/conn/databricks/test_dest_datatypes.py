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
from tabsdatak.conn.databricks.error import (
    DatabricksErrorCode,
    DatabricksRuntimeException,
)

pytestmark = pytest.mark.databricks

# Types the destination refuses up front, before any upload or CREATE: they
# have no single correct Databricks type (DATABRICKS_13: no time-of-day type).
_REFUSED: dict[str, DatabricksErrorCode] = {
    "time": DatabricksErrorCode.DATABRICKS_13,
}


def _table(case: DataTypeCase) -> str:
    return f"td_dt_{case.id}"


def _write(domain: DatabricksDomain, case: DataTypeCase, tmp_path: Path):
    return write_named_datatype_table(
        case,
        table=_table(case),
        dest_plugin=plugin._DatabricksDestPlugin(),
        make_dest=lambda tables: DatabricksDest(
            tables=tables, if_table_exists="replace"
        ),
        dest_conn=dest_conn(domain),
        sql=store(domain),
        work_dir=work_dir(tmp_path),
    )


def _cases(refused: bool) -> list:
    return [
        pytest.param(case, id=case.id)
        for case in DATATYPE_CASES
        if (case.id in _REFUSED) is refused
    ]


@pytest.mark.parametrize("case", _cases(refused=False))
@databricks_domain("databricks")
def test_datatype_create_and_load(
    domain_databricks: DatabricksDomain,
    case: DataTypeCase,
    tmp_path: Path,
):
    frame = _write(domain_databricks, case, tmp_path)
    assert frame.height == len(case.values)


@pytest.mark.parametrize("case", _cases(refused=True))
@databricks_domain("databricks")
def test_datatype_refused(
    domain_databricks: DatabricksDomain,
    case: DataTypeCase,
    tmp_path: Path,
):
    with pytest.raises(DatabricksRuntimeException) as refusal:
        _write(domain_databricks, case, tmp_path)
    assert refusal.value.api_error is _REFUSED[case.id]
    assert "'c'" in str(refusal.value)
    assert _table(case) in str(refusal.value)

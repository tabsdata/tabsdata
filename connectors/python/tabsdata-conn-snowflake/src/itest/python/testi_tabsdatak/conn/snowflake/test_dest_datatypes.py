#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake data-type coverage on write."""

from __future__ import annotations

from pathlib import Path

import pytest
from tabsdatax.itest.fixture.backends.snowflake import SnowflakeDomain, snowflake_domain
from testi_tabsdatak.conn.common.sql.datatype_matrix import (
    DATATYPE_CASES,
    DataTypeCase,
    write_named_datatype_table,
)
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir
from testi_tabsdatak.conn.snowflake.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import SnowflakeDest, _plugin as plugin
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode, SnowflakeRuntimeException

pytestmark = pytest.mark.snowflake

# Types the destination refuses up front, before the stage and table exist:
# the bulk load cannot cast its variant to them (SNOWFLAKE_8: binary).
_REFUSED: dict[str, SnowflakeErrorCode] = {
    "binary": SnowflakeErrorCode.SNOWFLAKE_8,
}


# Upper case because the connector quotes the name it is given, and
# Snowflake stores unquoted identifiers upper-folded.
def _table(case: DataTypeCase) -> str:
    return f"TD_DT_{case.id.upper()}"


def _write(domain: SnowflakeDomain, case: DataTypeCase, tmp_path: Path):
    return write_named_datatype_table(
        case,
        table=_table(case),
        dest_plugin=plugin.SnowflakeDestPlugin(),
        make_dest=lambda tables: SnowflakeDest(
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
@snowflake_domain("snowflake")
def test_datatype_create_and_load(
    domain_snowflake: SnowflakeDomain,
    case: DataTypeCase,
    tmp_path: Path,
):
    frame = _write(domain_snowflake, case, tmp_path)
    assert frame.height == len(case.values)


@pytest.mark.parametrize("case", _cases(refused=True))
@snowflake_domain("snowflake")
def test_datatype_refused(
    domain_snowflake: SnowflakeDomain,
    case: DataTypeCase,
    tmp_path: Path,
):
    with pytest.raises(SnowflakeRuntimeException) as refusal:
        _write(domain_snowflake, case, tmp_path)
    assert refusal.value.api_error is _REFUSED[case.id]
    assert "'c'" in str(refusal.value)
    assert _table(case) in str(refusal.value)

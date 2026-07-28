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

pytestmark = pytest.mark.snowflake

# Types the Snowflake write path can't create/load today (surfaced by the matrix).
_XFAIL: dict[str, str] = {
    "binary": (
        "SNOWFLAKE_8: the bulk load matches columns by name, which routes every "
        "value through a variant, and a variant cannot be cast to BINARY. The "
        "destination now refuses a binary column up front rather than letting "
        "COPY INTO fail once the stage and table exist -- encode it (base64 into "
        "a string) before writing"
    ),
}


def _params():
    for case in DATATYPE_CASES:
        marks = (
            [pytest.mark.xfail(reason=_XFAIL[case.id], strict=True)]
            if case.id in _XFAIL
            else []
        )
        # One fixed table per case, so a run leaves nothing unpredictable behind.
        # Upper case because the connector quotes the name it is given, and
        # Snowflake stores unquoted identifiers upper-folded.
        table = f"TD_DT_{case.id.upper()}"
        yield pytest.param(case, table, id=case.id, marks=marks)


@pytest.mark.parametrize("case, table", list(_params()))
@snowflake_domain("snowflake")
def test_datatype_create_and_load(
    domain_snowflake: SnowflakeDomain,
    case: DataTypeCase,
    table: str,
    tmp_path: Path,
):
    frame = write_named_datatype_table(
        case,
        table=table,
        dest_plugin=plugin.SnowflakeDestPlugin(),
        make_dest=lambda tables: SnowflakeDest(
            tables=tables, if_table_exists="replace"
        ),
        dest_conn=dest_conn(domain_snowflake),
        sql=store(domain_snowflake),
        work_dir=work_dir(tmp_path),
    )
    assert frame.height == len(case.values)

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.widening import outgrows

pytestmark = pytest.mark.sql_common


@pytest.mark.parametrize(
    "stored,batch,dialect,widens",
    [
        ("INTEGER", "BIGINT", "postgresql", True),
        ("BIGINT", "INTEGER", "postgresql", False),
        ("NUMBER(10, 0)", "NUMBER(19)", "oracle", True),
        ("DECIMAL(10, 2)", "DECIMAL(20, 2)", "mssql", True),
        ("DECIMAL(10, 2)", "DECIMAL(10, 4)", "mssql", False),
        ("REAL", "DOUBLE PRECISION", "postgresql", True),
        ("FLOAT", "DOUBLE", "mysql", True),
        ("FLOAT", "FLOAT(53)", "mssql", False),
        ("BINARY_FLOAT", "BINARY_DOUBLE", "oracle", True),
        ("TINYINT(1)", "BIGINT", "mysql", False),
        ("INTEGER", "DOUBLE PRECISION", "postgresql", False),
        ("VARCHAR(10)", "TEXT", "postgresql", False),
    ],
)
def test_a_number_column_is_widened_only_within_its_family(
    stored, batch, dialect, widens
):
    assert outgrows(stored, batch, dialect) is widens

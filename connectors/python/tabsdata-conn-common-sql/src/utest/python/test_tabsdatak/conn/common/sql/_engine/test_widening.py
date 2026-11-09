#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.widening import holds, outgrows

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
        ("REAL", "FLOAT", "sqlite", False),
        ("REAL", "DOUBLE", "mysql", False),
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


@pytest.mark.parametrize(
    "stored,batch,dialect,value,held",
    [
        ("INTEGER", "BIGINT", "postgresql", 2**31 - 1, True),
        ("INTEGER", "BIGINT", "postgresql", 2**31, False),
        ("INTEGER", "BIGINT", "postgresql", -(2**31), True),
        ("SMALLINT", "BIGINT", "sqlite", -32769, False),
        ("TINYINT", "BIGINT", "mssql", 255, True),
        ("TINYINT", "BIGINT", "mssql", -1, False),
        ("TINYINT", "BIGINT", "mysql", 128, False),
        ("INT UNSIGNED", "BIGINT", "mysql", 2**32 - 1, True),
        ("NUMBER(10, 0)", "NUMBER(19)", "oracle", 9_999_999_999, True),
        ("NUMBER(10, 0)", "NUMBER(19)", "oracle", 10_000_000_000, False),
        ("DECIMAL(10, 2)", "DECIMAL(20, 2)", "mssql", "99999999.99", True),
        ("DECIMAL(10, 2)", "DECIMAL(20, 2)", "mssql", "100000000", False),
    ],
)
def test_a_narrower_exact_column_holds_the_values_within_its_range(
    stored, batch, dialect, value, held
):
    check = holds(stored, batch, dialect)
    assert check is not None
    assert check(value) is held


@pytest.mark.parametrize(
    "stored,batch,dialect",
    [
        ("REAL", "DOUBLE PRECISION", "postgresql"),
        ("DECIMAL(10, 2)", "DECIMAL(20, 4)", "mssql"),
        ("VARCHAR(10)", "TEXT", "postgresql"),
    ],
)
def test_no_value_is_known_to_fit_a_float_or_a_column_keeping_fewer_digits(
    stored, batch, dialect
):
    assert holds(stored, batch, dialect) is None

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

import pytest

from tabsdatak.error import ConstraintError

# noinspection PyProtectedMember
from tabsdatak.conn.mssql import _validation


pytestmark = pytest.mark.mssql


# ---------------------------------------------------------------------------
# URI pattern
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "mssql://host:1433/db",
        "mssql+pyodbc://host/db",
        "MSSQL://host/db",
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=db.example.com,1433;"
        "Database=analytics;"
        "TrustServerCertificate=yes",
        "DRIVER=ODBC Driver 17 for SQL Server;SERVER=host;Database=db",
        "Server=host;Database=db;",
        pytest.param("Driver=ODBC Driver 18 for SQL Server;Server=host\\instance;Database=db", id="Driver"),
    ],
)
def test_mssql_uri_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.MSSQL_URI_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "host:1433/db",
        "postgres://host/db",
        "mysql://host/db",
        "://host/db",
    ],
)
def test_mssql_uri_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.MSSQL_URI_PATTERN, value) is None


# ---------------------------------------------------------------------------
# Table FQN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "dbo.users",
        "mydb.dbo.users",
        "[my db].[my schema].[my table]",
        "[bad]]name]",
    ],
)
def test_mssql_table_fqn_validator_accepts_valid(value):
    assert _validation.MSSQL_TABLE_FQN_VALIDATION.validate(value) == value


def test_mssql_table_fqn_validator_rejects_too_many_parts():
    with pytest.raises(ConstraintError, match="exceeds the maximum of 3"):
        _validation.MSSQL_TABLE_FQN_VALIDATION.validate("a.b.c.d")


def test_mssql_table_fqn_validator_rejects_unterminated_bracket():
    with pytest.raises(ConstraintError, match="unterminated quoted identifier"):
        _validation.MSSQL_TABLE_FQN_VALIDATION.validate("[db.users")


def test_mssql_table_fqn_validator_double_quoted_form_no_longer_unwrapped():
    # MSSQL's bracket convention is canonical for this connector; the
    # legacy double-quoted form (``"name"``) is no longer treated as a
    # quoted identifier. The dots inside still drive splitting, so a
    # 4-segment value like ``"a.b.c.d"`` overflows max_parts=3.
    with pytest.raises(ConstraintError, match="exceeds the maximum of 3"):
        _validation.MSSQL_TABLE_FQN_VALIDATION.validate('"a.b.c.d"')


def test_mssql_fqn_def_uses_brackets_with_doubled_close_escape():
    assert _validation._MSSQL_FQN_DEF.separator == "."
    assert _validation._MSSQL_FQN_DEF.quote == "["
    assert _validation._MSSQL_FQN_DEF.close_quote == "]"
    assert _validation._MSSQL_FQN_DEF.escape == "]"
    assert _validation._MSSQL_FQN_DEF.outer_only is False

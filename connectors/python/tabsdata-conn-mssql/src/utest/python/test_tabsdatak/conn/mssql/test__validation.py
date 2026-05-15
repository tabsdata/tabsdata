from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnInitException
# noinspection PyProtectedMember
from tabsdatak.conn.mssql import _validation
from tabsdatak.conn.mssql.error import MssqlErrorCode


pytestmark = pytest.mark.mssql


# ---------------------------------------------------------------------------
# Identifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "_t",
        "@var",
        "#temp",
        "[weird name]",
        '"weird name"',
    ],
)
def test_validate_identifier_accepts_valid(value):
    assert _validation.validate_identifier(value) == value


def test_validate_identifier_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("")
    assert ei.value.api_error is MssqlErrorCode.MSSQL_5


def test_validate_identifier_rejects_too_long():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("a" * 129)
    assert ei.value.api_error is MssqlErrorCode.MSSQL_11


def test_validate_identifier_rejects_bracket_with_unescaped_close():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("[bad]name]")
    assert ei.value.api_error is MssqlErrorCode.MSSQL_12


def test_validate_identifier_rejects_bracket_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("[]")
    assert ei.value.api_error is MssqlErrorCode.MSSQL_13


def test_validate_identifier_rejects_double_quoted_with_unescaped_quote():
    # Body with an unescaped " mid-string (escape pattern is "").
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier('"a"b"')
    assert ei.value.api_error is MssqlErrorCode.MSSQL_14


def test_validate_identifier_rejects_double_quoted_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier('""')
    assert ei.value.api_error is MssqlErrorCode.MSSQL_15


@pytest.mark.parametrize("value", ["1users", "user-name", "user.name", "with space"])
def test_validate_identifier_rejects_regular_bad_shape(value):
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier(value)
    assert ei.value.api_error is MssqlErrorCode.MSSQL_16


# ---------------------------------------------------------------------------
# URI
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "mssql://host:1433/db",
        "mssql+pyodbc://host/db",
        "MSSQL://host/db",
    ],
)
def test_validate_uri_accepts_valid(value):
    assert _validation.validate_uri(value) == value


# ODBC key=value connection strings: the validator only checks that
# the input starts with a ``<KEY>=`` (alphanumeric / space key), since
# pyodbc parses the full string itself. These shapes are what
# ``_adapter.build_mssql_engine`` routes through the
# ``mssql+pyodbc:///?odbc_connect=<encoded>`` branch.
@pytest.mark.parametrize(
    "value",
    [
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=db.example.com,1433;"
        "Database=analytics;"
        "TrustServerCertificate=yes",
        "DRIVER=ODBC Driver 17 for SQL Server;SERVER=host;Database=db",
        "Server=host;Database=db;",
        "Driver=ODBC Driver 18 for SQL Server;Server=host\\instance;Database=db",
    ],
)
def test_validate_uri_accepts_odbc_connection_string(value):
    assert _validation.validate_uri(value) == value


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
def test_validate_uri_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_uri(value)
    assert ei.value.api_error is MssqlErrorCode.MSSQL_17


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
    ],
)
def test_validate_table_fqn_accepts_valid(value):
    assert _validation.validate_table_fqn(value) == value


def test_validate_table_fqn_rejects_bad_arity():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn("a.b.c.d")
    assert ei.value.api_error is MssqlErrorCode.MSSQL_19


def test_validate_table_fqn_rejects_unterminated_bracket():
    # split_fqn_brackets raises ValueError when a bracket is left open.
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn("[db.users")
    assert ei.value.api_error is MssqlErrorCode.MSSQL_18


def test_validate_table_fqn_propagates_part_validators():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn("1bad.users")
    assert ei.value.api_error is MssqlErrorCode.MSSQL_16

from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnInitException
# noinspection PyProtectedMember
from tabsdatak.conn.oracle import _validation
from tabsdatak.conn.oracle.error import OracleErrorCode


pytestmark = pytest.mark.oracle


# ---------------------------------------------------------------------------
# Identifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "USERS",
        "U_1",
        "X#$",
        '"weird name"',
        '"with""embedded quote"',
    ],
)
def test_validate_identifier_accepts_valid(value):
    assert _validation.validate_identifier(value) == value


def test_validate_identifier_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("")
    assert ei.value.api_error is OracleErrorCode.ORACLE_5


def test_validate_identifier_rejects_too_long():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("A" * 129)
    assert ei.value.api_error is OracleErrorCode.ORACLE_12


def test_validate_identifier_rejects_quoted_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier('""')
    assert ei.value.api_error is OracleErrorCode.ORACLE_14


def test_validate_identifier_rejects_quoted_unescaped_quote():
    # Body 'a"b"c' has an unescaped " (no adjacent "" pair).
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier('"a"b"c"')
    assert ei.value.api_error is OracleErrorCode.ORACLE_13


@pytest.mark.parametrize("value", ["_USERS", "1USERS", "user-name", "with space"])
def test_validate_identifier_rejects_ordinary_bad_shape(value):
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier(value)
    assert ei.value.api_error is OracleErrorCode.ORACLE_15


# ---------------------------------------------------------------------------
# URI
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "oracle://host:1521/orcl",
        "oracle+oracledb://host/orcl",
        "ORACLE://host/orcl",
    ],
)
def test_validate_uri_accepts_valid(value):
    assert _validation.validate_uri(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "host:1521/orcl",
        "mysql://host/db",
        "://host/db",
    ],
)
def test_validate_uri_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_uri(value)
    assert ei.value.api_error is OracleErrorCode.ORACLE_6


# ---------------------------------------------------------------------------
# Table FQN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "USERS",
        "ANALYTICS.USERS",
        '"My Schema"."My Table"',
        '"weird.name".USERS',
    ],
)
def test_validate_table_fqn_accepts_valid(value):
    assert _validation.validate_table_fqn(value) == value


def test_validate_table_fqn_rejects_bad_arity():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn("a.b.c")
    assert ei.value.api_error is OracleErrorCode.ORACLE_17


def test_validate_table_fqn_rejects_unterminated_quote():
    # ``split_fqn`` raises ValueError when a double-quote is left open.
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn('"db.users')
    assert ei.value.api_error is OracleErrorCode.ORACLE_16


def test_validate_table_fqn_propagates_part_validators():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn("1BAD.USERS")
    assert ei.value.api_error is OracleErrorCode.ORACLE_15

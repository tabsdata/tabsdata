from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnInitException
# noinspection PyProtectedMember
from tabsdatak.conn.db2 import _validation
from tabsdatak.conn.db2.error import Db2ErrorCode


pytestmark = pytest.mark.db2


# ---------------------------------------------------------------------------
# Identifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "USERS",
        "U_1",
        "@SES",
        '"weird name"',
        '"with""embedded quote"',
    ],
)
def test_validate_identifier_accepts_valid(value):
    assert _validation.validate_identifier(value) == value


def test_validate_identifier_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("")
    assert ei.value.api_error is Db2ErrorCode.DB2_5


def test_validate_identifier_rejects_too_long():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("a" * 129)
    assert ei.value.api_error is Db2ErrorCode.DB2_12


def test_validate_identifier_rejects_delimited_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier('""')
    assert ei.value.api_error is Db2ErrorCode.DB2_14


def test_validate_identifier_rejects_delimited_with_unescaped_quote():
    # A delimited identifier with a stray unescaped double quote in
    # the body (after collapsing escapes) is malformed.
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier('"bad"name"')
    assert ei.value.api_error is Db2ErrorCode.DB2_13


@pytest.mark.parametrize("value", ["1USERS", "user-name", "user.name", "with space"])
def test_validate_identifier_rejects_ordinary_bad_shape(value):
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier(value)
    assert ei.value.api_error is Db2ErrorCode.DB2_15


# ---------------------------------------------------------------------------
# URI
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "db2://host:50000/SAMPLE",
        "ibm_db_sa://host/SAMPLE",
        "db2+ibm_db://host/SAMPLE",
        "DB2://host/db",
    ],
)
def test_validate_uri_accepts_valid(value):
    assert _validation.validate_uri(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "host:50000/SAMPLE",
        "mysql://host/db",
        "://host/db",
    ],
)
def test_validate_uri_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_uri(value)
    assert ei.value.api_error is Db2ErrorCode.DB2_6


# ---------------------------------------------------------------------------
# Table FQN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "USERS",
        "ANALYTICS.USERS",
        '"db"."tbl"',
        '"weird.name".USERS',
    ],
)
def test_validate_table_fqn_accepts_valid(value):
    assert _validation.validate_table_fqn(value) == value


def test_validate_table_fqn_rejects_bad_arity():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn("a.b.c")
    assert ei.value.api_error is Db2ErrorCode.DB2_17


def test_validate_table_fqn_rejects_unterminated_quote():
    # ``split_fqn`` raises ValueError when a double quote is left open.
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn('"db.users')
    assert ei.value.api_error is Db2ErrorCode.DB2_16


def test_validate_table_fqn_propagates_part_validators():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn("user-name")
    assert ei.value.api_error is Db2ErrorCode.DB2_15

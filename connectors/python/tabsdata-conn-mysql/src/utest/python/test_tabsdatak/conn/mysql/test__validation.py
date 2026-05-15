from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnInitException
# noinspection PyProtectedMember
from tabsdatak.conn.mysql import _validation
from tabsdatak.conn.mysql.error import MysqlErrorCode


pytestmark = pytest.mark.mysql


# ---------------------------------------------------------------------------
# Identifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "u_1",
        "$$",
        "`weird name`",
        "`with``embedded backtick`",
    ],
)
def test_validate_identifier_accepts_valid(value):
    assert _validation.validate_identifier(value) == value


def test_validate_identifier_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("")
    assert ei.value.api_error is MysqlErrorCode.MYSQL_5


def test_validate_identifier_rejects_too_long():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("a" * 65)
    assert ei.value.api_error is MysqlErrorCode.MYSQL_12


def test_validate_identifier_rejects_backtick_with_null_byte():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("`bad\x00name`")
    assert ei.value.api_error is MysqlErrorCode.MYSQL_13


def test_validate_identifier_rejects_backtick_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("``")
    assert ei.value.api_error is MysqlErrorCode.MYSQL_14


@pytest.mark.parametrize("value", ["user-name", "user.name", "with space"])
def test_validate_identifier_rejects_unquoted_bad_shape(value):
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier(value)
    assert ei.value.api_error is MysqlErrorCode.MYSQL_15


def test_validate_identifier_rejects_all_digits():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("123")
    assert ei.value.api_error is MysqlErrorCode.MYSQL_16


# ---------------------------------------------------------------------------
# URI
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "mysql://host:3306/db",
        "mariadb://host/db",
        "mysql+pymysql://host/db",
        "MYSQL://host/db",
    ],
)
def test_validate_uri_accepts_valid(value):
    assert _validation.validate_uri(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "host:3306/db",
        "postgres://host/db",
        "://host/db",
    ],
)
def test_validate_uri_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_uri(value)
    assert ei.value.api_error is MysqlErrorCode.MYSQL_6


# ---------------------------------------------------------------------------
# Table FQN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "analytics.users",
        "`db`.`tbl`",
        "`weird.name`.users",
    ],
)
def test_validate_table_fqn_accepts_valid(value):
    assert _validation.validate_table_fqn(value) == value


def test_validate_table_fqn_rejects_bad_arity():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn("a.b.c")
    assert ei.value.api_error is MysqlErrorCode.MYSQL_18


def test_validate_table_fqn_rejects_unterminated_quote():
    # ``split_fqn`` raises ValueError when a backtick is left open.
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn("`db.users")
    assert ei.value.api_error is MysqlErrorCode.MYSQL_17


def test_validate_table_fqn_propagates_part_validators():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn("user-name")
    assert ei.value.api_error is MysqlErrorCode.MYSQL_15

from __future__ import annotations

import re

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.mysql import _validation

pytestmark = pytest.mark.mysql


# ---------------------------------------------------------------------------
# URI pattern
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "mysql://host/db",
        "mysql://host:3306/db",
        "mariadb://host/db",
        "mysql+pymysql://host/db",
        "MYSQL://host/db",  # case-insensitive
    ],
)
def test_mysql_uri_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.MSYQL_URI_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "host:3306/db",
        "postgres://host/db",
        "://host/db",
        "mysqlx://host/db",  # mysqlx is not mysql/mariadb
    ],
)
def test_mysql_uri_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.MSYQL_URI_PATTERN, value) is None


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
        "`with``embedded backtick`",
    ],
)
def test_mysql_table_fqn_validator_accepts_valid(value):
    assert _validation.MYSQL_TABLE_FQN_VALIDATOR.func(value) == value


def test_mysql_table_fqn_validator_rejects_too_many_parts():
    with pytest.raises(ValueError, match="exceeds max_parts=2"):
        _validation.MYSQL_TABLE_FQN_VALIDATOR.func("a.b.c")


def test_mysql_table_fqn_validator_rejects_unterminated_quote():
    with pytest.raises(ValueError, match="unterminated quoted identifier"):
        _validation.MYSQL_TABLE_FQN_VALIDATOR.func("`db.users")


def test_mysql_fqn_def_uses_backtick_quote_and_doubled_backtick_escape():
    assert _validation._MYSQL_FQN_DEF.separator == "."
    assert _validation._MYSQL_FQN_DEF.quote == "`"
    assert _validation._MYSQL_FQN_DEF.escape == "`"
    assert _validation._MYSQL_FQN_DEF.outer_only is False

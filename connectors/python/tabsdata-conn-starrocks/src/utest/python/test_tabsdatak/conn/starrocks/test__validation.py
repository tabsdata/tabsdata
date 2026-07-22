#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import _validation


pytestmark = pytest.mark.starrocks


# ---------------------------------------------------------------------------
# URI pattern
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "starrocks://host:9030/db",
        "mysql://host:9030/db",
        "starrocks+pyhive://host/db",
        "STARROCKS://host/db",
    ],
)
def test_starrocks_uri_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.URI_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "host:9030/db",
        "postgres://host/db",
        "://host/db",
    ],
)
def test_starrocks_uri_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.URI_PATTERN, value) is None


# ---------------------------------------------------------------------------
# Table FQN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "analytics.users",
        "`db`.`tbl`",
        "`with``embedded backtick`",
    ],
)
def test_starrocks_table_fqn_validator_accepts_valid(value):
    assert _validation.STARROCKS_TABLE_FQN_VALIDATOR.func(value) == value


def test_starrocks_table_fqn_validator_rejects_too_many_parts():
    with pytest.raises(ValueError, match="exceeds max_parts=2"):
        _validation.STARROCKS_TABLE_FQN_VALIDATOR.func("a.b.c")


def test_starrocks_table_fqn_validator_rejects_unterminated_quote():
    with pytest.raises(ValueError, match="unterminated quoted identifier"):
        _validation.STARROCKS_TABLE_FQN_VALIDATOR.func("`db.users")


def test_starrocks_fqn_def_uses_backtick_and_doubled_backtick_escape():
    assert _validation._STARROCKS_FQN_DEF.separator == "."
    assert _validation._STARROCKS_FQN_DEF.quote == "`"
    assert _validation._STARROCKS_FQN_DEF.escape == "`"
    assert _validation._STARROCKS_FQN_DEF.outer_only is False

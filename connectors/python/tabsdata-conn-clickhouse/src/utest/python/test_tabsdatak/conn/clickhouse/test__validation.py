#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse import _validation


pytestmark = pytest.mark.clickhouse


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
def test_clickhouse_table_fqn_validator_accepts_valid(value):
    assert _validation.CLICKHOUSE_TABLE_FQN_VALIDATOR.func(value) == value


def test_clickhouse_table_fqn_validator_rejects_too_many_parts():
    with pytest.raises(ValueError, match="exceeds max_parts=2"):
        _validation.CLICKHOUSE_TABLE_FQN_VALIDATOR.func("a.b.c")


def test_clickhouse_table_fqn_validator_rejects_unterminated_quote():
    with pytest.raises(ValueError, match="unterminated quoted identifier"):
        _validation.CLICKHOUSE_TABLE_FQN_VALIDATOR.func("`db.users")


def test_clickhouse_fqn_def_uses_backtick_quote_and_doubled_backtick_escape():
    assert _validation.CLICKHOUSE_FQN_DEF.separator == "."
    assert _validation.CLICKHOUSE_FQN_DEF.quote == "`"
    assert _validation.CLICKHOUSE_FQN_DEF.escape == "`"
    assert _validation.CLICKHOUSE_FQN_DEF.outer_only is False

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.error import ConstraintError

# noinspection PyProtectedMember
from tabsdatak.conn.starrocks import _validation


pytestmark = pytest.mark.starrocks


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
    assert _validation.STARROCKS_TABLE_FQN_VALIDATION.validate(value) == value


def test_starrocks_table_fqn_validator_rejects_too_many_parts():
    with pytest.raises(ConstraintError, match="exceeds the maximum of 2"):
        _validation.STARROCKS_TABLE_FQN_VALIDATION.validate("a.b.c")


def test_starrocks_table_fqn_validator_rejects_unterminated_quote():
    with pytest.raises(ConstraintError, match="unterminated quoted identifier"):
        _validation.STARROCKS_TABLE_FQN_VALIDATION.validate("`db.users")


def test_starrocks_fqn_def_uses_backtick_and_doubled_backtick_escape():
    assert _validation._STARROCKS_FQN_DEF.separator == "."
    assert _validation._STARROCKS_FQN_DEF.quote == "`"
    assert _validation._STARROCKS_FQN_DEF.escape == "`"
    assert _validation._STARROCKS_FQN_DEF.outer_only is False

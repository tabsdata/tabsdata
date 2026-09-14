#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

import pytest

from tabsdatak.error import ConstraintError

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import _validation


pytestmark = pytest.mark.snowflake


# ---------------------------------------------------------------------------
# Identifier / account patterns (kept inline as regex patterns)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "USERS",
        "U_1",
        "_X$",
        '"weird name"',
        '"with""embedded quote"',
    ],
)
def test_identifier_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.IDENTIFIER_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "1USERS",
        "user-name",
        '""',
    ],
)
def test_identifier_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.IDENTIFIER_PATTERN, value) is None


@pytest.mark.parametrize(
    "value",
    [
        "ACME-MAIN",
        "abc.us-east-1",
        "ABC_123",
    ],
)
def test_account_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.ACCOUNT_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "-bad",
        "bad-",
        "ab cd",
    ],
)
def test_account_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.ACCOUNT_PATTERN, value) is None


# ---------------------------------------------------------------------------
# Table FQN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "USERS",
        "PUBLIC.USERS",
        "ANALYTICS.PUBLIC.USERS",
        '"My DB"."My Schema"."My Table"',
        '"with""embedded quote"',
    ],
)
def test_snowflake_table_fqn_validator_accepts_valid(value):
    assert _validation.SNOWFLAKE_TABLE_FQN_VALIDATION.validate(value) == value


def test_snowflake_table_fqn_validator_rejects_too_many_parts():
    with pytest.raises(ConstraintError, match="exceeds the maximum of 3"):
        _validation.SNOWFLAKE_TABLE_FQN_VALIDATION.validate("a.b.c.d")


def test_snowflake_table_fqn_validator_rejects_unterminated_quote():
    with pytest.raises(ConstraintError, match="unterminated quoted identifier"):
        _validation.SNOWFLAKE_TABLE_FQN_VALIDATION.validate('"db.users')


def test_snowflake_fqn_def_uses_double_quote_and_doubled_quote_escape():
    assert _validation._SNOWFLAKE_FQN_DEF.separator == "."
    assert _validation._SNOWFLAKE_FQN_DEF.quote == '"'
    assert _validation._SNOWFLAKE_FQN_DEF.escape == '"'
    assert _validation._SNOWFLAKE_FQN_DEF.outer_only is False


# ---------------------------------------------------------------------------
# Merge columns
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,stored",
    [
        ("order_id", "ORDER_ID"),
        ("ORDER_ID", "ORDER_ID"),
        ("mIxEd$1", "MIXED$1"),
        ('"myCol"', "myCol"),
        ('"weird""name"', 'weird"name'),
        ('"a.b"', "a.b"),
    ],
)
def test_merge_column_key_folds_unquoted_and_keeps_quoted(value, stored):
    assert _validation.merge_column_key(value) == stored


@pytest.mark.parametrize("value", ["orders.id", "", "1st", "a-b"])
def test_merge_column_validator_rejects_invalid(value):
    with pytest.raises(ValueError, match="not a Snowflake column identifier"):
        _validation.MERGE_COLUMN_VALIDATOR.func(value)

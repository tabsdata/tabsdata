#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

import pytest

from tabsdatak.error import ConstraintError

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import _validation


pytestmark = pytest.mark.bigquery


# ---------------------------------------------------------------------------
# Project / dataset patterns (kept inline as regex patterns)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "my-project",
        "ab-cd-ef",
        "p-" + "a" * 27 + "9",
    ],
)
def test_project_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.PROJECT_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "Abc-def",
        "abc",
        "1abc-def-ghi",
        "abc-def-",
        "abcd",
    ],
)
def test_project_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.PROJECT_PATTERN, value) is None


@pytest.mark.parametrize(
    "value",
    [
        "analytics",
        "data_v1",
        "X1Y2",
        "a",
    ],
)
def test_dataset_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.DATASET_PATTERN, value) is not None


def test_dataset_pattern_accepts_max_length():
    assert re.fullmatch(_validation.DATASET_PATTERN, "a" * 1024) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "bad-name",
        "bad.name",
    ],
)
def test_dataset_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.DATASET_PATTERN, value) is None


def test_dataset_pattern_rejects_above_max_length():
    assert re.fullmatch(_validation.DATASET_PATTERN, "a" * 1025) is None


# ---------------------------------------------------------------------------
# Table FQN (outer-only backtick wrapping, backslash escape)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "`proj.ds.tbl`",
        "`my-project.analytics.users`",
        "`analytics.users`",
        "`users`",
        "`proj.ds.weird\\`name`",
    ],
)
def test_bigquery_table_fqn_validator_accepts_valid(value):
    assert _validation.BIGQUERY_TABLE_FQN_VALIDATION.validate(value) == value


def test_bigquery_table_fqn_validator_rejects_unwrapped_value():
    # ``outer_only=True`` mode requires the FQN be wrapped in a single
    # pair of backticks; a bare ``proj.ds.tbl`` (legal in some
    # downstream SQL dialects but not what we validate against) is
    # rejected.
    with pytest.raises(ConstraintError, match="outer_only FQN must be wrapped"):
        _validation.BIGQUERY_TABLE_FQN_VALIDATION.validate("proj.ds.tbl")


def test_bigquery_table_fqn_validator_rejects_too_many_parts():
    with pytest.raises(ConstraintError, match="exceeds the maximum of 3"):
        _validation.BIGQUERY_TABLE_FQN_VALIDATION.validate("`a.b.c.d`")


def test_bigquery_table_fqn_validator_rejects_unterminated_quote():
    with pytest.raises(ConstraintError, match="outer_only FQN must be wrapped"):
        _validation.BIGQUERY_TABLE_FQN_VALIDATION.validate("`unterminated")


def test_bigquery_table_fqn_validator_rejects_empty_string():
    with pytest.raises(ConstraintError, match="value must be non-empty"):
        _validation.BIGQUERY_TABLE_FQN_VALIDATION.validate("")


def test_bigquery_fqn_def_uses_outer_only_backtick_with_backslash_escape():
    assert _validation._BIGQUERY_FQN_DEF.separator == "."
    assert _validation._BIGQUERY_FQN_DEF.quote == "`"
    assert _validation._BIGQUERY_FQN_DEF.escape == "\\"
    assert _validation._BIGQUERY_FQN_DEF.outer_only is True


# ---------------------------------------------------------------------------
# Merge columns
#
# One column name, folded to lower case. BigQuery resolves column names
# case-insensitively, so the folded form is a comparison key, not the stored
# name -- a column keeps the case it was defined with.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "op",
        "OP",
        "order_id",
        "_hidden",
        "Col1",
        "`op`",
        "`weird col`",
        "`OrderId`",
        "`ünïcode`",
        "a" * 300,
        "`" + "a" * 300 + "`",
    ],
)
def test_merge_column_validator_accepts_a_column_name(value):
    assert _validation._column_validate(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "1st",
        "with space",
        "with-hyphen",
        "``",
        "`unterminated",
        "unopened`",
        "`lead `",
        "` trail`",
        "a" * 301,
        "`" + "a" * 301 + "`",
        "col\x00",
        "`col\n`",
    ],
)
def test_merge_column_validator_rejects_a_non_column_name(value):
    with pytest.raises(ValueError, match="not a BigQuery column name"):
        _validation._column_validate(value)


def test_merge_column_validator_rejects_a_dot_even_inside_backticks():
    """A dot separates a path, so BigQuery cannot resolve a column carrying
    one."""
    with pytest.raises(ValueError, match="not a BigQuery column name"):
        _validation._column_validate("`a.b`")


def test_merge_column_validator_rejects_a_dotted_column():
    """A merge column is one column, not a qualified reference -- otherwise
    ``orders.id`` would read as a path."""
    with pytest.raises(ValueError, match="not a BigQuery column name"):
        _validation._column_validate("orders.id")


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("op", "op"),
        ("OP", "op"),
        ("Order_Id", "order_id"),
    ],
)
def test_merge_column_key_folds_to_lower_case(value, expected):
    assert _validation.merge_column_key(value) == expected


def test_merge_column_key_strips_backticks():
    assert _validation.merge_column_key("`weird col`") == "weird col"
    assert _validation.merge_column_key("`op`") == "op"


def test_merge_column_key_treats_quoted_and_bare_as_one_column():
    """Backticks quote a flexible column name; they do not make it
    case-sensitive, so these are the same column."""
    assert _validation.merge_column_key("`OP`") == "op"
    assert _validation.merge_column_key("op") == "op"


def test_merge_column_fqn_def_quotes_per_part():
    """A column name is one part, so ``outer_only`` is off -- otherwise a bare
    name would have to be backtick-wrapped."""
    assert _validation._BIGQUERY_COLUMN_FQN_DEF.outer_only is False

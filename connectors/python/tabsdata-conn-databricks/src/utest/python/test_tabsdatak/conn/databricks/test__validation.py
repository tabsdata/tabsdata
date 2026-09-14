#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

import pytest

from tabsdatak.error import ConstraintError

from tabsdatak.conn.databricks.error import DatabricksErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.databricks import _validation


pytestmark = pytest.mark.databricks


# ---------------------------------------------------------------------------
# Identifier / warehouse patterns (kept inline as regex patterns)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "_t",
        "T1",
        # Hyphens are part of real-world Databricks resource names
        # (e.g. ``databricks-ci-catalog`` / ``test-volume``) and
        # are SQL-safe via the per-part backtick wrapping the
        # impl applies in ``_fq_for_sql``.
        "user-name",
        "databricks-ci-catalog",
        "test-volume",
        "`weird name`",
    ],
)
def test_identifier_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.IDENTIFIER_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "1users",          # must start with letter or underscore
        "user.name",       # dot is the FQN separator
        "`bad.name`",      # dots forbidden inside backticks too
        "` leading`",      # leading whitespace inside backticks
    ],
)
def test_identifier_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.IDENTIFIER_PATTERN, value) is None


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "analytics.users",
        "main.analytics.users",
        "`my catalog`.analytics.users",
    ],
)
def test_qualified_name_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.QUALIFIED_NAME_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "a.b.c.d",
        "1users",
    ],
)
def test_qualified_name_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.QUALIFIED_NAME_PATTERN, value) is None


@pytest.mark.parametrize(
    "value",
    [
        "MY_WAREHOUSE",
        "abc",
        pytest.param("a" * 100, id="100as"),
    ],
)
def test_warehouse_name_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.WAREHOUSE_NAME_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        pytest.param("a" * 101, id="101as"),
        " leading",
        "trailing ",
        "with\x00null",
    ],
)
def test_warehouse_name_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.WAREHOUSE_NAME_PATTERN, value) is None


@pytest.mark.parametrize(
    "value",
    [
        "0123456789abcdef",
        "ffffffffffffffff",
    ],
)
def test_warehouse_id_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.WAREHOUSE_ID_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "0123456789ABCDEF",
        "0123456789abc",
        "0123456789abcdefg",
        "zzzzzzzzzzzzzzzz",
    ],
)
def test_warehouse_id_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.WAREHOUSE_ID_PATTERN, value) is None


# ---------------------------------------------------------------------------
# Table FQN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "analytics.users",
        "main.analytics.users",
        "`my catalog`.`my schema`.`my table`",
        "`with``embedded backtick`",
    ],
)
def test_databricks_table_fqn_validator_accepts_valid(value):
    assert _validation.DATABRICKS_TABLE_FQN_VALIDATION.validate(value) == value


def test_databricks_table_fqn_validator_rejects_too_many_parts():
    with pytest.raises(ConstraintError, match="exceeds the maximum of 3"):
        _validation.DATABRICKS_TABLE_FQN_VALIDATION.validate("a.b.c.d")


def test_databricks_table_fqn_validator_rejects_unterminated_quote():
    with pytest.raises(ConstraintError, match="unterminated quoted identifier"):
        _validation.DATABRICKS_TABLE_FQN_VALIDATION.validate("main.`unterminated")


def test_databricks_fqn_def_uses_backtick_and_doubled_backtick_escape():
    assert _validation._DATABRICKS_FQN_DEF.separator == "."
    assert _validation._DATABRICKS_FQN_DEF.quote == "`"
    assert _validation._DATABRICKS_FQN_DEF.escape == "`"
    assert _validation._DATABRICKS_FQN_DEF.outer_only is False


# ---------------------------------------------------------------------------
# Merge columns
#
# Databricks resolves identifiers case-insensitively and a backtick only quotes
# a special-char name, so the canonical key is unquoted and lower-cased. That is
# a comparison key, not the stored name -- a Delta column keeps the case it was
# defined with.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    ["op", "order_id", "ORDER_ID", "OrderId", "order-id", "_x", "`weird col`"],
)
def test_merge_column_validation_accepts_an_identifier(value):
    assert _validation.COLUMN_VALIDATION.validate(value) == value


@pytest.mark.parametrize(
    "value",
    ["orders.id", "`a.b`", "", " ", "1abc", "a`b", "`unclosed"],
)
def test_merge_column_validation_rejects_a_non_identifier(value):
    with pytest.raises(ConstraintError) as ei:
        _validation.COLUMN_VALIDATION.validate(value)
    assert ei.value.code is DatabricksErrorCode.DATABRICKS_14


def test_merge_column_validation_rejects_a_dot_even_inside_backticks():
    """A dot is out whichever way it is written: Delta reads one as nested-field
    access, so a column cannot carry it without column mapping."""
    with pytest.raises(ConstraintError):
        _validation.COLUMN_VALIDATION.validate("`a.b`")


def test_merge_column_validation_rejects_a_dotted_column():
    """A merge column is one column, not a qualified reference -- otherwise
    ``orders.id`` would read as a table-qualified name."""
    with pytest.raises(ConstraintError, match="not a valid Databricks identifier"):
        _validation.COLUMN_VALIDATION.validate("orders.id")


def test_merge_column_validation_is_the_identifier_constraint():
    """A column is one identifier, so it reuses that constraint instead of
    restating the pattern -- the two cannot drift apart."""
    assert _validation.COLUMN_VALIDATION is _validation.IDENTIFIER_VALIDATION


@pytest.mark.parametrize(
    "value,expected",
    [
        ("op", "op"),
        ("OP", "op"),
        ("order_id", "order_id"),
        ("ORDER_ID", "order_id"),
        ("OrderId", "orderid"),
        ("order-id", "order-id"),
        ("`OrderId`", "orderid"),
        ("`weird col`", "weird col"),
    ],
)
def test_merge_column_key_folds_to_lower_case(value, expected):
    assert _validation.merge_column_key(value) == expected


def test_merge_column_key_strips_backticks():
    """Backticks are quoting, not part of the name."""
    assert _validation.merge_column_key("`op`") == "op"


def test_merge_column_key_treats_quoted_and_bare_as_one_column():
    """Unlike Snowflake, quoting does not make a Databricks name
    case-sensitive, so these are the same column."""
    assert (
        _validation.merge_column_key("`OP`")
        == _validation.merge_column_key("op")
        == "op"
    )


def test_merge_column_key_unescapes_a_doubled_backtick():
    assert _validation.merge_column_key("`we``ird`") == "we`ird"

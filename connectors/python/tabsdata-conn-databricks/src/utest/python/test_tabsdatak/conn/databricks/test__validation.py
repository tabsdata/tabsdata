#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

import pytest

from tabsdatak.error import ConstraintError

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

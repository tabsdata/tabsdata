#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

import pytest

from tabsdatak.error import ConstraintError

# noinspection PyProtectedMember
from tabsdatak.conn.db2 import _validation


pytestmark = pytest.mark.db2


# ---------------------------------------------------------------------------
# URI pattern
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "db2://host/SAMPLE",
        "db2://host:50000/SAMPLE",
        "ibm_db_sa://host/SAMPLE",
        "db2+ibm_db://host/SAMPLE",
        "DB2://host/db",
    ],
)
def test_db2_uri_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.URI_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "mysql://host/db",
        "://host/db",
        "host:50000/SAMPLE",
    ],
)
def test_db2_uri_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.URI_PATTERN, value) is None


# ---------------------------------------------------------------------------
# Table FQN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "analytics.users",
        '"schema name".users',
        '"with""embedded quote"',
    ],
)
def test_db2_table_fqn_validator_accepts_valid(value):
    assert _validation.DB2_TABLE_FQN_VALIDATION.validate(value) == value


def test_db2_table_fqn_validator_rejects_too_many_parts():
    with pytest.raises(ConstraintError, match="exceeds the maximum of 2"):
        _validation.DB2_TABLE_FQN_VALIDATION.validate("a.b.c")


def test_db2_table_fqn_validator_rejects_unterminated_quote():
    with pytest.raises(ConstraintError, match="unterminated quoted identifier"):
        _validation.DB2_TABLE_FQN_VALIDATION.validate('"db.users')


def test_db2_fqn_def_uses_double_quote_and_doubled_quote_escape():
    assert _validation._DB2_FQN_DEF.separator == "."
    assert _validation._DB2_FQN_DEF.quote == '"'
    assert _validation._DB2_FQN_DEF.escape == '"'
    assert _validation._DB2_FQN_DEF.outer_only is False

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

import pytest

from tabsdatak.error import ConstraintError

# noinspection PyProtectedMember
from tabsdatak.conn.postgres import _validation


pytestmark = pytest.mark.postgres


# ---------------------------------------------------------------------------
# URI pattern
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "postgres://host/db",
        "postgres://host:5432/db",
        "postgresql://host/db",
        "postgresql+psycopg2://host/db",
        "POSTGRES://host/db",
    ],
)
def test_postgres_uri_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.URI_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "host:5432/db",
        "mysql://host/db",
        "://host/db",
    ],
)
def test_postgres_uri_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.URI_PATTERN, value) is None


# ---------------------------------------------------------------------------
# Table FQN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "public.users",
        "analytics.public.users",
        '"my schema"."my table"',
        '"with""embedded quote"',
    ],
)
def test_postgres_table_fqn_validator_accepts_valid(value):
    assert _validation.POSTGRES_TABLE_FQN_VALIDATION.validate(value) == value


def test_postgres_table_fqn_validator_rejects_too_many_parts():
    with pytest.raises(ConstraintError, match="exceeds the maximum of 3"):
        _validation.POSTGRES_TABLE_FQN_VALIDATION.validate("a.b.c.d")


def test_postgres_table_fqn_validator_rejects_unterminated_quote():
    with pytest.raises(ConstraintError, match="unterminated quoted identifier"):
        _validation.POSTGRES_TABLE_FQN_VALIDATION.validate('"db.users')


def test_postgres_fqn_def_uses_double_quote_and_doubled_quote_escape():
    assert _validation._POSTGRES_FQN_DEF.separator == "."
    assert _validation._POSTGRES_FQN_DEF.quote == '"'
    assert _validation._POSTGRES_FQN_DEF.escape == '"'
    assert _validation._POSTGRES_FQN_DEF.outer_only is False

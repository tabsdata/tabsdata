from __future__ import annotations

import re

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.oracle import _validation


pytestmark = pytest.mark.oracle


# ---------------------------------------------------------------------------
# URI pattern
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "oracle://host/orcl",
        "oracle://host:1521/orcl",
        "oracle+oracledb://host/orcl",
        "ORACLE://host/orcl",
    ],
)
def test_oracle_uri_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.URI_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "postgres://host/db",
        "://host/db",
        "host:1521/orcl",
    ],
)
def test_oracle_uri_pattern_rejects_invalid(value):
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
        '"weird name"',
    ],
)
def test_oracle_table_fqn_validator_accepts_valid(value):
    assert _validation.ORACLE_TABLE_FQN_VALIDATOR.func(value) == value


def test_oracle_table_fqn_validator_rejects_too_many_parts():
    with pytest.raises(ValueError, match="exceeds max_parts=2"):
        _validation.ORACLE_TABLE_FQN_VALIDATOR.func("a.b.c")


def test_oracle_table_fqn_validator_rejects_unterminated_quote():
    with pytest.raises(ValueError, match="unterminated quoted identifier"):
        _validation.ORACLE_TABLE_FQN_VALIDATOR.func('"db.users')


def test_oracle_table_fqn_validator_rejects_doubled_quote_embed():
    # Oracle disallows the doubled-quote convention -- there is no
    # escape mechanism for embedding ``"`` in a quoted identifier.
    # ``"a""b"`` parses as the quoted identifier ``a`` followed by an
    # unexpected ``"`` at position 3, surfaced as ``expected separator``.
    with pytest.raises(ValueError):
        _validation.ORACLE_TABLE_FQN_VALIDATOR.func('"a""b"')


def test_oracle_fqn_def_uses_double_quote_and_no_escape():
    assert _validation._ORACLE_FQN_DEF.separator == "."
    assert _validation._ORACLE_FQN_DEF.quote == '"'
    assert _validation._ORACLE_FQN_DEF.escape is None
    assert _validation._ORACLE_FQN_DEF.outer_only is False

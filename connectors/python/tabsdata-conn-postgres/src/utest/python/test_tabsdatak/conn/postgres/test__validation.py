from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnInitException
# noinspection PyProtectedMember
from tabsdatak.conn.postgres import _validation
from tabsdatak.conn.postgres.error import PostgresErrorCode


pytestmark = pytest.mark.postgres


# ---------------------------------------------------------------------------
# Identifier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "u_1",
        "_t",
        '"weird name"',
        '"with""embedded quote"',
    ],
)
def test_validate_identifier_accepts_valid(value):
    assert _validation.validate_identifier(value) == value


def test_validate_identifier_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("")
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_5


def test_validate_identifier_rejects_too_long():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier("a" * 64)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_12


def test_validate_identifier_rejects_double_quoted_with_null_byte():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier('"bad\x00name"')
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_13


def test_validate_identifier_rejects_double_quoted_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier('""')
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_14


@pytest.mark.parametrize("value", ["1users", "user-name", "user.name", "with space"])
def test_validate_identifier_rejects_unquoted_bad_shape(value):
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_identifier(value)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_15


# ---------------------------------------------------------------------------
# URI
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "postgres://host:5432/db",
        "postgresql://host/db",
        "postgresql+psycopg2://host/db",
        "POSTGRES://host/db",
    ],
)
def test_validate_uri_accepts_valid(value):
    assert _validation.validate_uri(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "host:5432/db",
        "mysql://host/db",
        "://host/db",
    ],
)
def test_validate_uri_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_uri(value)
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_6


# ---------------------------------------------------------------------------
# Table FQN
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "public.users",
        '"my schema"."my table"',
        '"weird.name".users',
    ],
)
def test_validate_table_fqn_accepts_valid(value):
    assert _validation.validate_table_fqn(value) == value


def test_validate_table_fqn_rejects_bad_arity():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn("a.b.c")
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_17


def test_validate_table_fqn_rejects_unterminated_quote():
    # ``split_fqn`` raises ValueError when a quote is left open.
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn('"db.users')
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_16


def test_validate_table_fqn_propagates_part_validators():
    with pytest.raises(ConnInitException) as ei:
        _validation.validate_table_fqn("1bad.users")
    assert ei.value.api_error is PostgresErrorCode.POSTGRES_15


import pytest

from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.snowflake import _validation
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode


pytestmark = pytest.mark.snowflake


@pytest.mark.parametrize("value", ["USERS", "U_1", "_X$", '"weird name"'])
def test_validate_identifier_accepts_valid(value):
    assert _validation._validate_identifier(value) == value


def test_validate_identifier_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier("")
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_4


def test_validate_identifier_rejects_too_long():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier("a" * 256)
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_9


def test_validate_identifier_rejects_double_quoted_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier('""')
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_11


@pytest.mark.parametrize("value", ["1USERS", "user-name"])
def test_validate_identifier_rejects_unquoted_bad_shape(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier(value)
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_12


@pytest.mark.parametrize("value", ["ACME-MAIN", "abc.us-east-1", "ABC_123"])
def test_validate_account_accepts_valid(value):
    assert _validation._validate_account(value) == value


def test_validate_account_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_account("")
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_5


@pytest.mark.parametrize("value", ["-bad", "bad-", "ab cd"])
def test_validate_account_rejects_bad_shape(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_account(value)
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_13


@pytest.mark.parametrize(
    "value",
    [
        "USERS",
        "PUBLIC.USERS",
        "ANALYTICS.PUBLIC.USERS",
        '"My DB"."My Schema"."My Table"',
    ],
)
def test_validate_table_fqn_accepts_valid(value):
    assert _validation._validate_table_fqn(value) == value


def test_validate_table_fqn_rejects_bad_arity():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_table_fqn("a.b.c.d")
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_15


def test_validate_table_fqn_propagates_part_validators():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_table_fqn("1bad.users")
    assert ei.value.api_error is SnowflakeErrorCode.SNOWFLAKE_12

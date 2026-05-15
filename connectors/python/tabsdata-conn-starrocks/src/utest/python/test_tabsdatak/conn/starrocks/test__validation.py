
import pytest

from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.starrocks import _validation
from tabsdatak.conn.starrocks.error import StarRocksErrorCode


pytestmark = pytest.mark.starrocks


@pytest.mark.parametrize("value", ["users", "u_1", "$$", "`weird name`"])
def test_validate_identifier_accepts_valid(value):
    assert _validation._validate_identifier(value) == value


def test_validate_identifier_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier("")
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_3


def test_validate_identifier_rejects_too_long():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier("a" * 65)
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_5


def test_validate_identifier_rejects_backtick_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier("``")
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_7


@pytest.mark.parametrize("value", ["user-name", "user.name"])
def test_validate_identifier_rejects_unquoted_bad_shape(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier(value)
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_8


def test_validate_identifier_rejects_all_digits():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier("123")
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_9


@pytest.mark.parametrize(
    "value",
    [
        "starrocks://host:9030/db",
        "mysql://host:9030/db",
        "starrocks+pyhive://host/db",
        "STARROCKS://host/db",
    ],
)
def test_validate_uri_accepts_valid(value):
    assert _validation._validate_uri(value) == value


@pytest.mark.parametrize("value", ["", "host:9030/db", "postgres://host/db"])
def test_validate_uri_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_uri(value)
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_4


@pytest.mark.parametrize("value", ["users", "analytics.users", "`db`.`tbl`"])
def test_validate_table_fqn_accepts_valid(value):
    assert _validation._validate_table_fqn(value) == value


def test_validate_table_fqn_rejects_bad_arity():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_table_fqn("a.b.c")
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_11


def test_validate_table_fqn_propagates_part_validators():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_table_fqn("user-name")
    assert ei.value.api_error is StarRocksErrorCode.STARROCKS_8

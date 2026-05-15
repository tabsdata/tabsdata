
import pytest

from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.clickhouse import _validation
from tabsdatak.conn.clickhouse.error import ClickHouseErrorCode


pytestmark = pytest.mark.clickhouse


@pytest.mark.parametrize(
    "value", ["users", "_t", "T1", "`weird name`", '"weird name"']
)
def test_validate_identifier_accepts_valid(value):
    assert _validation._validate_identifier(value) == value


def test_validate_identifier_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier("")
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_3


def test_validate_identifier_rejects_backtick_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier("``")
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_6


def test_validate_identifier_rejects_double_quoted_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier('""')
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_8


@pytest.mark.parametrize(
    "value",
    [
        "1users",
        "user-name",
        "user.name",
    ],
)
def test_validate_identifier_rejects_unquoted_bad_shape(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier(value)
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_9


@pytest.mark.parametrize(
    "value",
    [
        "clickhouse://host:9000",
        "clickhouse+http://host",
        "CLICKHOUSE://host",
    ],
)
def test_validate_uri_accepts_valid(value):
    assert _validation._validate_uri(value) == value


@pytest.mark.parametrize("value", ["", "clickhouse:host", "http://host", "ch://host"])
def test_validate_uri_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_uri(value)
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_4


@pytest.mark.parametrize("value", ["users", "analytics.users", "`db name`.`table name`"])
def test_validate_table_fqn_accepts_valid(value):
    assert _validation._validate_table_fqn(value) == value


def test_validate_table_fqn_rejects_bad_arity():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_table_fqn("a.b.c")
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_10


def test_validate_table_fqn_propagates_part_validators():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_table_fqn("1bad.users")
    assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_9


import pytest

from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.databricks import _validation
from tabsdatak.conn.databricks.error import DatabricksErrorCode


pytestmark = pytest.mark.databricks


@pytest.mark.parametrize(
    "value", ["users", "_t", "T1", "`weird name`"]
)
def test_validate_identifier_accepts_valid(value):
    assert _validation._validate_identifier(value) == value


def test_validate_identifier_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier("")
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_5


def test_validate_identifier_rejects_too_long():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier("a" * 256)
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_9


@pytest.mark.parametrize("value", ["`bad.name`", "` leading`"])
def test_validate_identifier_rejects_backtick_bad(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier(value)
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_10


@pytest.mark.parametrize("value", ["1users", "user-name"])
def test_validate_identifier_rejects_unquoted_bad_shape(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_identifier(value)
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_11


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "analytics.users",
        "main.analytics.users",
        "`my catalog`.analytics.users",
    ],
)
def test_validate_qualified_name_accepts_valid(value):
    assert _validation._validate_qualified_name(value) == value


def test_validate_qualified_name_rejects_unterminated_backtick():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_qualified_name("main.`unterminated")
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_6


def test_validate_qualified_name_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_qualified_name("")
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_12


def test_validate_qualified_name_rejects_bad_arity():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_qualified_name("a.b.c.d")
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_13


@pytest.mark.parametrize("value", ["MY_WAREHOUSE", "abc", "a" * 100])
def test_validate_warehouse_name_accepts_valid(value):
    assert _validation._validate_warehouse_name(value) == value


@pytest.mark.parametrize("value", ["", "a" * 101])
def test_validate_warehouse_name_rejects_bad_length(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_warehouse_name(value)
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_7


@pytest.mark.parametrize("value", [" leading", "trailing "])
def test_validate_warehouse_name_rejects_whitespace(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_warehouse_name(value)
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_14


def test_validate_warehouse_name_rejects_control_char():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_warehouse_name("with\x00null")
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_15


@pytest.mark.parametrize("value", ["0123456789abcdef", "ffffffffffffffff"])
def test_validate_warehouse_id_accepts_valid(value):
    assert _validation._validate_warehouse_id(value) == value


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
def test_validate_warehouse_id_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_warehouse_id(value)
    assert ei.value.api_error is DatabricksErrorCode.DATABRICKS_8

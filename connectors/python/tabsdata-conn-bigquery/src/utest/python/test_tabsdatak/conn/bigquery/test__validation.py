
import pytest

from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.bigquery import _validation
from tabsdatak.conn.bigquery.error import BigqueryErrorCode


pytestmark = pytest.mark.bigquery


@pytest.mark.parametrize("value", ["my-project", "ab-cd-ef", "p-" + "a" * 27 + "9"])
def test_validate_project_id_accepts_valid(value):
    assert _validation._validate_project_id(value) == value


@pytest.mark.parametrize(
    "value", ["", "Abc-def", "abc", "1abc-def-ghi", "abc-def-", "abcd"]
)
def test_validate_project_id_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_project_id(value)
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_4


@pytest.mark.parametrize("value", ["analytics", "data_v1", "X1Y2"])
def test_validate_dataset_id_accepts_valid(value):
    assert _validation._validate_dataset_id(value) == value


@pytest.mark.parametrize("value", ["", "bad-name", "bad.name", "a" * 1025])
def test_validate_dataset_id_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_dataset_id(value)
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_5


@pytest.mark.parametrize("value", ["users", "my_table", "X1"])
def test_validate_table_id_accepts_valid(value):
    assert _validation._validate_table_id(value) == value


@pytest.mark.parametrize("value", ["", "bad-name", "a" * 1025])
def test_validate_table_id_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_table_id(value)
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_6


@pytest.mark.parametrize(
    "value",
    [
        "users",
        "analytics.users",
        "my-project.analytics.users",
        "`my-project`.`analytics`.`users`",
    ],
)
def test_validate_table_fqn_accepts_valid(value):
    assert _validation._validate_table_fqn(value) == value


def test_validate_table_fqn_rejects_bad_arity():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_table_fqn("a.b.c.d")
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_8


def test_validate_table_fqn_propagates_part_validators():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_table_fqn("bad-name")
    assert ei.value.api_error is BigqueryErrorCode.BIGQUERY_6

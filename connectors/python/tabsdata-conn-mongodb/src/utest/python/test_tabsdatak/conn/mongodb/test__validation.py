
import pytest

from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.mongodb import _validation
from tabsdatak.conn.mongodb.error import MongodbErrorCode


pytestmark = pytest.mark.mongodb


@pytest.mark.parametrize("value", ["mydb", "users_v1", "a" * 63])
def test_validate_database_name_accepts_valid(value):
    assert _validation._validate_database_name(value) == value


def test_validate_database_name_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_database_name("")
    assert ei.value.api_error is MongodbErrorCode.MONGODB_3


def test_validate_database_name_rejects_too_long():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_database_name("a" * 64)
    assert ei.value.api_error is MongodbErrorCode.MONGODB_7


def test_validate_database_name_rejects_null_byte():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_database_name("bad\x00name")
    assert ei.value.api_error is MongodbErrorCode.MONGODB_8


@pytest.mark.parametrize(
    "value", ["bad/name", "bad\\name", 'bad"name', "bad.name", "bad name"]
)
def test_validate_database_name_rejects_forbidden_char(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_database_name(value)
    assert ei.value.api_error is MongodbErrorCode.MONGODB_9


@pytest.mark.parametrize("value", ["users", "users.events", "a" * 235])
def test_validate_collection_name_accepts_valid(value):
    assert _validation._validate_collection_name(value) == value


def test_validate_collection_name_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_collection_name("")
    assert ei.value.api_error is MongodbErrorCode.MONGODB_4


def test_validate_collection_name_rejects_too_long():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_collection_name("a" * 236)
    assert ei.value.api_error is MongodbErrorCode.MONGODB_10


def test_validate_collection_name_rejects_null_byte():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_collection_name("bad\x00name")
    assert ei.value.api_error is MongodbErrorCode.MONGODB_11


def test_validate_collection_name_rejects_dollar():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_collection_name("bad$name")
    assert ei.value.api_error is MongodbErrorCode.MONGODB_12


def test_validate_collection_name_rejects_system_prefix():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_collection_name("system.users")
    assert ei.value.api_error is MongodbErrorCode.MONGODB_13


@pytest.mark.parametrize("value", ["name", "user_id", "field-with-dashes"])
def test_validate_field_name_accepts_valid(value):
    assert _validation._validate_field_name(value) == value


def test_validate_field_name_rejects_empty():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_field_name("")
    assert ei.value.api_error is MongodbErrorCode.MONGODB_5


def test_validate_field_name_rejects_null_byte():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_field_name("a\x00b")
    assert ei.value.api_error is MongodbErrorCode.MONGODB_14


def test_validate_field_name_rejects_dot():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_field_name("a.b")
    assert ei.value.api_error is MongodbErrorCode.MONGODB_15


def test_validate_field_name_rejects_dollar_prefix():
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_field_name("$op")
    assert ei.value.api_error is MongodbErrorCode.MONGODB_16


@pytest.mark.parametrize(
    "value",
    [
        "mongodb://localhost",
        "mongodb://user:pass@host:27017/db",
        "mongodb+srv://cluster.example.com",
        "MONGODB://localhost",
    ],
)
def test_validate_uri_accepts_valid(value):
    assert _validation._validate_uri(value) == value


@pytest.mark.parametrize("value", ["", "localhost", "http://localhost", "mysql://localhost"])
def test_validate_uri_rejects_invalid(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_uri(value)
    assert ei.value.api_error is MongodbErrorCode.MONGODB_6

#
# Copyright 2026 Tabsdata Inc.
#


import pytest

from tabsdatak.error import ConstraintError

from tabsdatak.conn.common.error import ConnInitException
from tabsdatak.conn.mongodb import _validation
from tabsdatak.conn.mongodb.error import MongoDBErrorCode


pytestmark = pytest.mark.mongodb


@pytest.mark.parametrize("value", ["users", "users.events", pytest.param("a" * 235, id="a35")])
def test_validate_collection_name_accepts_valid(value):
    assert _validation.COLLECTION_NAME_VALIDATION.validate(value) == value


def test_validate_collection_name_rejects_empty():
    with pytest.raises(ConstraintError) as ei:
        _validation.COLLECTION_NAME_VALIDATION.validate("")
    assert ei.value.code is MongoDBErrorCode.MONGODB_4


def test_validate_collection_name_rejects_too_long():
    with pytest.raises(ConstraintError) as ei:
        _validation.COLLECTION_NAME_VALIDATION.validate("a" * 236)
    assert ei.value.code is MongoDBErrorCode.MONGODB_10


def test_validate_collection_name_rejects_null_byte():
    with pytest.raises(ConstraintError) as ei:
        _validation.COLLECTION_NAME_VALIDATION.validate("bad\x00name")
    assert ei.value.code is MongoDBErrorCode.MONGODB_11


def test_validate_collection_name_rejects_dollar():
    with pytest.raises(ConstraintError) as ei:
        _validation.COLLECTION_NAME_VALIDATION.validate("bad$name")
    assert ei.value.code is MongoDBErrorCode.MONGODB_12


def test_validate_collection_name_rejects_system_prefix():
    with pytest.raises(ConstraintError) as ei:
        _validation.COLLECTION_NAME_VALIDATION.validate("system.users")
    assert ei.value.code is MongoDBErrorCode.MONGODB_13


@pytest.mark.parametrize("value", ["name", "user_id", "field-with-dashes"])
def test_validate_field_name_accepts_valid(value):
    assert _validation.FIELD_NAME_VALIDATION.validate(value) == value


def test_validate_field_name_rejects_empty():
    with pytest.raises(ConstraintError) as ei:
        _validation.FIELD_NAME_VALIDATION.validate("")
    assert ei.value.code is MongoDBErrorCode.MONGODB_5


def test_validate_field_name_rejects_null_byte():
    with pytest.raises(ConstraintError) as ei:
        _validation.FIELD_NAME_VALIDATION.validate("a\x00b")
    assert ei.value.code is MongoDBErrorCode.MONGODB_14


def test_validate_field_name_rejects_dot():
    with pytest.raises(ConstraintError) as ei:
        _validation.FIELD_NAME_VALIDATION.validate("a.b")
    assert ei.value.code is MongoDBErrorCode.MONGODB_15


def test_validate_field_name_rejects_dollar_prefix():
    with pytest.raises(ConstraintError) as ei:
        _validation.FIELD_NAME_VALIDATION.validate("$op")
    assert ei.value.code is MongoDBErrorCode.MONGODB_16


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
    assert _validation.URI_VALIDATION.validate(value) == value


@pytest.mark.parametrize("value", ["", "localhost", "http://localhost", "mysql://localhost"])
def test_validate_uri_rejects_invalid(value):
    with pytest.raises(ConstraintError) as ei:
        _validation.URI_VALIDATION.validate(value)
    assert ei.value.code is MongoDBErrorCode.MONGODB_6

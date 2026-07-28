#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.mongodb.error import (
    MongoDBRuntimeException,
    MongoDBValidateException,
    MongoDBErrorCode,
)

from tabsdatak.error import TabsdataException

pytestmark = pytest.mark.mongodb


def test_validate_exception_is_a_conn_exception():
    assert issubclass(MongoDBValidateException, ConnException)


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(MongoDBRuntimeException, ConnException)


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


def test_every_code_uses_known_exception():
    """Defence: each code's declared exception must be one of the package's
    hierarchy roots."""
    allowed = (
        ConnInitException,
        MongoDBValidateException,
        MongoDBRuntimeException,
    )
    for code in MongoDBErrorCode:
        assert issubclass(code.value.exception, allowed)

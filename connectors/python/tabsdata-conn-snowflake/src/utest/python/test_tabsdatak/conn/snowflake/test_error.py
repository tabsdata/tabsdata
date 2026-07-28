#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.snowflake.error import (
    SnowflakeErrorCode,
    SnowflakeRuntimeException,
    SnowflakeValidateException,
)

from tabsdatak.error import TabsdataException

pytestmark = pytest.mark.snowflake


def test_validate_exception_is_a_conn_exception():
    assert issubclass(SnowflakeValidateException, ConnException)


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(SnowflakeRuntimeException, ConnException)


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


def test_every_code_uses_known_exception():
    """Defence: each code's declared exception must be one of the package's
    hierarchy roots."""
    allowed = (
        ConnInitException,
        SnowflakeValidateException,
        SnowflakeRuntimeException,
    )
    for code in SnowflakeErrorCode:
        assert issubclass(code.value.exception, allowed)

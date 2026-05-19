from __future__ import annotations

import pytest

from tabsdatak.error import TabsdataException
from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.clickhouse.error import (
    ClickHouseErrorCode,
    ClickHouseRuntimeException,
    ClickHouseValidateException,
)


pytestmark = pytest.mark.clickhouse


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------


def test_validate_exception_is_a_conn_exception():
    assert issubclass(ClickHouseValidateException, ConnException)


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(ClickHouseRuntimeException, ConnException)


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


# ---------------------------------------------------------------------------
# Code numbering
# ---------------------------------------------------------------------------


def test_every_code_uses_known_exception():
    """Defence: each code's declared exception must be one of the
    package's hierarchy roots."""
    allowed = (
        ConnInitException,
        ClickHouseValidateException,
        ClickHouseRuntimeException,
    )
    for code in ClickHouseErrorCode:
        assert issubclass(code.value.exception, allowed)

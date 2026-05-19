from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.databricks.error import (
    DatabricksErrorCode,
    DatabricksRuntimeException,
    DatabricksValidateException,
)
from tabsdatak.error import TabsdataException


pytestmark = pytest.mark.databricks


def test_validate_exception_is_a_conn_exception():
    assert issubclass(DatabricksValidateException, ConnException)


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(DatabricksRuntimeException, ConnException)


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


def test_every_code_uses_known_exception():
    allowed = (
        ConnInitException,
        DatabricksValidateException,
        DatabricksRuntimeException,
    )
    for code in DatabricksErrorCode:
        assert issubclass(code.value.exception, allowed)

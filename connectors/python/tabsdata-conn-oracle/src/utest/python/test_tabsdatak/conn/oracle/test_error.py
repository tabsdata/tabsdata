from __future__ import annotations

import pytest

from tabsdatak.error import DEBUG_FRAMES_ENV_VAR, TabsdataException
from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.oracle.error import (
    OracleRuntimeException,
    OracleValidateException,
    OracleErrorCode,
)


pytestmark = pytest.mark.oracle


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------


def test_validate_exception_is_a_conn_exception():
    assert issubclass(OracleValidateException, ConnException)


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(OracleRuntimeException, ConnException)


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


# ---------------------------------------------------------------------------
# Code numbering
# ---------------------------------------------------------------------------


def test_every_code_uses_known_exception(monkeypatch):
    """Defence: each code's declared exception must be one of the
    package's hierarchy roots."""
    allowed = (
        ConnInitException,
        OracleValidateException,
        OracleRuntimeException,
    )
    for code in OracleErrorCode:
        assert issubclass(code.value.exception, allowed)

from __future__ import annotations

import pytest

from tabsdatak.error import DEBUG_FRAMES_ENV_VAR, TabsdataException
from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.postgres.error import (
    PostgresRuntimeException,
    PostgresValidateException,
    PostgresErrorCode,
)


pytestmark = pytest.mark.postgres


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------


def test_validate_exception_is_a_conn_exception():
    assert issubclass(PostgresValidateException, ConnException)


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(PostgresRuntimeException, ConnException)


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
        PostgresValidateException,
        PostgresRuntimeException,
    )
    for code in PostgresErrorCode:
        assert issubclass(code.value.exception, allowed)

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.error import DEBUG_FRAMES_ENV_VAR, TabsdataException
from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.mysql.error import (
    MySQLRuntimeException,
    MySQLValidateException,
    MysqlErrorCode,
)


pytestmark = pytest.mark.mysql


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------


def test_validate_exception_is_a_conn_exception():
    assert issubclass(MySQLValidateException, ConnException)


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(MySQLRuntimeException, ConnException)


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
        MySQLValidateException,
        MySQLRuntimeException,
    )
    for code in MysqlErrorCode:
        assert issubclass(code.value.exception, allowed)

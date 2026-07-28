#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.error import TabsdataException
from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.localfile.error import (
    LocalFileErrorCode,
    LocalFileRuntimeException,
    LocalFileValidateException,
)


pytestmark = pytest.mark.localfile


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------


def test_validate_exception_is_a_conn_exception():
    assert issubclass(LocalFileValidateException, ConnException)


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(LocalFileRuntimeException, ConnException)


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
        LocalFileValidateException,
        LocalFileRuntimeException,
    )
    for code in LocalFileErrorCode:
        assert issubclass(code.value.exception, allowed)

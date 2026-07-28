#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.gcs.error import (
    GcsErrorCode,
    GcsRuntimeException,
    GcsValidateException,
)
from tabsdatak.error import TabsdataException

pytestmark = pytest.mark.gcs


def test_validate_exception_is_a_conn_exception():
    assert issubclass(GcsValidateException, ConnException)


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(GcsRuntimeException, ConnException)


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


def test_every_code_uses_known_exception():
    """Defence: each code's declared exception must be one of the package's
    hierarchy roots."""
    allowed = (
        ConnInitException,
        GcsValidateException,
        GcsRuntimeException,
    )
    for code in GcsErrorCode:
        assert issubclass(code.value.exception, allowed)

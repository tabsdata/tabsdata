#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.gcs.error import (
    GCSErrorCode,
    GCSRuntimeException,
    GCSValidateException,
)
from tabsdatak.error import TabsdataException

pytestmark = pytest.mark.gcs


def test_validate_exception_is_a_conn_exception():
    assert issubclass(GCSValidateException, ConnException)


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(GCSRuntimeException, ConnException)


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


def test_every_code_uses_known_exception():
    """Defence: each code's declared exception must be one of the package's
    hierarchy roots."""
    allowed = (
        ConnInitException,
        GCSValidateException,
        GCSRuntimeException,
    )
    for code in GCSErrorCode:
        assert issubclass(code.value.exception, allowed)

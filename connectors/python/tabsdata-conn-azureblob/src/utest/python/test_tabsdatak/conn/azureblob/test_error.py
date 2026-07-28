#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.azureblob.error import (
    AzureBlobErrorCode,
    AzureBlobRuntimeException,
    AzureBlobValidateException,
)
from tabsdatak.error import TabsdataException

pytestmark = pytest.mark.azureblob


def test_validate_exception_is_a_conn_exception():
    assert issubclass(AzureBlobValidateException, ConnException)


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(AzureBlobRuntimeException, ConnException)


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


def test_every_code_uses_known_exception():
    """Defence: each code's declared exception must be one of the package's
    hierarchy roots."""
    allowed = (
        ConnInitException,
        AzureBlobValidateException,
        AzureBlobRuntimeException,
    )
    for code in AzureBlobErrorCode:
        assert issubclass(code.value.exception, allowed)

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.druid.error import (
    DruidErrorCode,
    DruidRuntimeException,
)

from tabsdatak.error import TabsdataException

pytestmark = pytest.mark.druid



def test_runtime_exception_is_a_conn_exception():
    assert issubclass(DruidRuntimeException, ConnException)


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


def test_every_code_uses_known_exception():
    """Defence: each code's declared exception must be one of the package's
    hierarchy roots."""
    allowed = (
        ConnInitException,
        DruidRuntimeException,
    )
    for code in DruidErrorCode:
        assert issubclass(code.value.exception, allowed)

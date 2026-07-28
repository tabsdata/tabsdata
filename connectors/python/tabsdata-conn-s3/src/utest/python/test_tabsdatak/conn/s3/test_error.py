#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.s3.error import (
    S3ErrorCode,
    S3RuntimeException,
    S3ValidateException,
)
from tabsdatak.error import TabsdataException

pytestmark = pytest.mark.s3


def test_validate_exception_is_a_conn_exception():
    assert issubclass(S3ValidateException, ConnException)


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(S3RuntimeException, ConnException)


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


def test_every_code_uses_known_exception():
    """Defence: each code's declared exception must be one of the package's
    hierarchy roots."""
    allowed = (
        ConnInitException,
        S3ValidateException,
        S3RuntimeException,
    )
    for code in S3ErrorCode:
        assert issubclass(code.value.exception, allowed)

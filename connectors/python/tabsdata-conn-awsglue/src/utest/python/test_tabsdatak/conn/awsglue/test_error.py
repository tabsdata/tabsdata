#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.awsglue.error import AwsGlueErrorCode, AwsGlueRuntimeException
from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.error import TabsdataException

pytestmark = pytest.mark.conn_awsglue


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(AwsGlueRuntimeException, ConnException)


def test_conn_exception_is_a_tabsdata_exception():
    assert issubclass(ConnException, TabsdataException)


def test_every_code_uses_known_exception():
    allowed = (ConnInitException, AwsGlueRuntimeException)
    for code in AwsGlueErrorCode:
        assert issubclass(code.value.exception, allowed)

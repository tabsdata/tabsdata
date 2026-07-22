#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

import pytest

from tabsdatak.conn.common.error import ConnInitException
# noinspection PyProtectedMember
from tabsdatak.conn.druid import _validation
from tabsdatak.conn.druid.error import DruidErrorCode


pytestmark = pytest.mark.druid


# ---------------------------------------------------------------------------
# Datasource
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["events", "metrics_v1", pytest.param("a" * 128, id="a128")])
def test_validate_datasource_accepts_valid(value):
    assert _validation._validate_datasource(value) == value


@pytest.mark.parametrize("value", ["", pytest.param("a" * 129, id="a129")])
def test_validate_datasource_rejects_bad_length(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_datasource(value)
    assert ei.value.api_error is DruidErrorCode.DRUID_3


@pytest.mark.parametrize("value", [".hidden", "_hidden"])
def test_validate_datasource_rejects_leading_dot_or_underscore(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_datasource(value)
    assert ei.value.api_error is DruidErrorCode.DRUID_4


@pytest.mark.parametrize("value", ["bad/name", "bad\\name", "bad\x00name"])
def test_validate_datasource_rejects_forbidden_char(value):
    with pytest.raises(ConnInitException) as ei:
        _validation._validate_datasource(value)
    assert ei.value.api_error is DruidErrorCode.DRUID_5


# ---------------------------------------------------------------------------
# Broker URL pattern
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        "http://druid.example.com:8081",
        "https://druid.example.com",
        "https://druid.example.com/path/here",
        "HTTPS://DRUID.EXAMPLE.COM",
    ],
)
def test_druid_url_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.URL_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "druid.example.com",
        "ftp://druid.example.com",
        "http://",
        "http:// with space",
    ],
)
def test_druid_url_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.URL_PATTERN, value) is None

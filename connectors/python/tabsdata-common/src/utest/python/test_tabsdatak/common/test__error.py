from __future__ import annotations

from tabsdatak.error import DEBUG_FRAMES_ENV_VAR, TabsdataException
from tabsdatak.common._error import (
    CommonErrorCode,
    CommonException,
    CommonRuntimeException,
)


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------


def test_common_exception_is_a_tabsdata_exception():
    assert issubclass(CommonException, TabsdataException)


def test_common_runtime_exception_is_a_common_exception():
    assert issubclass(CommonRuntimeException, CommonException)


# ---------------------------------------------------------------------------
# Error codes
# ---------------------------------------------------------------------------


def test_every_code_constructs_a_common_runtime_exception(monkeypatch):
    monkeypatch.setenv(DEBUG_FRAMES_ENV_VAR, "0")
    for code in CommonErrorCode:
        exc = code.exception()
        # All current codes in CommonErrorCode are JSONPath parser /
        # secret-hydration / bundle-production failures tagged with
        # CommonRuntimeException.
        assert isinstance(exc, CommonRuntimeException)
        assert type(exc) is code.value.exception


def test_codes_have_expected_names():
    names = [code.name for code in CommonErrorCode]
    assert names == [
        "COMMON_1",
        "COMMON_2",
        "COMMON_3",
        "COMMON_4",
        "COMMON_5",
        "COMMON_6",
        "COMMON_7",
        "COMMON_8",
        "COMMON_9",
        "COMMON_10",
        "COMMON_11",
        "COMMON_12",
        "COMMON_13",
    ]

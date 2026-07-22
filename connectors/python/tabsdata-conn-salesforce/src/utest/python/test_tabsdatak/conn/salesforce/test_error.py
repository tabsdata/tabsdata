#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.conn.common.error import ConnException, ConnInitException
from tabsdatak.conn.salesforce.error import (
    SalesforceErrorCode,
    SalesforceRuntimeException,
    SalesforceValidateException,
)
from tabsdatak.error import TabsdataException


pytestmark = pytest.mark.salesforce


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------


def test_validate_exception_is_a_conn_exception():
    assert issubclass(SalesforceValidateException, ConnException)


def test_runtime_exception_is_a_conn_exception():
    assert issubclass(SalesforceRuntimeException, ConnException)


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
        SalesforceValidateException,
        SalesforceRuntimeException,
    )
    for code in SalesforceErrorCode:
        assert issubclass(code.value.exception, allowed)


def test_codes_are_consecutively_numbered():
    """Defence: each code must end with the (1-based) integer index."""
    members = list(SalesforceErrorCode)
    for idx, member in enumerate(members, start=1):
        assert member.name == f"SALESFORCE_{idx}", (
            f"expected SALESFORCE_{idx}, got {member.name}"
        )

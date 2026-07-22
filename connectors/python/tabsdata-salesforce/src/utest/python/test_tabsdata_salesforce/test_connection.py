#
# Copyright 2026 Tabsdata Inc.
#

import pytest

import tabsdata as td
from tabsdata.connector.salesforce._connection import CATEGORIES, SalesforceConn
from tabsdata.exceptions import ConnectionConfigurationError, ErrorCode
from test_tabsdata_salesforce.conftest import FAKE_CREDENTIALS


@pytest.mark.parametrize("category", list(CATEGORIES.keys()))
def test_valid_category_accepted(category):
    conn = SalesforceConn(credentials=FAKE_CREDENTIALS, category=category)
    assert conn.category == category
    assert conn.objects == CATEGORIES[category]


def test_invalid_category_raises_cxce13():
    with pytest.raises(ConnectionConfigurationError) as exc_info:
        SalesforceConn(credentials=FAKE_CREDENTIALS, category="not_a_category")
    assert exc_info.value.code == ErrorCode.CXCE13.value["code"]


def test_invalid_category_via_setter_raises_cxce13():
    conn = SalesforceConn(credentials=FAKE_CREDENTIALS, category="sales_crm")
    with pytest.raises(ConnectionConfigurationError) as exc_info:
        conn.category = "bogus"
    assert exc_info.value.code == ErrorCode.CXCE13.value["code"]


def test_objects_only_allows_none_category():
    conn = SalesforceConn(credentials=FAKE_CREDENTIALS, objects=["Account"])
    assert conn.category is None
    assert conn.objects == ["Account"]


def test_objects_resolves_to_category_objects():
    conn = SalesforceConn(credentials=FAKE_CREDENTIALS, category="sales_crm")
    assert conn.objects == CATEGORIES["sales_crm"]


def test_objects_follows_category_change_via_setter():
    conn = SalesforceConn(credentials=FAKE_CREDENTIALS, category="sales_crm")
    conn.category = "service"
    assert conn.objects == CATEGORIES["service"]

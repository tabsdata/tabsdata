#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.awsglue import _validation

pytestmark = pytest.mark.conn_awsglue


@pytest.mark.parametrize("value", ["sales", "sales_2026", "s", "9lives", "a" * 255])
def test_database_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.DATABASE_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value", ["", "Sales", "sales-eu", "sales.eu", "sales eu", "a" * 256]
)
def test_database_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.DATABASE_PATTERN, value) is None


@pytest.mark.parametrize(
    "value", ["orders", "sales.orders", "sales_2026.orders_v2", "a" * 255 + ".b"]
)
def test_table_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.TABLE_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value",
    [
        "",
        "Orders",
        "sales.Orders",
        "catalog.sales.orders",
        "sales.",
        ".orders",
        "sales..orders",
        "`sales`.orders",
        '"orders"',
        "my-orders",
    ],
)
def test_table_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.TABLE_PATTERN, value) is None


@pytest.mark.parametrize("value", ["123456789012", "000000000000"])
def test_catalog_id_pattern_accepts_valid(value):
    assert re.fullmatch(_validation.CATALOG_ID_PATTERN, value) is not None


@pytest.mark.parametrize(
    "value", ["", "12345678901", "1234567890123", "12345678901a", "1234-5678-9012"]
)
def test_catalog_id_pattern_rejects_invalid(value):
    assert re.fullmatch(_validation.CATALOG_ID_PATTERN, value) is None


def test_table_parts_splits_a_qualified_name():
    assert _validation.table_parts("sales.orders") == ("sales", "orders")


def test_table_parts_leaves_the_database_out_of_a_bare_name():
    assert _validation.table_parts("orders") == (None, "orders")

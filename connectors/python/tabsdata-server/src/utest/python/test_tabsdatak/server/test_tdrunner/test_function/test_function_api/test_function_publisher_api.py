#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

from tabsdatak.conn.localfile import LocalFileSrcConn

from .conftest import RESOURCES
from .driver import PUBLISHER, Function, copy_project, forget
from .golden import check_system_columns, compare, snapshot

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

FIRST_ORDERS = pl.DataFrame({
    "order_id": [1, 2, 3],
    "customer_id": [10, 20, 10],
    "amount": [5.5, 12.0, 30.25],
})
SECOND_ORDERS = pl.DataFrame({
    "order_id": [4, 5],
    "customer_id": [30, 20],
    "amount": [8.0, 99.9],
})


@pytest.fixture
def publisher_function(tmp_path, monkeypatch):
    project = copy_project(RESOURCES, tmp_path, monkeypatch, "publisher_project")
    yield Function(
        PUBLISHER, project, "function_api_publisher", "publisher", "publish_orders"
    )
    forget("function_api_publisher")


def test_function_publisher_api(driver, publisher_function):
    run = driver.run(
        publisher_function,
        outputs=["orders_a", "orders_b", "untouched"],
        source_files={
            "orders_1.parquet": FIRST_ORDERS,
            "orders_2.parquet": SECOND_ORDERS,
        },
        connection=lambda base_path: LocalFileSrcConn(
            base_path=str(base_path)
        ).to_dict(),
    )
    check_system_columns(run)
    compare("publisher", snapshot(run))

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

from tabsdatak.conn.localfile import LocalFileDestConn

from .conftest import RESOURCES
from .driver import SUBSCRIBER, Function, copy_project, forget
from .golden import compare, snapshot

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

ORDERS = pl.DataFrame({
    "order_id": [1, 2, 3, 4],
    "customer_id": [10, 20, 10, 30],
    "amount": [5.5, 12.0, 30.25, 8.0],
})


@pytest.fixture
def subscriber_function(tmp_path, monkeypatch):
    project = copy_project(RESOURCES, tmp_path, monkeypatch, "subscriber_project")
    yield Function(
        SUBSCRIBER, project, "function_api_subscriber", "subscriber", "deliver_orders"
    )
    forget("function_api_subscriber")


def test_function_subscriber_api(driver, subscriber_function):
    run = driver.run(
        subscriber_function,
        outputs=[],
        inputs={"orders": ORDERS},
        connection=lambda base_path: LocalFileDestConn(
            base_path=str(base_path)
        ).to_dict(),
    )
    compare("subscriber", snapshot(run))

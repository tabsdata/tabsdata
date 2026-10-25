#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import polars as pl
import pytest

from .conftest import RESOURCES
from .driver import TRANSFORMER, Function, copy_project, forget
from .golden import check_system_columns, compare, snapshot

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

ORDERS = pl.DataFrame({
    "order_id": [1, 2, 3, 4],
    "customer_id": [10, 20, 10, 30],
    "amount": [5.5, 12.0, 30.25, 8.0],
})
CUSTOMERS = pl.DataFrame({
    "customer_id": [10, 20, 40],
    "customer": ["acme", "globex", "initech"],
})


@pytest.fixture
def transformer_function(tmp_path, monkeypatch):
    project = copy_project(RESOURCES, tmp_path, monkeypatch, "transformer_project")
    yield Function(
        TRANSFORMER,
        project,
        "function_api_transformer",
        "transformer",
        "enrich_orders",
    )
    forget("function_api_transformer")


def test_function_transformer_api(driver, transformer_function):
    run = driver.run(
        transformer_function,
        outputs=["enriched", "untouched"],
        inputs={"orders": ORDERS, "customers": CUSTOMERS},
    )
    check_system_columns(run)
    compare("transformer", snapshot(run))

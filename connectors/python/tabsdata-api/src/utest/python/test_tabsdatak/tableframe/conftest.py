#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from test_tabsdatak.tableframe.support import datasets
from test_tabsdatak.tableframe.support.frames import DataSet


@pytest.fixture
def simple_dataset() -> DataSet:
    return datasets.simple_dataset()


@pytest.fixture
def small_dataset() -> DataSet:
    return datasets.small_dataset()


@pytest.fixture(scope="session")
def large_dataset() -> DataSet:
    return datasets.large_dataset()

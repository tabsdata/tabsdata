#
# Copyright 2026 Tabsdata Inc.
#

import os
from pathlib import Path

import pytest

FOLDER = Path(__file__).parent


def record(group: str) -> None:
    with (FOLDER / f"{group}.log").open("a") as log:
        log.write(os.environ["PYTEST_XDIST_WORKER"] + "\n")


@pytest.mark.xdist_group("first")
@pytest.mark.parametrize("index", range(8))
def test_first_records_its_worker(index):
    record("first")


@pytest.mark.xdist_group("second")
@pytest.mark.parametrize("index", range(8))
def test_second_records_its_worker(index):
    record("second")

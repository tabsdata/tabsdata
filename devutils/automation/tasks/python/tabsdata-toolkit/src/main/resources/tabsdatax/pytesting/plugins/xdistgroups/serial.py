#
# Copyright 2026 Tabsdata Inc.
#

import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.xdist_group("serial")

LOG = Path(__file__).with_suffix(".log")


@pytest.mark.parametrize("index", range(8))
def test_records_its_worker(index):
    with LOG.open("a") as log:
        log.write(os.environ["PYTEST_XDIST_WORKER"] + "\n")

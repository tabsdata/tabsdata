#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import os
import sys

import pytest
from xdist.workermanage import WorkerController

from testi_tabsdatak.bootest import check_assets, enrich_sys_path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logging.getLogger("snowflake.connector.network").setLevel(logging.CRITICAL)

enrich_sys_path()
check_assets()

os.environ["POLARS_VERBOSE"] = "0"
os.environ["POLARS_FORCE_ASYNC"] = "1"

TESTS_ROOT_FOLDER = os.path.dirname(__file__)
sys.path.insert(0, TESTS_ROOT_FOLDER)


def pytest_addoption(parser):
    # Guarded against ValueError so multiple suites can register the same
    # option without colliding when run in parallel.
    try:
        parser.addoption(
            "--performance-size",
            help="Amount of records per table when running performance tests",
            default=25000,
            type=int,
        )
    except ValueError:
        pass


def pytest_generate_tests(metafunc):
    if "size" in metafunc.fixturenames:
        metafunc.parametrize("size", [metafunc.config.getoption("performance_size")])


def clean_python_virtual_environments(): ...


def setup_temp_folder(config: pytest.Config) -> None:
    from tabsdatak.common._temps import tabsdata_temp_folder

    if getattr(config.option, "basetemp", None):
        return
    config.option.basetemp = tabsdata_temp_folder()


def setup_temp_folder_node(node: WorkerController) -> None:
    from tabsdatak.common._temps import tabsdata_temp_folder

    if not getattr(node.config.option, "basetemp", None):
        base_temp = tabsdata_temp_folder()
        worker_temp = os.path.join(base_temp, f"pyw_{node.workerinput['workerid']}")
        node.config.option.basetemp = worker_temp

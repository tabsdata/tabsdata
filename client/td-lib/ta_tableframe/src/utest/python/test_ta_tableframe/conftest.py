#
#  Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from tests_tabsdata.bootest import TESTING_RESOURCES_PATH, enrich_sys_path


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

import pytest
from xdist.workermanage import WorkerController

from tabsdata._utils.tdlogging import setup_tests_logging
from tests_tabsdata.conftest import setup_temp_folder, setup_temp_folder_node

logger = logging.getLogger(__name__)


def pytest_configure(config: pytest.Config):
    setup_tests_logging()
    if not hasattr(config, "workerinput"):
        setup_temp_folder(config)


def pytest_configure_node(node: WorkerController):
    setup_temp_folder_node(node)

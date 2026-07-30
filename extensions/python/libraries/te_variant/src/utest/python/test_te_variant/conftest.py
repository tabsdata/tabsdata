#
# Copyright 2025 Tabsdata Inc.
#

from __future__ import annotations

import logging

# import sys

# from test_te_variant.bootest import TESTS_ROOT_FOLDER

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdatak.bootest import enrich_sys_path
from test_te_variant.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

# sys.path.insert(0, os.path.join(TESTS_ROOT_FOLDER, ".."))

import pytest
from xdist.workermanage import WorkerController

# noinspection PyProtectedMember
from test_tabsdatak._testing import setup_tests_logging
from test_tabsdata.conftest import setup_temp_folder, setup_temp_folder_node


def pytest_configure(config: pytest.Config):
    setup_tests_logging()
    if not hasattr(config, "workerinput"):
        setup_temp_folder(config)


def pytest_configure_node(node: WorkerController):
    setup_temp_folder_node(node)

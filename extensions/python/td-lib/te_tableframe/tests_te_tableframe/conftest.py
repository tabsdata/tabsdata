#
#  Copyright 2025 Tabs Data Inc.
#

import logging

from tests_tabsdata.bootest import TESTING_RESOURCES_PATH, enrich_sys_path

from tabsdata._utils.logging import setup_tests_logging

logger = logging.getLogger(__name__)


def pytest_configure():
    setup_tests_logging()


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

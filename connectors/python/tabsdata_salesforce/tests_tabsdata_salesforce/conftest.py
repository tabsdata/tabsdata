#
# Copyright 2025 Tabs Data Inc.
#

from tests_tabsdata.bootest import enrich_sys_path
from tests_tabsdata_salesforce.bootest import TESTING_RESOURCES_PATH

TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()

from tests_tabsdata.conftest import clean_python_virtual_environments


def pytest_sessionfinish(session, exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_python_virtual_environments()

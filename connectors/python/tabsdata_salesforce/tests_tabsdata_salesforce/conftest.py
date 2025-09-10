#
# Copyright 2025 Tabs Data Inc.
#

import logging

import pytest
from tests_tabsdata_salesforce.bootest import TESTING_RESOURCES_PATH
from xdist.workermanage import WorkerController

from tabsdata._utils.logging import setup_tests_logging
from tests_tabsdata.bootest import enrich_sys_path

TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()

import tabsdata as td
from tests_tabsdata.conftest import (
    clean_python_virtual_environments,
    setup_temp_folder,
    setup_temp_folder_node,
)

logger = logging.getLogger(__name__)

FAKE_CREDENTIALS = td.SalesforceTokenCredentials(
    username="username",
    password="password",
    security_token="security_token",
)

CORRECT_CREDENTIALS = td.SalesforceTokenCredentials(
    username=td.EnvironmentSecret("SALESFORCE_USERNAME"),
    password=td.EnvironmentSecret("SALESFORCE_PASSWORD"),
    security_token=td.EnvironmentSecret("SALESFORCE_SECURITY_TOKEN"),
)


def pytest_configure(config: pytest.Config):
    setup_tests_logging()
    if not hasattr(config, "workerinput"):
        setup_temp_folder(config)


def pytest_configure_node(node: WorkerController):
    setup_temp_folder_node(node)


# noinspection PyUnusedLocal
def pytest_sessionfinish(session, exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_python_virtual_environments()

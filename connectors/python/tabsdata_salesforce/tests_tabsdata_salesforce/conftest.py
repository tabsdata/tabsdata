#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from tests_tabsdata.bootest import enrich_sys_path
from tests_tabsdata_salesforce.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

import os

import pytest
from xdist.workermanage import WorkerController

import tabsdata as td
from tabsdata._utils.tdlogging import setup_tests_logging
from tests_tabsdata.conftest import (
    clean_python_virtual_environments,
    pytest_addoption,
    pytest_generate_tests,
    setup_temp_folder,
    setup_temp_folder_node,
)

FAKE_CREDENTIALS = td.SalesforceTokenCredentials(
    username="username",
    password="password",
    security_token="security_token",
)


@pytest.fixture(scope="session")
def sf_config():
    config: dict[str, Any] = {
        "USERNAME_ENV": "SF0__USERNAME",
        "PASSWORD_ENV": "SF0__PASSWORD",
        "SECURITY_TOKEN_ENV": "SF0__SECURITY_TOKEN",
    }
    username = os.environ.get(config["USERNAME_ENV"])
    password = os.environ.get(config["PASSWORD_ENV"])
    security_token = os.environ.get(config["SECURITY_TOKEN_ENV"])
    if not username or not password or not security_token:
        raise Exception(
            f"The environment variables {config['USERNAME_ENV']}, "
            f"{config['PASSWORD_ENV']}, and/or "
            f"{config['SECURITY_TOKEN_ENV']} are not set. Unable to run tests "
            "using the 'sf_config' fixture."
        )
    config["CREDENTIALS"] = td.SalesforceTokenCredentials(
        username=td.EnvironmentSecret(config["USERNAME_ENV"]),
        password=td.EnvironmentSecret(config["PASSWORD_ENV"]),
        security_token=td.EnvironmentSecret(config["SECURITY_TOKEN_ENV"]),
    )
    yield config


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

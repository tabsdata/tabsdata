#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdata.bootest import enrich_sys_path
from test_tabsdata_snowflake.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

import pytest
from xdist.workermanage import WorkerController

from tabsdata._utils.tdlogging import setup_tests_logging
from test_tabsdata.conftest import (
    clean_everything,
    clean_python_virtual_environments,
    pytest_addoption,
    pytest_generate_tests,
    setup_temp_folder,
    setup_temp_folder_node,
)

pytest_plugins = ["test_tabsdata.fntest"]


def pytest_configure(config: pytest.Config):
    setup_tests_logging()
    if not hasattr(config, "workerinput"):
        setup_temp_folder(config)
        clean_everything()


def pytest_configure_node(node: WorkerController):
    setup_temp_folder_node(node)


# noinspection PyUnusedLocal
def pytest_sessionfinish(session, exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_everything()


@pytest.fixture(scope="session")
def snowflake_config():
    import os  # Lazy import

    import tabsdata as td  # Lazy import

    config: dict[str, Any] = {
        "ACCOUNT_ENV": "SNW__ACCOUNT",
        "USER_ENV": "SNW__USER",
        "PAT_ENV": "SNW__PAT",
        "ROLE_ENV": "SNW__ROLE",
        "DATABASE_ENV": "SNW__DATABASE",
        "SCHEMA_ENV": "SNW__SCHEMA",
        "WAREHOUSE_ENV": "SNW__WAREHOUSE",
    }
    missing_vars = []
    for var in config.values():
        if not os.environ.get(var):
            missing_vars.append(var)
    if missing_vars:
        raise Exception(
            f"The environment variables {', '.join(missing_vars)} are not set. "
            "Unable to run tests using the 'snowflake_config' fixture."
        )

    config["CONNECTION_PARAMETERS"] = {
        "account": td.EnvironmentSecret(config["ACCOUNT_ENV"]),
        "user": td.EnvironmentSecret(config["USER_ENV"]),
        "password": td.EnvironmentSecret(config["PAT_ENV"]),
        "role": td.EnvironmentSecret(config["ROLE_ENV"]),
        "database": td.EnvironmentSecret(config["DATABASE_ENV"]),
        "schema": td.EnvironmentSecret(config["SCHEMA_ENV"]),
        "warehouse": td.EnvironmentSecret(config["WAREHOUSE_ENV"]),
    }

    yield config


@pytest.fixture(scope="session")
def snowflake_connection(snowflake_config):
    from snowflake.connector import connect  # Lazy import

    from tabsdata._secret import _recursively_evaluate_secret  # Lazy import

    conn = connect(
        **_recursively_evaluate_secret(snowflake_config["CONNECTION_PARAMETERS"])
    )
    try:
        yield conn
    finally:
        conn.close()

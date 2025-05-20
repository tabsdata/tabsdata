#
# Copyright 2025 Tabs Data Inc.
#

from tests_tabsdata.bootest import enrich_sys_path
from tests_tabsdata_snowflake.bootest import TESTING_RESOURCES_PATH

TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()

import logging
import os

import pytest
from snowflake.connector import connect
from tests_tabsdata.conftest import (
    clean_python_virtual_environments,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger("filelock").setLevel(logging.INFO)


def pytest_sessionfinish(session, exitstatus):
    # Based on the following discussion:
    # https://github.com/pytest-dev/pytest-xdist/issues/271
    if getattr(session.config, "workerinput", None) is not None:
        # No need to download, the master process has already done that.
        return
    clean_python_virtual_environments()


@pytest.fixture(scope="session")
def snowflake_connection():
    connection_parameters = {
        "account": os.environ.get("TD_SNOWFLAKE_ACCOUNT"),
        "user": os.environ.get("TD_SNOWFLAKE_USER"),
        "password": os.environ.get("TD_SNOWFLAKE_PASSWORD"),
        "role": "SYSADMIN",
        "database": "TESTING_DB",
        "schema": "PUBLIC",
        "warehouse": "SNOWFLAKE_LEARNING_WH",
    }
    conn = connect(**connection_parameters)
    try:
        yield conn
    finally:
        conn.close()

#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdata.bootest import enrich_sys_path
from test_tabsdata_salesforce.bootest import TESTING_RESOURCES_PATH


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


def _create_fake_credentials():
    """Create fake credentials with lazy import"""
    import tabsdata as td  # Lazy import

    return td.SalesforceTokenCredentials(
        username="username",
        password="password",
        security_token="security_token",
    )


FAKE_CREDENTIALS = _create_fake_credentials()


@pytest.fixture(scope="session")
def sf_config():
    import os  # Lazy import

    import tabsdata as td  # Lazy import

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


SFR_TESTING_REPORT_NAME = "Foo_for_dev_ci_testing"
SFR_INITIAL_VALUES_TESTING_REPORT_NAME = "users_dev_ci"


def _chunk_source(source, tmp_dir):
    import os

    import polars as pl

    from test_tabsdata.conftest import clean_polars_df

    [result] = source.chunk(str(tmp_dir))
    if result is None:
        return pl.DataFrame()
    path = os.path.join(str(tmp_dir), result)
    df = pl.read_parquet(path)
    return clean_polars_df(df)


@pytest.fixture(scope="session")
def sf_expected_report_basic(sf_config, tmp_path_factory):
    import tabsdata as td

    tmp = tmp_path_factory.mktemp("sf_report_basic")
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_TESTING_REPORT_NAME,
        column_name_strategy="columnName",
    )
    return _chunk_source(source, tmp)


@pytest.fixture(scope="session")
def sf_expected_contacts(sf_config, tmp_path_factory):
    import os

    import polars as pl
    import tabsdata as td

    from test_tabsdata.conftest import clean_polars_df

    tmp = tmp_path_factory.mktemp("sf_contacts")
    source = td.SalesforceSource(
        sf_config["CREDENTIALS"],
        query="SELECT Name FROM Contact",
    )
    [result] = source.chunk(str(tmp))
    if result is None:
        pytest.fail("Salesforce returned no Contact data — seed contacts first")
    path = os.path.join(str(tmp), result)
    df = pl.read_parquet(path)
    df = df.drop_nulls()
    return clean_polars_df(df)


@pytest.fixture(scope="session")
def sf_expected_contacts_with_modstamp(sf_config, tmp_path_factory):
    import os

    import polars as pl
    import tabsdata as td

    from test_tabsdata.conftest import clean_polars_df

    tmp = tmp_path_factory.mktemp("sf_contacts_modstamp")
    source = td.SalesforceSource(
        sf_config["CREDENTIALS"],
        query=(
            "SELECT Name,SystemModstamp FROM Contact "
            "WHERE SystemModstamp > $lastModified"
        ),
        initial_last_modified="2024-03-10T11:03:08.000+0000",
    )
    [result] = source.chunk(str(tmp))
    if result is None:
        pytest.fail("Salesforce returned no Contact data — seed contacts first")
    path = os.path.join(str(tmp), result)
    df = pl.read_parquet(path)
    df = df.drop_nulls()
    return clean_polars_df(df)


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

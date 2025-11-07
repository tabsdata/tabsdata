#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging

import tabsdata

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from tests_tabsdata.bootest import enrich_sys_path
from tests_tabsdata_bigquery.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

import json
import os

import pytest
from google.cloud import bigquery
from xdist.workermanage import WorkerController

from tabsdata._utils.tdlogging import setup_tests_logging

# noinspection PyUnusedImports
from tests_tabsdata.conftest import (
    clean_python_virtual_environments,
    pytest_addoption,
    pytest_generate_tests,
    setup_temp_folder,
    setup_temp_folder_node,
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


@pytest.fixture(scope="session")
def bigquery_client(bigquery_config: dict):
    service_account_key = os.environ.get(bigquery_config["ENV"])
    if not service_account_key:
        raise Exception(
            "The environment variable "
            f"{bigquery_config['ENV']} is not set. Unable to run "
            "tests using the 'bigquery_client' fixture."
        )
    serv_acc_dict = json.loads(service_account_key)
    bg_client = bigquery.Client.from_service_account_info(serv_acc_dict)
    yield bg_client


@pytest.fixture(scope="session")
def bigquery_config():
    # Note: this is currently very simple, but if needed it can be extended. This
    # dictionary will provide all the pieces necessary for testing against BigQuery

    config = {
        "ENV": "GCP0__GCP_SERVICE_ACCOUNT_KEY",
        "URI_ENV": "GCP0__GCP_STORAGE_URI",
        "PROJECT_ENV": "GCP0__BIGQUERY_PROJECT",
        "DATASET_ENV": "GCP0__BIGQUERY_DATASET",
    }
    uri = os.environ.get(config["URI_ENV"])
    if not uri:
        raise Exception(
            f"The environment variable {config['URI_ENV']} is not set. Unable to run "
            "tests using the 'bigquery_config' fixture."
        )
    bucket_name = uri.replace("gs://", "").split("/", 1)[0]
    config["BUCKET"] = bucket_name
    project = os.environ.get(config["PROJECT_ENV"])
    if not project:
        raise Exception(
            f"The environment variable {config['PROJECT_ENV']} is not set. Unable to"
            " run tests using the 'bigquery_config' fixture."
        )
    config["PROJECT"] = project
    dataset = os.environ.get(config["DATASET_ENV"])
    if not project:
        raise Exception(
            f"The environment variable {config['DATASET_ENV']} is not set. Unable to "
            "run tests using the 'bigquery_config' fixture."
        )
    config["DATASET"] = dataset
    config["GCS_FOLDER"] = f"{uri}/bigquery_staging_folder"
    config["CREDENTIALS"] = tabsdata.GCPServiceAccountKeyCredentials(
        tabsdata.EnvironmentSecret(config["ENV"])
    )
    yield config

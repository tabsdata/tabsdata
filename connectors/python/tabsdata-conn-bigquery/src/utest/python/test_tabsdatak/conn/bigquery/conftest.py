#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

from test_tabsdatak.bootest import enrich_sys_path
from test_tabsdatak.conn.bigquery.bootest import TESTING_RESOURCES_PATH


def _enrich_sys_path():
    pass


TESTING_RESOURCES_FOLDER = TESTING_RESOURCES_PATH
enrich_sys_path()
_enrich_sys_path()

import pytest
from xdist.workermanage import WorkerController

from test_tabsdatak._testing import setup_tests_logging

# noinspection PyUnusedImports
from test_tabsdata.conftest import (
    clean_everything,
    clean_python_virtual_environments,
    pytest_addoption,
    pytest_generate_tests,
    setup_temp_folder,
    setup_temp_folder_node,
)

# ``_BqConfig`` / ``_split_gs_uri`` live in a shared helper module (not
# here) so the integration test files can import them without reaching
# into this conftest -- see _integration_helpers.py.
from test_tabsdatak.conn.bigquery._integration_helpers import _BqConfig, _split_gs_uri

_PREFIX = "GCP0"

_REQUIRED_ENV = [
    f"{_PREFIX}__GCP_SERVICE_ACCOUNT_KEY",
    f"{_PREFIX}__BIGQUERY_PROJECT",
    f"{_PREFIX}__BIGQUERY_DATASET",
    f"{_PREFIX}__GCP_STORAGE_URI",
]


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
def fresh_bigquery() -> _BqConfig:
    missing = [name for name in _REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        pytest.skip(
            "BigQuery integration tests require environment variables: "
            + ", ".join(missing)
        )
    bucket, base_path = _split_gs_uri(os.environ[f"{_PREFIX}__GCP_STORAGE_URI"])
    return _BqConfig(
        sa_json=os.environ[f"{_PREFIX}__GCP_SERVICE_ACCOUNT_KEY"],
        project=os.environ[f"{_PREFIX}__BIGQUERY_PROJECT"],
        dataset=os.environ[f"{_PREFIX}__BIGQUERY_DATASET"],
        bucket=bucket,
        base_path=base_path,
    )

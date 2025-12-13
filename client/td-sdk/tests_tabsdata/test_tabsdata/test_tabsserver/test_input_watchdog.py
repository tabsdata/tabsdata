#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os
from io import StringIO
from unittest import mock

import pytest

from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._tabsserver.invoker import invoke as tabsserver_main
from tabsdata._utils.bundle_utils import create_bundle_archive
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    write_v1_listener_yaml_file,
)
from tests_tabsdata.testing_resources.test_input_watchdog.example import (
    testing_tabsdata_function as input_watchdog,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


@pytest.mark.watchdog
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_watchdog(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    testing_function = input_watchdog()
    context_archive = create_bundle_archive(
        testing_function, local_packages=LOCAL_PACKAGES_LIST, save_location=tmp_path
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    write_v1_listener_yaml_file(
        input_yaml_file,
        context_archive,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    os.makedirs(tabsserver_output_folder, exist_ok=True)
    environment_name, result = tabsserver_main(
        tmp_path,
        response_folder,
        tabsserver_output_folder,
        environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
        logs_folder=logs_folder,
        temp_cwd=True,
    )
    assert result == 0
    assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

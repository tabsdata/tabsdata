#
# Copyright 2025 Tabs Data Inc.
#

import os

import pytest

from tabsdata._tabsserver.tools.mount_extractor import resolve
from tests_tabsdata.conftest import (
    TESTING_RESOURCES_FOLDER,
)

# noinspection PyUnresolvedReferences
from .. import pytestmark  # noqa: F401


@pytest.mark.mount_extractor
@pytest.mark.unit
def test_resolve_example_yaml():
    destination_path = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "mount_extractor_resources",
        "example_input.yaml",
    )
    with open(destination_path, "r") as file:
        piped_input = file.read()

    result = resolve(piped_input)

    expected_result = {
        "TDS_AZB_AZURE_STORAGE_ACCOUNT_KEY": "MY_ACCOUNT_KEY",
        "TDS_AZB_AZURE_STORAGE_ACCOUNT_NAME": "MY_ACCOUNT_NAME",
        "TDS_S3A_AWS_ACCESS_KEY_ID": "MY_ACCESS_KEY",
        "TDS_S3A_AWS_REGION": "eu-north1",
        "TDS_S3A_AWS_SECRET_ACCESS_KEY": "MY_SECRET_KEY",
    }

    assert result == expected_result

#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
import sys
from io import StringIO
from unittest import mock

import polars as pl
import pytest
import yaml

from tabsdata._utils.bundle_utils import (
    PYTHON_IGNORE_UNAVAILABLE_PUBLIC_PACKAGES_KEY,
    PYTHON_INSTALL_DEPENDENCIES_KEY,
    PYTHON_LOCAL_PACKAGES_KEY,
    PYTHON_PUBLIC_PACKAGES_KEY,
    PYTHON_VERSION_KEY,
    create_bundle_archive,
)
from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.invoker import (
    REQUEST_FILE_NAME,
)
from tabsdata._tabsserver.invoker import invoke as tabsserver_main
from test_tabsdata.bootest import TDLOCAL_FOLDER
from test_tabsdata.common.constants import PYTEST_DEFAULT_ENVIRONMENT_PREFIX
from test_tabsdata.conftest import (
    FUNCTION_DATA_FOLDER,
    LOCAL_PACKAGES_LIST,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
    write_v2_function_yaml_file,
)
from test_tabsdata.test_custom_requirements.example import (
    custom_requirements,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_custom_requirements(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, "test_custom_requirements")
    correct_requirements = {
        PYTHON_LOCAL_PACKAGES_KEY: LOCAL_PACKAGES_LIST,
        PYTHON_VERSION_KEY: f"{sys.version_info.major}.{sys.version_info.minor}",
        PYTHON_PUBLIC_PACKAGES_KEY: [
            "archspec==0.2.5",
            "colorama==0.4.6",
            "psutil==7.2.2",
            "py-cpuinfo==9.0.0",
            "PyYAML==6.0.3",
            "types-PyYAML==6.0.3",
            "setuptools==83.0.0",
            "tqdm==4.67.3",
            "tzlocal==5.3.1",
            "wheel==0.47.0",
            "pandas==2.2.3",
            "simple-salesforce==1.12.6",
            "tabsdata",
            "tabsdata-toolkit"],
        PYTHON_IGNORE_UNAVAILABLE_PUBLIC_PACKAGES_KEY: True,
    }
    correct_custom_requirements_path = os.path.join(
        tmp_path, "correct_custom_requirements.yaml"
    )
    with open(correct_custom_requirements_path, "w") as f:
        yaml.dump(correct_requirements, f)
    context_archive = create_bundle_archive(
        custom_requirements,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
        requirements=correct_custom_requirements_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, "response_folder")
    os.makedirs(response_folder, exist_ok=True)
    output1_path = os.path.join(tmp_path, "output1.parquet")
    output2_path = os.path.join(tmp_path, "output2.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output1_path, output2_path],
        function_data_path=function_data_folder,
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

    assert os.path.isfile(output1_path)
    output = pl.read_parquet(output1_path)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_custom_requirements",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output2_path)
    output = pl.read_parquet(output2_path)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_custom_requirements",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_custom_requirements_no_package_version(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, "test_custom_requirements_no_package_version")
    correct_requirements = {
        PYTHON_LOCAL_PACKAGES_KEY: LOCAL_PACKAGES_LIST,
        PYTHON_VERSION_KEY: f"{sys.version_info.major}.{sys.version_info.minor}",
        PYTHON_PUBLIC_PACKAGES_KEY: [
            "archspec==0.2.5",
            "colorama==0.4.6",
            "psutil==7.2.2",
            "py-cpuinfo==9.0.0",
            "PyYAML==6.0.3",
            "types-PyYAML==6.0.3",
            "setuptools==83.0.0",
            "tqdm==4.67.3",
            "tzlocal==5.3.1",
            "wheel==0.47.0",
            "pandas",
            "simple-salesforce==1.12.6",
            "tabsdata",
            "tabsdata-toolkit"],
        PYTHON_IGNORE_UNAVAILABLE_PUBLIC_PACKAGES_KEY: True,
    }
    correct_custom_requirements_path = os.path.join(
        tmp_path, "correct_custom_requirements.yaml"
    )
    with open(correct_custom_requirements_path, "w") as f:
        yaml.dump(correct_requirements, f)
    context_archive = create_bundle_archive(
        custom_requirements,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
        requirements=correct_custom_requirements_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, "response_folder")
    os.makedirs(response_folder, exist_ok=True)
    output_file1 = os.path.join(tmp_path, "output1.parquet")
    output_file2 = os.path.join(tmp_path, "output2.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file1, output_file2],
        function_data_path=function_data_folder,
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

    assert os.path.isfile(output_file1)
    output = pl.read_parquet(output_file1)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_custom_requirements",
        "expected_result1.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    assert os.path.isfile(output_file2)
    output = pl.read_parquet(output_file2)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_custom_requirements",
        "expected_result2.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_custom_requirements_no_dependencies_raises_exception(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, "test_custom_requirements_no_dependencies_raises_exception")
    correct_requirements = {
        PYTHON_LOCAL_PACKAGES_KEY: LOCAL_PACKAGES_LIST,
        PYTHON_VERSION_KEY: f"{sys.version_info.major}.{sys.version_info.minor}",
        PYTHON_PUBLIC_PACKAGES_KEY: ["pandas==2.2.3", "simple-salesforce==1.12.6"],
        PYTHON_INSTALL_DEPENDENCIES_KEY: False,
        PYTHON_IGNORE_UNAVAILABLE_PUBLIC_PACKAGES_KEY: True,
    }
    correct_custom_requirements_path = os.path.join(
        tmp_path, "correct_custom_requirements.yaml"
    )
    with open(correct_custom_requirements_path, "w") as f:
        yaml.dump(correct_requirements, f)
    context_archive = create_bundle_archive(
        custom_requirements,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
        requirements=correct_custom_requirements_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, "response_folder")
    os.makedirs(response_folder, exist_ok=True)
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_function_yaml_file(
        input_yaml_file,
        context_archive,
        function_data_path=function_data_folder,
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
    assert result != 0

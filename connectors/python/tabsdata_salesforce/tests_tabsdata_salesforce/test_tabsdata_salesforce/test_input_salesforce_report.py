#
# Copyright 2025 Tabs Data Inc.
#

import copy
import inspect
import logging
import os
from io import StringIO
from unittest import mock

import polars as pl

# noinspection PyPackageRequirements
import pytest
from tests_tabsdata_salesforce.conftest import (
    FAKE_CREDENTIALS,
    TESTING_RESOURCES_FOLDER,
)
from tests_tabsdata_salesforce.testing_resources.test_input_salesforce_report.example import (
    input_salesforce_report,
)
from tests_tabsdata_salesforce.testing_resources.test_input_salesforce_report_initial_values.example import (
    input_salesforce_report_initial_values,
)

import tabsdata as td
from tabsdata._secret import DirectSecret
from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._tabsserver.invoker import invoke as tabsserver_main
from tabsdata._utils.bundle_utils import create_bundle_archive
from tabsdata.exceptions import SecretConfigurationError
from tests_tabsdata.bootest import ROOT_FOLDER, TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    FUNCTION_DATA_FOLDER,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    clean_polars_df,
    write_v2_yaml_file,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = ROOT_FOLDER
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER

SFR_TESTING_REPORT = {
    "long_id": "00OgL000004gpqDUAQ",
    "short_id": "00OgL000004gpqD",
    "name": "Foo_for_dev_ci_testing",
}

SFR_TESTING_ALIASES_AND_ROWS = {
    "OEPIC": 12,
    "autoproc": 1,
}

SFR_INITIAL_VALUES_TESTING_REPORT = "users_dev_ci"

SFR_INITIAL_VALUES_TESTING_ALIASES_AND_ROWS = {
    "User": 2,
    "EPIC": 3,
}


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
@pytest.mark.tabsserver
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_salesforce_report_by_long_id(tmp_path, sf_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    input_salesforce_report_long_id = copy.deepcopy(input_salesforce_report)
    input_salesforce_report_long_id.input.report = SFR_TESTING_REPORT["long_id"]
    input_salesforce_report_long_id.input.credentials = sf_config["CREDENTIALS"]
    context_archive = create_bundle_archive(
        input_salesforce_report_long_id,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        output_initial_values_path=path_to_output_initial_values,
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_salesforce_report",
        "expected_result.parquet",
    )
    expected_output = pl.read_parquet(expected_output_file)
    expected_output = clean_polars_df(expected_output)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
@pytest.mark.tabsserver
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_salesforce_report_by_short_id(tmp_path, sf_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    input_salesforce_report_short_id = copy.deepcopy(input_salesforce_report)
    input_salesforce_report_short_id.input.report = SFR_TESTING_REPORT["short_id"]
    input_salesforce_report_short_id.input.credentials = sf_config["CREDENTIALS"]
    context_archive = create_bundle_archive(
        input_salesforce_report_short_id,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        output_initial_values_path=path_to_output_initial_values,
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_salesforce_report",
        "expected_result.parquet",
    )
    expected_output = pl.read_parquet(expected_output_file)
    expected_output = clean_polars_df(expected_output)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
@pytest.mark.tabsserver
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_salesforce_report_by_name(tmp_path, sf_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    input_salesforce_report_name = copy.deepcopy(input_salesforce_report)
    input_salesforce_report_name.input.report = SFR_TESTING_REPORT["name"]
    input_salesforce_report_name.input.credentials = sf_config["CREDENTIALS"]
    context_archive = create_bundle_archive(
        input_salesforce_report_name,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        output_initial_values_path=path_to_output_initial_values,
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
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_salesforce_report",
        "expected_result.parquet",
    )
    expected_output = pl.read_parquet(expected_output_file)
    expected_output = clean_polars_df(expected_output)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
@pytest.mark.tabsserver
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_salesforce_report_initial_values_by_label(tmp_path, sf_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    input_sfr_initial_values_by_label = copy.deepcopy(
        input_salesforce_report_initial_values
    )
    input_sfr_initial_values_by_label.input.report = SFR_INITIAL_VALUES_TESTING_REPORT
    input_sfr_initial_values_by_label.input.last_modified_column = "Last Modified Date"
    input_sfr_initial_values_by_label.input.column_name_strategy = "label"
    input_sfr_initial_values_by_label.input.initial_last_modified = (
        "2024-03-10T11:03:08.000+0000"
    )
    input_sfr_initial_values_by_label.input.credentials = sf_config["CREDENTIALS"]
    context_archive = create_bundle_archive(
        input_sfr_initial_values_by_label,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output_01.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        output_initial_values_path=path_to_output_initial_values,
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

    # Check first output file
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_salesforce_report_initial_values",
        "expected_result_by_label.parquet",
    )
    expected_output = pl.read_parquet(expected_output_file)
    expected_output = clean_polars_df(expected_output)
    assert output.shape == expected_output.shape
    assert output.columns == expected_output.columns

    # Check initial values properly stored
    assert os.path.isfile(path_to_output_initial_values)
    initial_values = pl.read_parquet(path_to_output_initial_values)
    expected_initial_values = pl.read_parquet(
        os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_input_salesforce_report_initial_values",
            "expected_initial_values.parquet",
        )
    )
    assert initial_values.shape == expected_initial_values.shape
    assert initial_values.columns == expected_initial_values.columns

    # Second iteration
    output_file = os.path.join(tmp_path, "output_02.parquet")
    path_to_input_initial_values = path_to_output_initial_values
    path_to_output_initial_values = os.path.join(
        tmp_path, "second_initial_values.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        input_initial_values_path=path_to_input_initial_values,
        output_initial_values_path=path_to_output_initial_values,
        function_data_path=function_data_folder,
    )
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
    assert not os.path.isfile(output_file)

    # In this second iteration, nothing has changed, so the initial values stayed the
    # same. Therefore, we will send a "NoData" for the initial values, and the file
    # will not exist
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
@pytest.mark.tabsserver
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_input_salesforce_report_initial_values_by_name(tmp_path, sf_config):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    input_sfr_initial_values_by_name = copy.deepcopy(
        input_salesforce_report_initial_values
    )
    input_sfr_initial_values_by_name.input.report = SFR_INITIAL_VALUES_TESTING_REPORT
    input_sfr_initial_values_by_name.input.last_modified_column = "LAST_UPDATE"
    input_sfr_initial_values_by_name.input.column_name_strategy = "columnName"
    input_sfr_initial_values_by_name.input.initial_last_modified = (
        "2024-03-10T11:03:08.000+0000"
    )
    input_sfr_initial_values_by_name.input.credentials = sf_config["CREDENTIALS"]
    context_archive = create_bundle_archive(
        input_sfr_initial_values_by_name,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output_01.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        output_initial_values_path=path_to_output_initial_values,
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

    # Check first output file
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_salesforce_report_initial_values",
        "expected_result_by_name.parquet",
    )
    expected_output = pl.read_parquet(expected_output_file)
    expected_output = clean_polars_df(expected_output)
    assert output.shape == expected_output.shape
    assert output.columns == expected_output.columns

    # Check initial values properly stored
    assert os.path.isfile(path_to_output_initial_values)
    initial_values = pl.read_parquet(path_to_output_initial_values)
    expected_initial_values = pl.read_parquet(
        os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_input_salesforce_report_initial_values",
            "expected_initial_values.parquet",
        )
    )
    assert initial_values.shape == expected_initial_values.shape
    assert initial_values.columns == expected_initial_values.columns

    # Second iteration
    output_file = os.path.join(tmp_path, "output_02.parquet")
    path_to_input_initial_values = path_to_output_initial_values
    path_to_output_initial_values = os.path.join(
        tmp_path, "second_initial_values.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        input_initial_values_path=path_to_input_initial_values,
        output_initial_values_path=path_to_output_initial_values,
        function_data_path=function_data_folder,
    )
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
    assert not os.path.isfile(output_file)

    # In this second iteration, nothing has changed, so the initial values stayed the
    # same. Therefore, we will send a "NoData" for the initial values, and the file
    # will not exist
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.salesforce
@pytest.mark.unit
def test_username_password_security_token():
    source = td.SalesforceReportSource(
        FAKE_CREDENTIALS,
        report="FAKE_ID",
        column_name_strategy="columnName",
    )
    assert source.credentials.username == DirectSecret("username")
    assert source.credentials.password == DirectSecret("password")
    assert source.credentials.security_token == DirectSecret("security_token")
    source = td.SalesforceReportSource(
        td.SalesforceTokenCredentials(
            username=td.EnvironmentSecret("username"),
            password=td.EnvironmentSecret("password"),
            security_token=td.EnvironmentSecret("security_token"),
        ),
        report="FAKE_ID",
        column_name_strategy="columnName",
    )
    assert source.credentials.username == td.EnvironmentSecret("username")
    assert source.credentials.password == td.EnvironmentSecret("password")
    assert source.credentials.security_token == td.EnvironmentSecret("security_token")


@pytest.mark.salesforce
@pytest.mark.unit
def test_wrong_type_username_password_security_token():
    with pytest.raises(SecretConfigurationError):
        # noinspection PyTypeChecker
        td.SalesforceReportSource(
            td.SalesforceTokenCredentials(
                username=1,
                password="password",
                security_token="security_token",
            ),
            report="report",
            column_name_strategy="columnName",
        )
    with pytest.raises(SecretConfigurationError):
        # noinspection PyTypeChecker
        td.SalesforceReportSource(
            td.SalesforceTokenCredentials(
                username="user",
                password=1,
                security_token="security_token",
            ),
            report="report",
            column_name_strategy="columnName",
        )
    with pytest.raises(SecretConfigurationError):
        # noinspection PyTypeChecker
        td.SalesforceReportSource(
            td.SalesforceTokenCredentials(
                username="user",
                password="password",
                security_token=1,
            ),
            report="report",
            column_name_strategy="columnName",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_initial_last_modified():
    source = td.SalesforceReportSource(
        FAKE_CREDENTIALS,
        report="FAKE REPORT",
        last_modified_column="fakeColumn",
        initial_last_modified="2024-03-10T11:03:08.000+0000",
        column_name_strategy="columnName",
    )
    assert source.initial_values == {
        "initial_last_modified": "2024-03-10T11:03:08.000000+0000"
    }
    assert source.report == ["FAKE REPORT"]

    source = td.SalesforceReportSource(
        FAKE_CREDENTIALS,
        report="FAKE REPORT",
        initial_last_modified="2025-03-12T05:24:32.543437-0400",
        last_modified_column="fakeColumn",
        column_name_strategy="columnName",
    )
    assert source.initial_values == {
        "initial_last_modified": "2025-03-12T05:24:32.543437-0400"
    }


@pytest.mark.salesforce
@pytest.mark.unit
def test_report():
    source = td.SalesforceReportSource(
        FAKE_CREDENTIALS,
        report="FAKE_ID",
        column_name_strategy="columnName",
    )
    assert source.report == ["FAKE_ID"]


@pytest.mark.salesforce
@pytest.mark.unit
def test_report_wrong_type():
    with pytest.raises(TypeError):
        # noinspection PyTypeChecker
        td.SalesforceReportSource(
            FAKE_CREDENTIALS,
            report=1,
            column_name_strategy="columnName",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_initial_last_modified_missing():
    with pytest.raises(ValueError):
        td.SalesforceReportSource(
            FAKE_CREDENTIALS,
            report="FAKE REPORT",
            column_name_strategy="columnName",
            last_modified_column="LastModifiedDate",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_initial_last_modified_column_missing():
    with pytest.raises(ValueError):
        td.SalesforceReportSource(
            FAKE_CREDENTIALS,
            report="FAKE REPORT",
            column_name_strategy="columnName",
            initial_last_modified="2024-03-10T11:03:08.000+0000",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_initial_last_modified_no_timezone():
    with pytest.raises(ValueError):
        td.SalesforceReportSource(
            FAKE_CREDENTIALS,
            report="FAKE REPORT",
            initial_last_modified="2098-02-05T11:27:47.000000",
            last_modified_column="LastModifiedDate",
            column_name_strategy="columnName",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_optional_parameters():
    source = td.SalesforceReportSource(
        FAKE_CREDENTIALS,
        report="FAKE_ID",
        api_version="50.0",
        instance_url="fake_url",
        column_name_strategy="columnName",
    )
    assert source.api_version == "50.0"
    assert source.instance_url == "fake_url"


@pytest.mark.salesforce
@pytest.mark.unit
def test_maximum_date():
    from tabsdata_salesforce._connector import _maximum_date

    date1 = "2098-02-05T11:27:47.000000+0000"
    date2 = "2068-06-16T18:47:39.000000-0400"
    date3 = "2061-02-19T23:50:07.000000+0100"
    date4 = "2004-04-27T07:04:56.000000-0400"
    date5 = "1934-04-16T14:10:02.000000+0000"
    assert _maximum_date(date1, date2) == date1
    assert _maximum_date(date2, date1) == date1
    assert _maximum_date(date1, date1) == date1
    assert _maximum_date(date1, date3) == date1
    assert _maximum_date(date3, date1) == date1
    assert _maximum_date(date1, date4) == date1
    assert _maximum_date(date4, date1) == date1
    assert _maximum_date(date1, date5) == date1
    assert _maximum_date(date5, date1) == date1
    assert _maximum_date(date2, date3) == date2
    assert _maximum_date(date3, date2) == date2
    assert _maximum_date(date2, date4) == date2
    assert _maximum_date(date4, date2) == date2
    assert _maximum_date(date2, date5) == date2
    assert _maximum_date(date5, date2) == date2
    assert _maximum_date(date3, date4) == date3
    assert _maximum_date(date4, date3) == date3
    assert _maximum_date(date3, date5) == date3
    assert _maximum_date(date5, date3) == date3
    assert _maximum_date(date4, date5) == date4
    assert _maximum_date(date5, date4) == date4


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
def test_chunk(tmp_path, sf_config):
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report="DOES NOT EXIST",
        column_name_strategy="columnName",
    )
    with pytest.raises(Exception):
        source.chunk(str(tmp_path))

    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_TESTING_REPORT["long_id"],
        column_name_strategy="columnName",
    )
    [result] = source.chunk(str(tmp_path))
    result = os.path.join(tmp_path, result)
    assert os.path.isfile(result)
    first_output = pl.read_parquet(result)
    first_output = clean_polars_df(first_output)
    assert not first_output.is_empty()

    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_TESTING_REPORT["long_id"],
        column_name_strategy="columnName",
    )
    [result] = source.chunk(str(tmp_path))
    result = os.path.join(tmp_path, result)
    assert os.path.isfile(result)
    second_output = pl.read_parquet(result)
    second_output = clean_polars_df(second_output)
    assert not second_output.is_empty()

    assert first_output.equals(second_output)


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
def test_chunk_with_filter_by_column_name(tmp_path, sf_config):
    for value, number in SFR_TESTING_ALIASES_AND_ROWS.items():
        source = td.SalesforceReportSource(
            sf_config["CREDENTIALS"],
            report=SFR_TESTING_REPORT["long_id"],
            column_name_strategy="columnName",
            filter=("CREATED_ALIAS", "equals", value),
        )
        [result] = source.chunk(str(tmp_path))
        result = os.path.join(tmp_path, result)
        assert os.path.isfile(result)
        first_output = pl.read_parquet(result)
        first_output = clean_polars_df(first_output)
        logger.debug(first_output)
        logger.debug("-" * 80)
        assert first_output.height == number
        os.remove(result)

    filter = [
        ("CREATED_ALIAS", "equals", value)
        for value in SFR_TESTING_ALIASES_AND_ROWS.keys()
    ]

    # Since we will and different values, the result should be empty
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_TESTING_REPORT["long_id"],
        column_name_strategy="columnName",
        filter=filter,
    )
    [result] = source.chunk(str(tmp_path))
    assert result is None

    # Since we will or different values, the result should be the sum
    logic = " OR ".join([f"{i+1}" for i in range(len(filter))])
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_TESTING_REPORT["long_id"],
        column_name_strategy="columnName",
        filter=filter,
        filter_logic=logic,
    )
    [result] = source.chunk(str(tmp_path))
    result = os.path.join(tmp_path, result)
    assert os.path.isfile(result)
    third_output = pl.read_parquet(result)
    logger.debug(third_output)
    logger.debug("-" * 80)
    third_output = clean_polars_df(third_output)
    assert third_output.height == sum(SFR_TESTING_ALIASES_AND_ROWS.values())
    os.remove(result)


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
def test_chunk_with_filter_by_label(tmp_path, sf_config):
    for value, number in SFR_TESTING_ALIASES_AND_ROWS.items():
        source = td.SalesforceReportSource(
            sf_config["CREDENTIALS"],
            report=SFR_TESTING_REPORT["long_id"],
            column_name_strategy="label",
            filter=("Created Alias", "equals", value),
        )
        [result] = source.chunk(str(tmp_path))
        result = os.path.join(tmp_path, result)
        assert os.path.isfile(result)
        first_output = pl.read_parquet(result)
        first_output = clean_polars_df(first_output)
        logger.debug(first_output)
        logger.debug("-" * 80)
        assert first_output.height == number
        os.remove(result)

    filter = [
        ("Created Alias", "equals", value)
        for value in SFR_TESTING_ALIASES_AND_ROWS.keys()
    ]

    # Since we will and different values, the result should be empty
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_TESTING_REPORT["long_id"],
        column_name_strategy="label",
        filter=filter,
    )
    [result] = source.chunk(str(tmp_path))
    assert result is None

    # Since we will or different values, the result should be the sum
    logic = " OR ".join([f"{i+1}" for i in range(len(filter))])
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_TESTING_REPORT["long_id"],
        column_name_strategy="label",
        filter=filter,
        filter_logic=logic,
    )
    [result] = source.chunk(str(tmp_path))
    result = os.path.join(tmp_path, result)
    assert os.path.isfile(result)
    third_output = pl.read_parquet(result)
    logger.debug(third_output)
    logger.debug("-" * 80)
    third_output = clean_polars_df(third_output)
    assert third_output.height == sum(SFR_TESTING_ALIASES_AND_ROWS.values())
    os.remove(result)


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
def test_chunk_with_filter_and_offset_by_column_name(tmp_path, sf_config):
    for value, number in SFR_INITIAL_VALUES_TESTING_ALIASES_AND_ROWS.items():
        source = td.SalesforceReportSource(
            sf_config["CREDENTIALS"],
            report=SFR_INITIAL_VALUES_TESTING_REPORT,
            column_name_strategy="columnName",
            last_modified_column="LAST_UPDATE",
            initial_last_modified="2024-03-10T11:03:08.000+0000",
            filter=("LAST_NAME", "equals", value),
        )
        [result] = source.chunk(str(tmp_path))
        result = os.path.join(tmp_path, result)
        assert os.path.isfile(result)
        first_output = pl.read_parquet(result)
        first_output = clean_polars_df(first_output)
        logger.debug(first_output)
        logger.debug("-" * 80)
        assert first_output.height == number
        os.remove(result)

    filter = [
        ("LAST_NAME", "equals", value)
        for value in SFR_INITIAL_VALUES_TESTING_ALIASES_AND_ROWS.keys()
    ]

    # Since we will and different values, the result should be empty
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_INITIAL_VALUES_TESTING_REPORT,
        column_name_strategy="columnName",
        last_modified_column="LAST_UPDATE",
        initial_last_modified="2024-03-10T11:03:08.000+0000",
        filter=filter,
    )
    [result] = source.chunk(str(tmp_path))
    assert result is None

    # Since we will or different values, the result should be the sum
    logic = " OR ".join([f"{i+1}" for i in range(len(filter))])
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_INITIAL_VALUES_TESTING_REPORT,
        column_name_strategy="columnName",
        filter=filter,
        last_modified_column="LAST_UPDATE",
        initial_last_modified="2024-03-10T11:03:08.000+0000",
        filter_logic=logic,
    )
    [result] = source.chunk(str(tmp_path))
    result = os.path.join(tmp_path, result)
    assert os.path.isfile(result)
    third_output = pl.read_parquet(result)
    logger.debug(third_output)
    logger.debug("-" * 80)
    third_output = clean_polars_df(third_output)
    assert third_output.height == sum(
        SFR_INITIAL_VALUES_TESTING_ALIASES_AND_ROWS.values()
    )
    os.remove(result)


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
def test_chunk_with_filter_and_offset_by_label(tmp_path, sf_config):
    for value, number in SFR_INITIAL_VALUES_TESTING_ALIASES_AND_ROWS.items():
        source = td.SalesforceReportSource(
            sf_config["CREDENTIALS"],
            report=SFR_INITIAL_VALUES_TESTING_REPORT,
            column_name_strategy="label",
            last_modified_column="Last Modified Date",
            initial_last_modified="2024-03-10T11:03:08.000+0000",
            filter=("Last Name", "equals", value),
        )
        [result] = source.chunk(str(tmp_path))
        result = os.path.join(tmp_path, result)
        assert os.path.isfile(result)
        first_output = pl.read_parquet(result)
        first_output = clean_polars_df(first_output)
        logger.debug(first_output)
        logger.debug("-" * 80)
        assert first_output.height == number
        os.remove(result)

    filter = [
        ("Last Name", "equals", value)
        for value in SFR_INITIAL_VALUES_TESTING_ALIASES_AND_ROWS.keys()
    ]

    # Since we will and different values, the result should be empty
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_INITIAL_VALUES_TESTING_REPORT,
        column_name_strategy="label",
        last_modified_column="Last Modified Date",
        initial_last_modified="2024-03-10T11:03:08.000+0000",
        filter=filter,
    )
    [result] = source.chunk(str(tmp_path))
    assert result is None

    # Since we will or different values, the result should be the sum
    logic = " OR ".join([f"{i+1}" for i in range(len(filter))])
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_INITIAL_VALUES_TESTING_REPORT,
        column_name_strategy="label",
        filter=filter,
        last_modified_column="Last Modified Date",
        initial_last_modified="2024-03-10T11:03:08.000+0000",
        filter_logic=logic,
    )
    [result] = source.chunk(str(tmp_path))
    result = os.path.join(tmp_path, result)
    assert os.path.isfile(result)
    third_output = pl.read_parquet(result)
    logger.debug(third_output)
    logger.debug("-" * 80)
    third_output = clean_polars_df(third_output)
    assert third_output.height == sum(
        SFR_INITIAL_VALUES_TESTING_ALIASES_AND_ROWS.values()
    )
    os.remove(result)


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
def test_column_name_strategy(tmp_path, sf_config):
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_TESTING_REPORT["long_id"],
        column_name_strategy="columnName",
    )
    [result] = source.chunk(str(tmp_path))
    result = os.path.join(tmp_path, result)
    assert os.path.isfile(result)
    first_output = pl.read_parquet(result)
    first_output = clean_polars_df(first_output)
    assert not first_output.is_empty()

    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_TESTING_REPORT["long_id"],
        column_name_strategy="label",
    )
    [result] = source.chunk(str(tmp_path))
    result = os.path.join(tmp_path, result)
    assert os.path.isfile(result)
    second_output = pl.read_parquet(result)
    second_output = clean_polars_df(second_output)
    assert not second_output.is_empty()

    assert first_output.shape == second_output.shape
    assert first_output.columns != second_output.columns
    assert "Account Name" in second_output.columns
    assert "Account Name" not in first_output.columns
    assert "ACCOUNT_ID" in first_output.columns
    assert "ACCOUNT_ID" not in second_output.columns


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
def test_chunk_with_filter_and_late_offset_by_column_name(tmp_path, sf_config):
    for value, number in SFR_INITIAL_VALUES_TESTING_ALIASES_AND_ROWS.items():
        source = td.SalesforceReportSource(
            sf_config["CREDENTIALS"],
            report=SFR_INITIAL_VALUES_TESTING_REPORT,
            column_name_strategy="columnName",
            last_modified_column="LAST_UPDATE",
            initial_last_modified="2034-03-10T11:03:08.000+0000",
            filter=("LAST_NAME", "equals", value),
        )
        [result] = source.chunk(str(tmp_path))
        assert result is None

    filter = [
        ("LAST_NAME", "equals", value)
        for value in SFR_INITIAL_VALUES_TESTING_ALIASES_AND_ROWS.keys()
    ]

    # Since we will and different values, the result should be empty
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_INITIAL_VALUES_TESTING_REPORT,
        column_name_strategy="columnName",
        last_modified_column="LAST_UPDATE",
        initial_last_modified="2034-03-10T11:03:08.000+0000",
        filter=filter,
    )
    [result] = source.chunk(str(tmp_path))
    assert result is None

    # Since we will or different values, the result should be the sum
    logic = " OR ".join([f"{i+1}" for i in range(len(filter))])
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_INITIAL_VALUES_TESTING_REPORT,
        column_name_strategy="columnName",
        filter=filter,
        last_modified_column="LAST_UPDATE",
        initial_last_modified="2034-03-10T11:03:08.000+0000",
        filter_logic=logic,
    )
    [result] = source.chunk(str(tmp_path))
    assert result is None


@pytest.mark.requires_internet
@pytest.mark.salesforce
@pytest.mark.slow
def test_chunk_with_filter_and_late_offset_by_label(tmp_path, sf_config):
    for value, number in SFR_INITIAL_VALUES_TESTING_ALIASES_AND_ROWS.items():
        source = td.SalesforceReportSource(
            sf_config["CREDENTIALS"],
            report=SFR_INITIAL_VALUES_TESTING_REPORT,
            column_name_strategy="label",
            last_modified_column="Last Modified Date",
            initial_last_modified="2034-03-10T11:03:08.000+0000",
            filter=("Last Name", "equals", value),
        )
        [result] = source.chunk(str(tmp_path))
        assert result is None

    filter = [
        ("Last Name", "equals", value)
        for value in SFR_INITIAL_VALUES_TESTING_ALIASES_AND_ROWS.keys()
    ]

    # Since we will and different values, the result should be empty
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_INITIAL_VALUES_TESTING_REPORT,
        column_name_strategy="label",
        last_modified_column="Last Modified Date",
        initial_last_modified="2034-03-10T11:03:08.000+0000",
        filter=filter,
    )
    [result] = source.chunk(str(tmp_path))
    assert result is None

    # Since we will or different values, the result should be the sum
    logic = " OR ".join([f"{i+1}" for i in range(len(filter))])
    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report=SFR_INITIAL_VALUES_TESTING_REPORT,
        column_name_strategy="label",
        filter=filter,
        last_modified_column="Last Modified Date",
        initial_last_modified="2034-03-10T11:03:08.000+0000",
        filter_logic=logic,
    )
    [result] = source.chunk(str(tmp_path))
    assert result is None


@pytest.mark.requires_internet
@pytest.mark.salesforce
def test_login(sf_config):
    from tabsdata_salesforce._connector import _log_into_salesforce

    source = td.SalesforceReportSource(
        sf_config["CREDENTIALS"],
        report="FAKE_ID",
        column_name_strategy="columnName",
    )
    _log_into_salesforce(source)


@pytest.mark.salesforce
@pytest.mark.unit
def test_filter_logic_inferred():
    source = td.SalesforceReportSource(
        FAKE_CREDENTIALS,
        report="FAKE_ID",
        filter=[("column", "operator", "value")],
        column_name_strategy="columnName",
    )
    assert source.filter_logic == "(1)"
    source.filter = [
        ("column2", "operator2", "value2"),
        ("column3", "operator3", "value3"),
    ]
    assert source.filter_logic == "(1 AND 2)"


@pytest.mark.salesforce
@pytest.mark.unit
def test_filter_logic_declared():
    source = td.SalesforceReportSource(
        FAKE_CREDENTIALS,
        report="FAKE_ID",
        filter=[
            ("column2", "operator2", "value2"),
            ("column3", "operator3", "value3"),
        ],
        filter_logic="(1 OR NOT 2)",
        column_name_strategy="columnName",
    )
    assert source.filter_logic == "(1 OR NOT 2)"


@pytest.mark.salesforce
@pytest.mark.unit
def test_filter_logic_wrong_type():
    with pytest.raises(TypeError):
        td.SalesforceReportSource(
            FAKE_CREDENTIALS,
            report="FAKE_ID",
            filter=[("column", "operator", "value")],
            filter_logic=42,
            column_name_strategy="columnName",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_filter_logic_no_filter():
    with pytest.raises(ValueError):
        td.SalesforceReportSource(
            FAKE_CREDENTIALS,
            report="FAKE_ID",
            filter_logic="(1 OR NOT 2)",
            column_name_strategy="columnName",
        )


@pytest.mark.requires_internet
@pytest.mark.salesforce
def test_login_fails():
    from tabsdata_salesforce._connector import _log_into_salesforce

    source = td.SalesforceReportSource(
        FAKE_CREDENTIALS,
        report="FAKE_ID",
        column_name_strategy="columnName",
    )
    with pytest.raises(Exception):
        _log_into_salesforce(source)


@pytest.mark.salesforce
@pytest.mark.unit
def test_find_report_by_wrong_value():
    with pytest.raises(ValueError):
        td.SalesforceReportSource(
            FAKE_CREDENTIALS,
            report="FAKE_ID",
            find_report_by="MyFilter",
            column_name_strategy="columnName",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_find_report_by_wrong_type():
    with pytest.raises(ValueError):
        td.SalesforceReportSource(
            FAKE_CREDENTIALS,
            report="FAKE_ID",
            find_report_by=42,
            column_name_strategy="columnName",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_find_report_by_declared():
    source = td.SalesforceReportSource(
        FAKE_CREDENTIALS,
        report="FAKE_ID",
        find_report_by="id",
        column_name_strategy="columnName",
    )
    assert source.find_report_by == "id"
    source.find_report_by = "name"
    assert source.find_report_by == "name"


@pytest.mark.salesforce
@pytest.mark.unit
def test_find_report_by_inferred():
    source = td.SalesforceReportSource(
        FAKE_CREDENTIALS,
        report="FAKE_ID",
        column_name_strategy="columnName",
    )
    assert source.find_report_by == "name"
    source.report = SFR_TESTING_REPORT["long_id"]
    assert source.find_report_by == "id"


@pytest.mark.salesforce
@pytest.mark.unit
def test_filter_parameter():
    source = td.SalesforceReportSource(
        FAKE_CREDENTIALS,
        report="FAKE_ID",
        filter=("column", "operator", "value"),
        column_name_strategy="columnName",
    )
    assert source.filter == [("column", "operator", "value")]
    source.filter = [
        ("column2", "operator2", "value2"),
        ("column3", "operator3", "value3"),
    ]
    assert source.filter == [
        ("column2", "operator2", "value2"),
        ("column3", "operator3", "value3"),
    ]


@pytest.mark.salesforce
@pytest.mark.unit
def test_filter_wrong_tuple_len():
    with pytest.raises(ValueError):
        td.SalesforceReportSource(
            FAKE_CREDENTIALS,
            report="FAKE_ID",
            filter=("column", "operator"),
            column_name_strategy="columnName",
        )
    with pytest.raises(ValueError):
        td.SalesforceReportSource(
            FAKE_CREDENTIALS,
            report="FAKE_ID",
            filter=("column", "operator", "value", "extra"),
            column_name_strategy="columnName",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_filter_wrong_tuple_type():
    with pytest.raises(ValueError):
        td.SalesforceReportSource(
            FAKE_CREDENTIALS,
            report="FAKE_ID",
            filter=("column", "operator", 42),
            column_name_strategy="columnName",
        )


@pytest.mark.salesforce
@pytest.mark.unit
def test_filter_wrong_tuple_list():
    with pytest.raises(ValueError):
        td.SalesforceReportSource(
            FAKE_CREDENTIALS,
            report="FAKE_ID",
            filter=[("column", "operator")],
            column_name_strategy="columnName",
        )
    with pytest.raises(ValueError):
        td.SalesforceReportSource(
            FAKE_CREDENTIALS,
            report="FAKE_ID",
            filter=[("column", "operator", "value", "extra")],
            column_name_strategy="columnName",
        )

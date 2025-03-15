#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os

import polars as pl
import pytest
from tests_tabsdata.bootest import root_folder
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    clean_polars_df,
    read_json_and_clean,
    write_v1_yaml_file,
)
from tests_tabsdata_salesforce.conftest import TESTING_RESOURCES_FOLDER
from tests_tabsdata_salesforce.testing_resources.test_input_salesforce.example import (
    input_salesforce,
)
from tests_tabsdata_salesforce.testing_resources.test_input_salesforce_initial_values.example import (
    input_salesforce_initial_values,
)

import tabsdata as td
from tabsdata.utils.bundle_utils import create_bundle_archive
from tabsserver.function_execution.response_utils import RESPONSE_FILE_NAME
from tabsserver.main import EXECUTION_CONTEXT_FILE_NAME
from tabsserver.main import do as tabsserver_main

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = root_folder()
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = os.path.join(
    os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION), "local_dev"
)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.salesforce
def test_input_salesforce(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_salesforce,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file],
        output_initial_values_path=path_to_output_initial_values,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    environment_name, result = tabsserver_main(
        tmp_path,
        response_folder,
        tabsserver_output_folder,
        environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
        logs_folder=logs_folder,
    )
    assert result == 0
    assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_salesforce",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)
    assert not os.path.isfile(path_to_output_initial_values)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.salesforce
def test_input_salesforce_initial_values(tmp_path):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        input_salesforce_initial_values,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    output_file = os.path.join(tmp_path, "output.parquet")
    second_output_file = os.path.join(tmp_path, "second_output.parquet")
    path_to_output_initial_values = os.path.join(tmp_path, "initial_values.parquet")
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file, second_output_file],
        output_initial_values_path=path_to_output_initial_values,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    environment_name, result = tabsserver_main(
        tmp_path,
        response_folder,
        tabsserver_output_folder,
        environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
        logs_folder=logs_folder,
    )
    assert result == 0
    assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

    # Check first output file
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_salesforce_initial_values",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    # Check second output file
    assert os.path.isfile(second_output_file)
    output = pl.read_parquet(second_output_file)
    output = clean_polars_df(output)
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_input_salesforce_initial_values",
        "expected_result.json",
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)

    # Check initial values properly stored
    assert os.path.isfile(path_to_output_initial_values)
    initial_values = pl.read_parquet(path_to_output_initial_values)
    initial_values.write_parquet(
        os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_input_salesforce_initial_values",
            "expected_initial_values.parquet",
        )
    )
    expected_initial_values = pl.read_parquet(
        os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_input_salesforce_initial_values",
            "expected_initial_values.parquet",
        )
    )
    assert initial_values.equals(expected_initial_values)

    # Second iteration
    path_to_input_initial_values = path_to_output_initial_values
    path_to_output_initial_values = os.path.join(
        tmp_path, "second_initial_values.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        mock_table_location=[output_file, second_output_file],
        input_initial_values_path=path_to_input_initial_values,
        output_initial_values_path=path_to_output_initial_values,
    )
    environment_name, result = tabsserver_main(
        tmp_path,
        response_folder,
        tabsserver_output_folder,
        environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
        logs_folder=logs_folder,
    )
    assert result == 0
    assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))
    assert os.path.isfile(output_file)
    output = pl.read_parquet(output_file)
    output = clean_polars_df(output)
    assert output.is_empty()
    assert os.path.isfile(second_output_file)
    output = pl.read_parquet(second_output_file)
    output = clean_polars_df(output)
    assert output.is_empty()
    assert os.path.isfile(path_to_output_initial_values)


@pytest.mark.salesforce
def test_username_password_security_token():
    source = td.SalesforceSource(
        username="username",
        password="password",
        security_token="security_token",
        query="SELECT Name FROM Contact",
    )
    assert source.username == td.DirectSecret("username")
    assert source.password == td.DirectSecret("password")
    assert source.security_token == td.DirectSecret("security_token")
    source = td.SalesforceSource(
        username=td.EnvironmentSecret("username"),
        password=td.EnvironmentSecret("password"),
        security_token=td.EnvironmentSecret("security_token"),
        query="SELECT Name FROM Contact",
    )
    assert source.username == td.EnvironmentSecret("username")
    assert source.password == td.EnvironmentSecret("password")
    assert source.security_token == td.EnvironmentSecret("security_token")


@pytest.mark.salesforce
def test_wrong_type_username_password_security_token():
    with pytest.raises(TypeError):
        td.SalesforceSource(
            username=1,
            password="password",
            security_token="security_token",
            query="query",
        )
    with pytest.raises(TypeError):
        td.SalesforceSource(
            username="user", password=1, security_token="security_token", query="query"
        )
    with pytest.raises(TypeError):
        td.SalesforceSource(
            username="user", password="password", security_token=1, query="query"
        )


@pytest.mark.salesforce
def test_initial_last_modified():
    source = td.SalesforceSource(
        username="username",
        password="password",
        security_token="security_token",
        query=(
            f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact WHERE"
            f" {td.SalesforceSource.LAST_MODIFIED_COLUMN} >"
            f" {td.SalesforceSource.LAST_MODIFIED_TOKEN}"
        ),
        initial_last_modified="2024-03-10T11:03:08.000+0000",
    )
    assert source.initial_values == {
        "initial_last_modified": "2024-03-10T11:03:08.000000+0000"
    }
    assert source.query == [
        f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact WHERE"
        f" {td.SalesforceSource.LAST_MODIFIED_COLUMN} >"
        f" {td.SalesforceSource.LAST_MODIFIED_TOKEN}"
    ]

    source = td.SalesforceSource(
        username="username",
        password="password",
        security_token="security_token",
        query=(
            f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact WHERE"
            f" {td.SalesforceSource.LAST_MODIFIED_COLUMN} >"
            f" {td.SalesforceSource.LAST_MODIFIED_TOKEN}"
        ),
        initial_last_modified="2025-03-12T05:24:32.543437-0400",
    )
    assert source.initial_values == {
        "initial_last_modified": "2025-03-12T05:24:32.543437-0400"
    }


@pytest.mark.salesforce
def test_query():
    source = td.SalesforceSource(
        username="username",
        password="password",
        security_token="security_token",
        query="SELECT Name FROM Contact",
    )
    assert source.query == ["SELECT Name FROM Contact"]


@pytest.mark.salesforce
def test_query_wrong_type():
    with pytest.raises(TypeError):
        td.SalesforceSource(
            username="username",
            password="password",
            security_token="security_token",
            query=1,
        )


@pytest.mark.salesforce
def test_initial_last_modified_missing():
    with pytest.raises(ValueError):
        td.SalesforceSource(
            username="username",
            password="password",
            security_token="security_token",
            query=(
                f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact"
                f" WHERE {td.SalesforceSource.LAST_MODIFIED_COLUMN} >"
                f" {td.SalesforceSource.LAST_MODIFIED_TOKEN}"
            ),
        )


@pytest.mark.salesforce
def test_initial_last_modified_no_timezone():
    with pytest.raises(ValueError):
        td.SalesforceSource(
            username="username",
            password="password",
            security_token="security_token",
            query=(
                f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact"
                f" WHERE {td.SalesforceSource.LAST_MODIFIED_COLUMN} >"
                f" {td.SalesforceSource.LAST_MODIFIED_TOKEN}"
            ),
            initial_last_modified="2098-02-05T11:27:47.000000",
        )


@pytest.mark.salesforce
def test_optional_parameters():
    source = td.SalesforceSource(
        username="username",
        password="password",
        security_token="security_token",
        query="SELECT Name FROM Contact",
        api_version="50.0",
        instance_url="fake_url",
        include_deleted=True,
    )
    assert source.api_version == "50.0"
    assert source.instance_url == "fake_url"
    assert source.include_deleted is True


@pytest.mark.salesforce
def test_replace_last_modified_token():
    source = td.SalesforceSource(
        username="username",
        password="password",
        security_token="security_token",
        query="SELECT Name FROM Contact",
    )
    last_modified_token = source.LAST_MODIFIED_TOKEN
    query = f"{last_modified_token}"
    assert source._replace_last_modified_token(query, "hello") == "hello"
    query = f"27_{last_modified_token}_hi_{last_modified_token}"
    assert source._replace_last_modified_token(query, "hello") == "27_hello_hi_hello"


@pytest.mark.salesforce
def test_maximum_date():
    source = td.SalesforceSource(
        username="username",
        password="password",
        security_token="security_token",
        query="SELECT Name FROM Contact",
    )
    date1 = "2098-02-05T11:27:47.000000+0000"
    date2 = "2068-06-16T18:47:39.000000-0400"
    date3 = "2061-02-19T23:50:07.000000+0100"
    date4 = "2004-04-27T07:04:56.000000-0400"
    date5 = "1934-04-16T14:10:02.000000+0000"
    assert source._maximum_date(date1, date2) == date1
    assert source._maximum_date(date2, date1) == date1
    assert source._maximum_date(date1, date1) == date1
    assert source._maximum_date(date1, date3) == date1
    assert source._maximum_date(date3, date1) == date1
    assert source._maximum_date(date1, date4) == date1
    assert source._maximum_date(date4, date1) == date1
    assert source._maximum_date(date1, date5) == date1
    assert source._maximum_date(date5, date1) == date1
    assert source._maximum_date(date2, date3) == date2
    assert source._maximum_date(date3, date2) == date2
    assert source._maximum_date(date2, date4) == date2
    assert source._maximum_date(date4, date2) == date2
    assert source._maximum_date(date2, date5) == date2
    assert source._maximum_date(date5, date2) == date2
    assert source._maximum_date(date3, date4) == date3
    assert source._maximum_date(date4, date3) == date3
    assert source._maximum_date(date3, date5) == date3
    assert source._maximum_date(date5, date3) == date3
    assert source._maximum_date(date4, date5) == date4
    assert source._maximum_date(date5, date4) == date4


@pytest.mark.salesforce
@pytest.mark.requires_internet
@pytest.mark.slow
def test_trigger_input(tmp_path):
    date1 = "2098-02-05T11:27:47.000000+0000"
    date5 = "1934-04-16T14:10:02.000000+0000"
    source = td.SalesforceSource(
        username=td.EnvironmentSecret("SALESFORCE_USERNAME"),
        password=td.EnvironmentSecret("SALESFORCE_PASSWORD"),
        security_token=td.EnvironmentSecret("SALESFORCE_SECURITY_TOKEN"),
        query=(
            f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact"
            f" WHERE {td.SalesforceSource.LAST_MODIFIED_COLUMN} > {date1}"
        ),
    )
    [result] = source.trigger_input(tmp_path)
    assert result is None

    source = td.SalesforceSource(
        username=td.EnvironmentSecret("SALESFORCE_USERNAME"),
        password=td.EnvironmentSecret("SALESFORCE_PASSWORD"),
        security_token=td.EnvironmentSecret("SALESFORCE_SECURITY_TOKEN"),
        query=(
            f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact"
            f" WHERE {td.SalesforceSource.LAST_MODIFIED_COLUMN} > {date5}"
        ),
    )
    [result] = source.trigger_input(tmp_path)
    result = os.path.join(tmp_path, result)
    assert os.path.isfile(result)
    output = pl.read_parquet(result)
    output = clean_polars_df(output)
    assert not output.is_empty()


@pytest.mark.salesforce
@pytest.mark.requires_internet
def test_login():
    date5 = "1934-04-16T14:10:02.000000+0000"
    source = td.SalesforceSource(
        username=td.EnvironmentSecret("SALESFORCE_USERNAME"),
        password=td.EnvironmentSecret("SALESFORCE_PASSWORD"),
        security_token=td.EnvironmentSecret("SALESFORCE_SECURITY_TOKEN"),
        query=(
            f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact"
            f" WHERE {td.SalesforceSource.LAST_MODIFIED_COLUMN} > {date5}"
        ),
    )
    source._log_into_salesforce()


@pytest.mark.salesforce
@pytest.mark.requires_internet
def test_login_fails():
    date5 = "1934-04-16T14:10:02.000000+0000"
    source = td.SalesforceSource(
        username="WRONG_USERNAME",
        password="WRONG_PASSWORD",
        security_token="WRONG_TOKEN",
        query=(
            f"SELECT Name,{td.SalesforceSource.LAST_MODIFIED_COLUMN} FROM Contact"
            f" WHERE {td.SalesforceSource.LAST_MODIFIED_COLUMN} > {date5}"
        ),
    )
    with pytest.raises(Exception):
        source._log_into_salesforce()

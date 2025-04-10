#
# Copyright 2025 Tabs Data Inc.
#

import copy
import datetime
import inspect
import logging
import os

import polars as pl
import pytest
from azure.core.exceptions import ResourceNotFoundError
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
    write_v1_yaml_file,
)
from tests_tabsdata.testing_resources.test_output_azure.example import (
    output_azure as output_azure_format_testing,
)

import tabsdata as td
from tabsdata.tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata.tabsserver.invoker import EXECUTION_CONTEXT_FILE_NAME
from tabsdata.tabsserver.invoker import invoke as tabsserver_main
from tabsdata.utils.bundle_utils import create_bundle_archive

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


ROOT_PROJECT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(ABSOLUTE_TEST_FOLDER_LOCATION))
)
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


@pytest.mark.requires_internet
@pytest.mark.integration
@pytest.mark.slow
def test_output_azure_parquet(tmp_path, azure_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_azure_parquet = copy.deepcopy(output_azure_format_testing)
    container_name = "tabsdataci"
    blob_name = (
        "test_output/test_output_azure_parquet_"
        f"{int(datetime.datetime.now().timestamp())}.parquet"
    )
    blob_client = None
    output_file = f"az://{container_name}/{blob_name}"
    output_azure_parquet.output.uri = output_file
    context_archive = create_bundle_archive(
        output_azure_parquet,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_azure", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    os.makedirs(tabsserver_output_folder, exist_ok=True)
    try:
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
        )
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        temporary_output_file = os.path.join(tabsserver_output_folder, "0.parquet")
        assert os.path.isfile(temporary_output_file)
        output = pl.read_parquet(temporary_output_file)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_azure",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        blob_client = azure_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        output = pl.read_parquet(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_azure",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        if blob_client is not None:
            try:
                blob_client.delete_blob()
            except ResourceNotFoundError as e:
                logger.warning(
                    f"The blob to delete wasn't created or was already deleted: {e}"
                )


@pytest.mark.requires_internet
@pytest.mark.integration
@pytest.mark.slow
def test_output_azure_csv(tmp_path, azure_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_azure_csv = copy.deepcopy(output_azure_format_testing)
    container_name = "tabsdataci"
    blob_name = (
        "test_output/test_output_azure_csv_"
        f"{int(datetime.datetime.now().timestamp())}.csv"
    )
    blob_client = None
    output_file = f"az://{container_name}/{blob_name}"
    output_azure_csv.output.uri = output_file
    # ToDo: Undo when https://github.com/pola-rs/polars/issues/21802 fix is available
    output_azure_csv.output.format = td.CSVFormat(
        eol_char="\n", separator=",", output_float_precision=4
    )
    context_archive = create_bundle_archive(
        output_azure_csv,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_azure", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
    )

    try:
        tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
        os.makedirs(tabsserver_output_folder, exist_ok=True)
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
        )
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        temporary_output_file = os.path.join(tabsserver_output_folder, "0.csv")
        assert os.path.isfile(temporary_output_file)
        # ToDo: Undo when https://github.com/pola-rs/polars/issues/21802 fix is available
        output = pl.read_csv(temporary_output_file, separator=",", eol_char="\n")
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_azure",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        blob_client = azure_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        local_file_path = os.path.join(tmp_path, "output.csv")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        # ToDo: Undo when https://github.com/pola-rs/polars/issues/21802 fix is available
        output = pl.read_csv(local_file_path, separator=",", eol_char="\n")
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_azure",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        if blob_client is not None:
            try:
                blob_client.delete_blob()
            except ResourceNotFoundError as e:
                logger.warning(
                    f"The blob to delete was' created or was already deleted: {e}"
                )


@pytest.mark.requires_internet
@pytest.mark.integration
@pytest.mark.slow
def test_output_azure_ndjson(tmp_path, azure_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_azure_ndjson = copy.deepcopy(output_azure_format_testing)
    container_name = "tabsdataci"
    blob_name = (
        "test_output/test_output_azure_ndjson_"
        f"{int(datetime.datetime.now().timestamp())}.ndjson"
    )
    blob_client = None
    output_file = f"az://{container_name}/{blob_name}"
    output_azure_ndjson.output.uri = output_file
    context_archive = create_bundle_archive(
        output_azure_ndjson,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, EXECUTION_CONTEXT_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_azure", "mock_table.parquet"
    )
    write_v1_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    os.makedirs(tabsserver_output_folder, exist_ok=True)
    try:
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
        )
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        temporary_output_file = os.path.join(tabsserver_output_folder, "0.ndjson")
        assert os.path.isfile(temporary_output_file)
        output = pl.read_ndjson(temporary_output_file)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_azure",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        blob_client = azure_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        local_file_path = os.path.join(tmp_path, "output.ndjson")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        output = pl.read_ndjson(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_azure",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

    finally:
        if blob_client is not None:
            try:
                blob_client.delete_blob()
            except ResourceNotFoundError as e:
                logger.warning(
                    f"The blob to delete wasn't created or was already deleted: {e}"
                )

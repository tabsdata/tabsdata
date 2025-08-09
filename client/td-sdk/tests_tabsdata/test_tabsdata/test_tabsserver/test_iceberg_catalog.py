#
# Copyright 2025 Tabs Data Inc.
#

import copy
import inspect
import logging
import os
import uuid
from io import StringIO
from unittest import mock

import numpy as np
import pandas as pd
import polars as pl
import pytest
from pyiceberg.catalog import load_catalog
from pyiceberg.transforms import YearTransform

from tabsdata._secret import _recursively_evaluate_secret
from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._tabsserver.invoker import invoke as tabsserver_main
from tabsdata._utils.bundle_utils import create_bundle_archive
from tests_tabsdata.bootest import TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    ABSOLUTE_TEST_FOLDER_LOCATION,
    FUNCTION_DATA_FOLDER,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
    write_v2_yaml_file,
)
from tests_tabsdata.testing_resources.test_output_s3_catalog.example import (
    output_s3_catalog,
)
from tests_tabsdata.testing_resources.test_output_s3_catalog_append.example import (
    output_s3_catalog_append,
)
from tests_tabsdata.testing_resources.test_output_s3_catalog_partition.example import (
    output_s3_catalog_partition,
)
from tests_tabsdata.testing_resources.test_output_s3_catalog_region_creds.example import (
    output_s3_catalog_region_creds,
)
from tests_tabsdata.testing_resources.test_output_s3_catalog_replace.example import (
    output_s3_catalog_replace,
)
from tests_tabsdata.testing_resources.test_output_s3_catalog_schema_strategy.example import (
    output_s3_catalog_schema_strategy,
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

pytestmark = pytest.mark.catalog


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_catalog(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_file_0 = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_catalog_"
        f"{uuid.uuid4()}_0.parquet"
    )
    output_file_1 = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_catalog_"
        f"{uuid.uuid4()}_1.parquet"
    )
    output_s3_catalog.output.uri = [
        output_file_0,
        output_file_1,
    ]
    catalog_definition = _recursively_evaluate_secret(
        output_s3_catalog.output.catalog.definition
    )
    catalog = load_catalog(**catalog_definition)
    namespace = f"testing_namespace_{uuid.uuid4()}"
    catalog.create_namespace(namespace)
    table_0 = f"{namespace}.s3_catalog_0"
    table_1 = f"{namespace}.s3_catalog_1"

    output_s3_catalog.output.catalog.tables = [table_0, table_1]
    output_s3_catalog.output.catalog.auto_create_at = [
        f"s3://tabsdata-us-east-1-catalog-metadata/{namespace}/s3_catalog_0",
        f"s3://tabsdata-us-east-1-catalog-metadata/{namespace}/s3_catalog_1",
    ]

    context_archive = create_bundle_archive(
        output_s3_catalog,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3_catalog", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")

    bucket_name = output_file_0.split("/")[2]
    file_name_0 = "/".join(output_file_0.split("/")[3:])
    file_name_1 = "/".join(output_file_1.split("/")[3:])
    try:
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

        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name_0, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3_catalog",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name_1, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        assert output.equals(expected_output)

        # Verify the catalog has the proper data
        table = catalog.load_table(table_0)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        assert output.equals(expected_output)

        table = catalog.load_table(table_1)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        assert output.equals(expected_output)

    finally:
        operations = [
            lambda: catalog.drop_table(table_0),
            lambda: catalog.drop_table(table_1),
            lambda: catalog.drop_namespace(namespace),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name_0),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name_1),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_catalog_replace(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)

    catalog_definition = _recursively_evaluate_secret(
        output_s3_catalog_replace.output.catalog.definition
    )
    catalog = load_catalog(**catalog_definition)
    namespace = f"testing_namespace_replace_{uuid.uuid4()}"
    catalog.create_namespace(namespace)
    table_name = f"{namespace}.s3_catalog_replace"
    output_s3_catalog_replace.output.catalog.tables = table_name
    output_s3_catalog_replace.output.catalog.auto_create_at = (
        f"s3://tabsdata-us-east-1-catalog-metadata/{namespace}/s3_catalog_replace"
    )

    files_to_delete = []
    try:
        for i in range(2):
            output_file = (
                "s3://tabsdata-testing-bucket/testing_output/test_output_s3_catalog_replace"
                f"{uuid.uuid4()}_{i}.parquet"
            )
            output_s3_catalog_replace.output.uri = output_file

            context_archive = create_bundle_archive(
                output_s3_catalog_replace,
                local_packages=LOCAL_PACKAGES_LIST,
                save_location=tmp_path,
            )

            input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
            response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
            os.makedirs(response_folder, exist_ok=True)
            mock_parquet_table = os.path.join(
                TESTING_RESOURCES_FOLDER,
                "test_output_s3_catalog_replace",
                "mock_table.parquet",
            )
            function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
            write_v2_yaml_file(
                input_yaml_file,
                context_archive,
                [mock_parquet_table],
                function_data_path=function_data_folder,
            )
            tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")

            bucket_name = output_file.split("/")[2]
            file_name = "/".join(output_file.split("/")[3:])
            files_to_delete.append(file_name)

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

            copy_destination = os.path.join(tmp_path, "output.parquet")
            s3_client.download_file(bucket_name, file_name, copy_destination)
            output = pl.read_parquet(copy_destination)
            output = clean_polars_df(output)
            expected_output_file = os.path.join(
                TESTING_RESOURCES_FOLDER,
                "test_output_s3_catalog_replace",
                "expected_result.json",
            )
            expected_output = read_json_and_clean(expected_output_file)
            assert output.equals(expected_output)

        # Verify the catalog has the proper data
        table = catalog.load_table(table_name)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3_catalog",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert len(output) == len(expected_output)

    finally:
        operations = [
            lambda: catalog.drop_table(table_name),
            lambda: catalog.drop_namespace(namespace),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass

        for file in files_to_delete:
            s3_client.delete_object(Bucket=bucket_name, Key=file)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_catalog_append(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)

    catalog_definition = _recursively_evaluate_secret(
        output_s3_catalog_append.output.catalog.definition
    )
    catalog = load_catalog(**catalog_definition)
    namespace = f"testing_namespace_append_{uuid.uuid4()}"
    catalog.create_namespace(namespace)
    table_name = f"{namespace}.s3_catalog_append"
    output_s3_catalog_append.output.catalog.tables = table_name
    output_s3_catalog_append.output.catalog.auto_create_at = (
        f"s3://tabsdata-us-east-1-catalog-metadata/{namespace}/s3_catalog_append"
    )

    files_to_delete = []
    try:
        for i in range(2):
            output_file = (
                "s3://tabsdata-testing-bucket/testing_output/test_output_s3_catalog_append"
                f"{uuid.uuid4()}_{i}.parquet"
            )
            output_s3_catalog_append.output.uri = output_file

            context_archive = create_bundle_archive(
                output_s3_catalog_append,
                local_packages=LOCAL_PACKAGES_LIST,
                save_location=tmp_path,
            )

            input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
            response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
            os.makedirs(response_folder, exist_ok=True)
            mock_parquet_table = os.path.join(
                TESTING_RESOURCES_FOLDER,
                "test_output_s3_catalog_append",
                "mock_table.parquet",
            )
            function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
            write_v2_yaml_file(
                input_yaml_file,
                context_archive,
                [mock_parquet_table],
                function_data_path=function_data_folder,
            )
            tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")

            bucket_name = output_file.split("/")[2]
            file_name = "/".join(output_file.split("/")[3:])
            files_to_delete.append(file_name)

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

            copy_destination = os.path.join(tmp_path, "output.parquet")
            s3_client.download_file(bucket_name, file_name, copy_destination)
            output = pl.read_parquet(copy_destination)
            output = clean_polars_df(output)
            expected_output_file = os.path.join(
                TESTING_RESOURCES_FOLDER,
                "test_output_s3_catalog_append",
                "expected_result.json",
            )
            expected_output = read_json_and_clean(expected_output_file)
            assert output.equals(expected_output)

        # Verify the catalog has the proper data
        table = catalog.load_table(table_name)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3_catalog",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert len(output) == 2 * len(expected_output)

    finally:
        operations = [
            lambda: catalog.drop_table(table_name),
            lambda: catalog.drop_namespace(namespace),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass

        for file in files_to_delete:
            s3_client.delete_object(Bucket=bucket_name, Key=file)


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_catalog_no_auto_create_at_fails(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_catalog_no_auto_create_at_fails = copy.deepcopy(output_s3_catalog)
    output_file_0 = (
        "s3://tabsdata-testing-bucket/testing_output"
        "/test_output_s3_catalog_no_autocreate_"
        f"{uuid.uuid4()}_0.parquet"
    )
    output_file_1 = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_catalog_no_autocreate_"
        f"{uuid.uuid4()}_1.parquet"
    )
    output_s3_catalog_no_auto_create_at_fails.output.uri = [
        output_file_0,
        output_file_1,
    ]
    catalog_definition = _recursively_evaluate_secret(
        output_s3_catalog_no_auto_create_at_fails.output.catalog.definition
    )
    catalog = load_catalog(**catalog_definition)
    namespace = f"testing_namespace_no_auto_create_at_fails_{uuid.uuid4()}"
    catalog.create_namespace(namespace)
    table_0 = f"{namespace}.s3_catalog_no_autocreate_0"
    table_1 = f"{namespace}.s3_catalog_no_autocreate_1"

    output_s3_catalog_no_auto_create_at_fails.output.catalog.tables = [table_0, table_1]
    output_s3_catalog_no_auto_create_at_fails.output.catalog.auto_create_at = None

    context_archive = create_bundle_archive(
        output_s3_catalog_no_auto_create_at_fails,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3_catalog", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")

    bucket_name = output_file_0.split("/")[2]
    file_name_0 = "/".join(output_file_0.split("/")[3:])
    file_name_1 = "/".join(output_file_1.split("/")[3:])
    try:
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
            temp_cwd=True,
        )
        assert result != 0
    finally:
        operations = [
            lambda: catalog.drop_table(table_0),
            lambda: catalog.drop_table(table_1),
            lambda: catalog.drop_namespace(namespace),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name_0),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name_1),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_catalog_schema_update(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_catalog_schema_update = copy.deepcopy(output_s3_catalog_schema_strategy)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output"
        "/test_output_s3_catalog_schema_update_"
        f"{uuid.uuid4()}.parquet"
    )
    output_s3_catalog_schema_update.output.uri = output_file
    catalog_definition = _recursively_evaluate_secret(
        output_s3_catalog_schema_update.output.catalog.definition
    )
    catalog = load_catalog(**catalog_definition)
    namespace = f"testing_namespace_schema_update_{uuid.uuid4()}"
    catalog.create_namespace(namespace)
    table_name = f"{namespace}.s3_catalog_schema_update"

    output_s3_catalog_schema_update.output.catalog.tables = table_name
    output_s3_catalog_schema_update.output.catalog.schema_strategy = "update"

    context_archive = create_bundle_archive(
        output_s3_catalog_schema_update,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    table_location = (
        f"s3://tabsdata-us-east-1-catalog-metadata/{namespace}/s3_catalog_schema_update"
    )
    example_df = pl.DataFrame({"first_column": [1, 2, 3], "second_column": [4, 5, 6]})
    pyarrow_table = example_df.to_arrow()
    catalog.create_table(
        identifier=table_name,
        location=table_location,
        schema=pyarrow_table.schema,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_s3_catalog_schema_strategy",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")

    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    try:
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

        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3_catalog_schema_strategy",
            "expected_result_bucket.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        # Verify the catalog has the proper data
        table = catalog.load_table(table_name)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3_catalog_schema_strategy",
            "expected_result_catalog.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        operations = [
            lambda: catalog.drop_table(table_name),
            lambda: catalog.drop_namespace(namespace),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_catalog_schema_strict(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_s3_catalog_schema_strict = copy.deepcopy(output_s3_catalog_schema_strategy)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output"
        "/test_output_s3_catalog_schema_strict_"
        f"{uuid.uuid4()}.parquet"
    )
    output_s3_catalog_schema_strict.output.uri = output_file
    catalog_definition = _recursively_evaluate_secret(
        output_s3_catalog_schema_strict.output.catalog.definition
    )
    catalog = load_catalog(**catalog_definition)
    namespace = f"testing_namespace_schema_strict_{uuid.uuid4()}"
    catalog.create_namespace(namespace)
    table_name = f"{namespace}.s3_catalog_schema_strict"

    output_s3_catalog_schema_strict.output.catalog.tables = table_name
    output_s3_catalog_schema_strict.output.catalog.schema_strategy = "strict"

    context_archive = create_bundle_archive(
        output_s3_catalog_schema_strict,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    table_location = (
        f"s3://tabsdata-us-east-1-catalog-metadata/{namespace}/s3_catalog_schema_strict"
    )
    example_df = pl.DataFrame({"first_column": [1, 2, 3], "second_column": [4, 5, 6]})
    pyarrow_table = example_df.to_arrow()
    catalog.create_table(
        identifier=table_name,
        location=table_location,
        schema=pyarrow_table.schema,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_s3_catalog_schema_strategy",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")

    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    try:
        environment_name, result = tabsserver_main(
            tmp_path,
            response_folder,
            tabsserver_output_folder,
            environment_prefix=PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
            logs_folder=logs_folder,
            temp_cwd=True,
        )
        assert result != 0
    finally:
        operations = [
            lambda: catalog.drop_table(table_name),
            lambda: catalog.drop_namespace(namespace),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_catalog_partition(tmp_path, s3_client):

    from tests_tabsdata.testing_resources.test_output_s3_catalog_partition.example import (
        NUMBER_OF_PARTITIONS,
    )

    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_file = (
        "s3://tabsdata-testing-bucket/testing_output"
        "/test_output_s3_catalog_partition_"
        f"{uuid.uuid4()}_$FRAGMENT_IDX.parquet"
    )
    output_s3_catalog_partition.output.uri = output_file
    catalog_definition = _recursively_evaluate_secret(
        output_s3_catalog_partition.output.catalog.definition
    )
    catalog = load_catalog(**catalog_definition)
    namespace = f"testing_namespace_partition_{uuid.uuid4()}"
    catalog.create_namespace(namespace)
    table_name = f"{namespace}.s3_catalog_partition"

    output_s3_catalog_partition.output.catalog.tables = table_name

    context_archive = create_bundle_archive(
        output_s3_catalog_partition,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    table_location = (
        f"s3://tabsdata-us-east-1-catalog-metadata/{namespace}/s3_catalog_partition"
    )
    date_range = pd.date_range(start="1900-01-01", end=pd.Timestamp.now(), freq="D")
    random_dates = np.random.choice(date_range, size=10)
    random_numbers = np.random.rand(10)
    example_df = pl.DataFrame(
        {"timestamp": random_dates, "random_number": random_numbers}
    )
    example_df = example_df.with_columns(
        example_df["timestamp"].cast(pl.Datetime("us"))
    )

    pyarrow_table = example_df.to_arrow()
    iceberg_table = catalog.create_table(
        identifier=table_name,
        location=table_location,
        schema=pyarrow_table.schema,
    )

    with iceberg_table.update_spec() as update:
        update.add_field("timestamp", YearTransform())

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_s3_catalog_partition",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")

    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    try:
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

        for i in range(NUMBER_OF_PARTITIONS):
            copy_destination = os.path.join(tmp_path, "output.parquet")
            s3_client.download_file(
                bucket_name,
                file_name.replace("$FRAGMENT_IDX", str(i)),
                copy_destination,
            )
            output = pl.read_parquet(copy_destination)
            assert len(output) == 1

        # Verify the catalog has the proper data
        table = catalog.load_table(table_name)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        assert len(output) == NUMBER_OF_PARTITIONS

    finally:
        operations = [
            lambda: catalog.drop_table(table_name),
            lambda: catalog.drop_namespace(namespace),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass

        for i in range(NUMBER_OF_PARTITIONS):
            try:
                s3_client.delete_object(
                    Bucket=bucket_name, Key=file_name.replace("$FRAGMENT_IDX", str(i))
                )
            except:
                pass


@pytest.mark.integration
@pytest.mark.requires_internet
@pytest.mark.slow
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_s3_catalog_region_creds(tmp_path, s3_client):
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    output_file_0 = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_catalog_region_creds_"
        f"{uuid.uuid4()}_0.parquet"
    )
    output_file_1 = (
        "s3://tabsdata-testing-bucket/testing_output/test_output_s3_catalog_region_creds_"
        f"{uuid.uuid4()}_1.parquet"
    )
    output_s3_catalog_region_creds.output.uri = [
        output_file_0,
        output_file_1,
    ]
    catalog_definition = _recursively_evaluate_secret(
        output_s3_catalog_region_creds.output.catalog.definition
    )
    catalog = load_catalog(**catalog_definition)
    namespace = f"testing_namespace_region_creds_{uuid.uuid4()}"
    catalog.create_namespace(namespace)
    table_0 = f"{namespace}.s3_catalog_region_creds_0"
    table_1 = f"{namespace}.s3_catalog_region_creds_1"

    output_s3_catalog_region_creds.output.catalog.tables = [table_0, table_1]
    output_s3_catalog_region_creds.output.catalog.auto_create_at = [
        (
            "s3://tabsdata-us-east-1-catalog-metadata/"
            f"{namespace}/s3_catalog_region_creds_0"
        ),
        (
            "s3://tabsdata-us-east-1-catalog-metadata/"
            f"{namespace}/s3_catalog_region_creds_1"
        ),
    ]

    context_archive = create_bundle_archive(
        output_s3_catalog_region_creds,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER,
        "test_output_s3_catalog_region_creds",
        "mock_table.parquet",
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")

    bucket_name = output_file_0.split("/")[2]
    file_name_0 = "/".join(output_file_0.split("/")[3:])
    file_name_1 = "/".join(output_file_1.split("/")[3:])
    try:
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

        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name_0, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3_catalog_region_creds",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name_1, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        assert output.equals(expected_output)

        # Verify the catalog has the proper data
        table = catalog.load_table(table_0)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        assert output.equals(expected_output)

        table = catalog.load_table(table_1)
        output = pl.DataFrame(table.scan().to_arrow())
        output = clean_polars_df(output)
        assert output.equals(expected_output)

    finally:
        operations = [
            lambda: catalog.drop_table(table_0),
            lambda: catalog.drop_table(table_1),
            lambda: catalog.drop_namespace(namespace),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name_0),
            lambda: s3_client.delete_object(Bucket=bucket_name, Key=file_name_1),
        ]

        for operation in operations:
            try:
                operation()
            except:
                pass

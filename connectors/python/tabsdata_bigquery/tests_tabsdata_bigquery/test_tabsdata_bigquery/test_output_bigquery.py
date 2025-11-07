#
# Copyright 2025 Tabs Data Inc.
#

import inspect
import logging
import os
import uuid
from io import StringIO
from unittest import mock

import polars as pl

# noinspection PyPackageRequirements
import pytest

import tabsdata as td
from tabsdata._tabsserver.function.response_utils import RESPONSE_FILE_NAME
from tabsdata._tabsserver.invoker import REQUEST_FILE_NAME
from tabsdata._tabsserver.invoker import invoke as tabsserver_main
from tabsdata._utils.bundle_utils import create_bundle_archive

# noinspection PyProtectedMember
from tests_tabsdata.bootest import ROOT_FOLDER, TDLOCAL_FOLDER
from tests_tabsdata.conftest import (
    FUNCTION_DATA_FOLDER,
    LOCAL_PACKAGES_LIST,
    PYTEST_DEFAULT_ENVIRONMENT_PREFIX,
    clean_polars_df,
    get_lf,
    read_json_and_clean,
    write_v2_yaml_file,
)
from tests_tabsdata_bigquery.conftest import TESTING_RESOURCES_FOLDER
from tests_tabsdata_bigquery.testing_resources.test_multiple_outputs_bigquery.example import (
    testing_tabsdata_function as multiple_outputs_bigquery_generator,
)
from tests_tabsdata_bigquery.testing_resources.test_output_bigquery.example import (
    testing_tabsdata_function as output_bigquery_generator,
)
from tests_tabsdata_bigquery.testing_resources.test_output_bigquery_none.example import (
    testing_tabsdata_function as output_bigquery_none_generator,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ROOT_PROJECT_DIR = ROOT_FOLDER
RESPONSE_FOLDER = "response_folder"

LOCAL_DEV_FOLDER = TDLOCAL_FOLDER


BIGQUERY_BUDGET_SAFETY_TIMEOUT = 600


def download_bigquery_table_as_df(client, full_table_name: str) -> pl.DataFrame:
    query = f"SELECT * FROM `{full_table_name}`"
    query_job = client.query(query)
    results = query_job.result()
    rows = [dict(row) for row in results]
    return pl.DataFrame(rows, infer_schema_length=25000)


@pytest.mark.bigquery
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_default_options(bigquery_config):
    folder = bigquery_config["GCS_FOLDER"]
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    tables = ["table1", "table2"]
    output = td.BigQueryDest(
        td.BigQueryConn(
            folder,
            credentials,
            project=project,
            dataset=dataset,
        ),
        tables,
    )
    assert output.tables == [f"{project}.{dataset}.{table}" for table in tables]
    assert output.conn.gcs_folder == folder
    assert output.conn.credentials == credentials
    assert output.conn.project == project
    assert output.conn.dataset == dataset
    assert output.conn.enforce_connection_params is True
    assert output.conn.cx_dst_configs_bigquery == {}
    assert output.conn.cx_dst_configs_gcs == {}
    assert output.schema_strategy == "update"
    assert output.if_table_exists == "append"


@pytest.mark.bigquery
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_table_none(bigquery_config):
    folder = bigquery_config["GCS_FOLDER"]
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    tables = None
    output = td.BigQueryDest(
        td.BigQueryConn(
            folder,
            credentials,
            project=project,
            dataset=dataset,
        ),
        tables,
    )
    assert output.tables is None


@pytest.mark.bigquery
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_single_table(bigquery_config):
    folder = bigquery_config["GCS_FOLDER"]
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    tables = "table1"
    output = td.BigQueryDest(
        td.BigQueryConn(
            folder,
            credentials,
            project=project,
            dataset=dataset,
        ),
        tables,
    )
    assert output.tables == [f"{project}.{dataset}.{tables}"]


@pytest.mark.bigquery
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_single_table_no_project_fails(bigquery_config):
    folder = bigquery_config["GCS_FOLDER"]
    credentials = bigquery_config["CREDENTIALS"]
    dataset = bigquery_config["DATASET"]
    tables = "table1"
    with pytest.raises(ValueError):
        td.BigQueryDest(
            td.BigQueryConn(
                folder,
                credentials,
                project=None,
                dataset=dataset,
            ),
            tables,
        )


@pytest.mark.bigquery
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_single_table_no_dataset_fails(bigquery_config):
    folder = bigquery_config["GCS_FOLDER"]
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    tables = "table1"
    with pytest.raises(ValueError):
        td.BigQueryDest(
            td.BigQueryConn(
                folder,
                credentials,
                project=project,
                dataset=None,
            ),
            tables,
        )


@pytest.mark.bigquery
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_tables_no_project_fails(bigquery_config):
    folder = bigquery_config["GCS_FOLDER"]
    credentials = bigquery_config["CREDENTIALS"]
    dataset = bigquery_config["DATASET"]
    tables = ["project.dataset.table0", "table1"]
    with pytest.raises(ValueError):
        td.BigQueryDest(
            td.BigQueryConn(
                folder,
                credentials,
                project=None,
                dataset=dataset,
            ),
            tables,
        )


@pytest.mark.bigquery
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_tables_no_dataset_fails(bigquery_config):
    folder = bigquery_config["GCS_FOLDER"]
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    tables = ["project.dataset.table0", "table1"]
    with pytest.raises(ValueError):
        td.BigQueryDest(
            td.BigQueryConn(
                folder,
                credentials,
                project=project,
                dataset=None,
            ),
            tables,
        )


@pytest.mark.bigquery
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_connection_with_capital_uri(bigquery_config):
    folder = bigquery_config["GCS_FOLDER"].upper()
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    conn = td.BigQueryConn(
        folder,
        credentials,
        project=project,
        dataset=dataset,
    )
    assert conn.gcs_folder == bigquery_config["GCS_FOLDER"].upper()


@pytest.mark.bigquery
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_connection_with_wrong_uri_fails(bigquery_config):
    folder = "s3://some-bucket/folder"
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    with pytest.raises(ValueError):
        td.BigQueryConn(
            folder,
            credentials,
            project=project,
            dataset=dataset,
        )


@pytest.mark.bigquery
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_multiple_different_tables_enforce_false(bigquery_config):
    folder = bigquery_config["GCS_FOLDER"]
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    tables = [
        "project.dataset.table0",
        "table1",
        "another_project.another_dataset.table2",
        "dataset.table3",
    ]
    expected_tables = [
        "project.dataset.table0",
        f"{project}.{dataset}.table1",
        "another_project.another_dataset.table2",
        f"{project}.dataset.table3",
    ]
    output = td.BigQueryDest(
        td.BigQueryConn(
            folder,
            credentials,
            project=project,
            dataset=dataset,
            enforce_connection_params=False,
        ),
        tables,
    )
    assert output.tables == expected_tables


@pytest.mark.bigquery
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_multiple_different_tables_enforce_true(bigquery_config):
    folder = bigquery_config["GCS_FOLDER"]
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    tables = [
        "project.dataset.table0",
        "table1",
        "another_project.another_dataset.table2",
        "dataset.table3",
    ]
    with pytest.raises(ValueError):
        td.BigQueryDest(
            td.BigQueryConn(
                folder,
                credentials,
                project=project,
                dataset=dataset,
                enforce_connection_params=True,
            ),
            tables,
        )


@pytest.mark.bigquery
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_class_initialization_all_options(bigquery_config):
    folder = bigquery_config["GCS_FOLDER"]
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    tables = ["table1", "table2"]
    cx_dst_configs_gcs = {"key1": "value1"}
    cx_dst_configs_bigquery = {"key2": "value2"}
    output = td.BigQueryDest(
        td.BigQueryConn(
            folder,
            credentials,
            project=project,
            dataset=dataset,
            enforce_connection_params=False,
            cx_dst_configs_gcs=cx_dst_configs_gcs,
            cx_dst_configs_bigquery=cx_dst_configs_bigquery,
        ),
        tables,
        if_table_exists="replace",
        schema_strategy="strict",
    )
    assert output.tables == [f"{project}.{dataset}.{table}" for table in tables]
    assert output.conn.gcs_folder == folder
    assert output.conn.credentials == credentials
    assert output.conn.project == project
    assert output.conn.dataset == dataset
    assert output.conn.enforce_connection_params is False
    assert output.conn.cx_dst_configs_bigquery == cx_dst_configs_bigquery
    assert output.conn.cx_dst_configs_gcs == cx_dst_configs_gcs
    assert output.schema_strategy == "strict"
    assert output.if_table_exists == "replace"


@pytest.mark.bigquery
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
def test_stream_different_len_raises_error(
    tmp_path,
    bigquery_config,
    size,
    bigquery_client,
):
    folder = bigquery_config["GCS_FOLDER"]
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    tables = ["table1", "table2"]
    lf = get_lf(size)
    output = td.BigQueryDest(
        td.BigQueryConn(
            folder,
            credentials,
            project=project,
            dataset=dataset,
        ),
        tables,
    )
    try:
        with pytest.raises(ValueError):
            output.stream(str(tmp_path), lf)

        with pytest.raises(ValueError):
            output.stream(str(tmp_path), lf, lf, lf)
    finally:
        # noinspection PyBroadException
        try:
            bigquery_client.delete_table(
                f"{project}.{dataset}.table1", not_found_ok=True
            )
        except Exception:
            pass
        # noinspection PyBroadException
        try:
            bigquery_client.delete_table(
                f"{project}.{dataset}.table2", not_found_ok=True
            )
        except Exception:
            pass


@pytest.mark.bigquery
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_bigquery(tmp_path, bigquery_config, bigquery_client):
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    folder = bigquery_config["GCS_FOLDER"]
    table = f"test_output_bigquery_table_{uuid.uuid4().hex[:16]}"
    output_bigquery = output_bigquery_generator(
        folder, credentials, project, dataset, table
    )
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_bigquery,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    full_table_name = f"{project}.{dataset}.{table}"

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_bigquery", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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
    try:
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_bigquery",
            "expected_result.json",
        )
        output = download_bigquery_table_as_df(bigquery_client, full_table_name)
        expected_output = read_json_and_clean(expected_output_file)
        assert expected_output.equals(clean_polars_df(output))
    finally:
        # noinspection PyBroadException
        try:
            bigquery_client.delete_table(full_table_name, not_found_ok=True)
        except Exception:
            pass


@pytest.mark.bigquery
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_bigquery_replace(tmp_path, bigquery_config, bigquery_client):
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    folder = bigquery_config["GCS_FOLDER"]
    table = f"test_output_bigquery_table_replace_{uuid.uuid4().hex[:16]}"
    output_bigquery = output_bigquery_generator(
        folder,
        credentials,
        project,
        dataset,
        table,
        if_table_exists="replace",
    )
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_bigquery,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    full_table_name = f"{project}.{dataset}.{table}"

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_bigquery", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    os.makedirs(tabsserver_output_folder, exist_ok=True)
    try:
        for _ in range(2):
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

            expected_output_file = os.path.join(
                TESTING_RESOURCES_FOLDER,
                "test_output_bigquery",
                "expected_result.json",
            )
            output = download_bigquery_table_as_df(bigquery_client, full_table_name)
            expected_output = read_json_and_clean(expected_output_file)
            assert expected_output.equals(clean_polars_df(output))
    finally:
        # noinspection PyBroadException
        try:
            bigquery_client.delete_table(full_table_name, not_found_ok=True)
        except Exception:
            pass


@pytest.mark.bigquery
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_bigquery_append(tmp_path, bigquery_config, bigquery_client):
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    folder = bigquery_config["GCS_FOLDER"]
    table = f"test_output_bigquery_table_append_{uuid.uuid4().hex[:16]}"
    output_bigquery = output_bigquery_generator(
        folder,
        credentials,
        project,
        dataset,
        table,
        if_table_exists="append",
    )
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_bigquery,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    full_table_name = f"{project}.{dataset}.{table}"

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_bigquery", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
        function_data_path=function_data_folder,
    )
    tabsserver_output_folder = os.path.join(tmp_path, "tabsserver_output")
    os.makedirs(tabsserver_output_folder, exist_ok=True)
    try:
        for i in range(2):
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

            expected_output_file = os.path.join(
                TESTING_RESOURCES_FOLDER,
                "test_output_bigquery",
                "expected_result.json",
            )
            output = download_bigquery_table_as_df(bigquery_client, full_table_name)
            output = clean_polars_df(output)
            expected_output = read_json_and_clean(expected_output_file)
            assert output.height == expected_output.height * (i + 1)
    finally:
        # noinspection PyBroadException
        try:
            bigquery_client.delete_table(full_table_name, not_found_ok=True)
        except Exception:
            pass


@pytest.mark.bigquery
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_bigquery_schema_strict(tmp_path, bigquery_config, bigquery_client):
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    folder = bigquery_config["GCS_FOLDER"]
    table = f"test_output_bigquery_table_strict_{uuid.uuid4().hex[:16]}"
    output_bigquery = output_bigquery_generator(
        folder,
        credentials,
        project,
        dataset,
        table,
        schema_strategy="strict",
    )
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_bigquery,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    full_table_name = f"{project}.{dataset}.{table}"

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    try:
        for i in range(2):
            mocked_table_name = (
                "mock_table_small_schema.parquet" if i == 0 else "mock_table.parquet"
            )
            mock_parquet_table = os.path.join(
                TESTING_RESOURCES_FOLDER, "test_output_bigquery", mocked_table_name
            )
            function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
            write_v2_yaml_file(
                input_yaml_file,
                context_archive,
                [mock_parquet_table],
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
            if i == 0:
                assert result == 0
                assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

                expected_output_file = os.path.join(
                    TESTING_RESOURCES_FOLDER,
                    "test_output_bigquery",
                    "expected_result.json",
                )
                output = download_bigquery_table_as_df(bigquery_client, full_table_name)
                output = clean_polars_df(output)
                expected_output = read_json_and_clean(expected_output_file)
                assert output.height == expected_output.height * (i + 1)
            else:
                assert result != 0
    finally:
        # noinspection PyBroadException
        try:
            bigquery_client.delete_table(full_table_name, not_found_ok=True)
        except Exception:
            pass


@pytest.mark.bigquery
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_bigquery_schema_update(tmp_path, bigquery_config, bigquery_client):
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    folder = bigquery_config["GCS_FOLDER"]
    table = f"test_output_bigquery_table_update_{uuid.uuid4().hex[:16]}"
    output_bigquery = output_bigquery_generator(
        folder,
        credentials,
        project,
        dataset,
        table,
        schema_strategy="update",
    )
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_bigquery,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    full_table_name = f"{project}.{dataset}.{table}"

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    try:
        for i in range(2):
            mocked_table_name = (
                "mock_table_small_schema.parquet" if i == 0 else "mock_table.parquet"
            )
            mock_parquet_table = os.path.join(
                TESTING_RESOURCES_FOLDER, "test_output_bigquery", mocked_table_name
            )
            function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
            write_v2_yaml_file(
                input_yaml_file,
                context_archive,
                [mock_parquet_table],
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

            expected_output_file = os.path.join(
                TESTING_RESOURCES_FOLDER,
                "test_output_bigquery",
                "expected_result.json",
            )
            output = download_bigquery_table_as_df(bigquery_client, full_table_name)
            output = clean_polars_df(output)
            expected_output = read_json_and_clean(expected_output_file)
            assert output.height == expected_output.height * (i + 1)
    finally:
        # noinspection PyBroadException
        try:
            bigquery_client.delete_table(full_table_name, not_found_ok=True)
        except Exception:
            pass


@pytest.mark.bigquery
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_multiple_outputs_bigquery(tmp_path, bigquery_config, bigquery_client):
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    folder = bigquery_config["GCS_FOLDER"]
    table1 = f"test_multiple_outputs_bigquery_table_1_{uuid.uuid4().hex[:16]}"
    table2 = f"test_multiple_outputs_bigquery_table_2_{uuid.uuid4().hex[:16]}"
    tables = [table1, table2]
    output_bigquery = multiple_outputs_bigquery_generator(
        folder, credentials, project, dataset, tables
    )
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_bigquery,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    full_table1_name = f"{project}.{dataset}.{table1}"
    full_table2_name = f"{project}.{dataset}.{table2}"

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_multiple_outputs_bigquery", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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
    try:
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_bigquery",
            "expected_result.json",
        )
        output = download_bigquery_table_as_df(bigquery_client, full_table1_name)
        expected_output = read_json_and_clean(expected_output_file)
        assert expected_output.equals(clean_polars_df(output))

        output = download_bigquery_table_as_df(bigquery_client, full_table2_name)
        assert expected_output.equals(clean_polars_df(output))
    finally:
        # noinspection PyBroadException
        try:
            bigquery_client.delete_table(full_table1_name, not_found_ok=True)
        except Exception:
            pass
        try:
            bigquery_client.delete_table(full_table2_name, not_found_ok=True)
        except Exception:
            pass


@pytest.mark.bigquery
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_bigquery_with_none(tmp_path, bigquery_config, bigquery_client):
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    folder = bigquery_config["GCS_FOLDER"]
    table = f"test_output_bigquery_none_table_{uuid.uuid4().hex[:16]}"
    output_bigquery = output_bigquery_none_generator(
        folder, credentials, project, dataset, table
    )
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_bigquery,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    full_table_name = f"{project}.{dataset}.{table}"

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_bigquery", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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
    try:
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        with pytest.raises(Exception):
            download_bigquery_table_as_df(bigquery_client, full_table_name)
    finally:
        # noinspection PyBroadException
        try:
            bigquery_client.delete_table(full_table_name, not_found_ok=True)
        except Exception:
            pass


@pytest.mark.bigquery
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tabsserver
@pytest.mark.timeout(BIGQUERY_BUDGET_SAFETY_TIMEOUT)
@mock.patch("sys.stdin", StringIO("FAKE_PREFIX_ROOT: FAKE_VALUE\n"))
def test_output_bigquery_none_table_name(tmp_path, bigquery_config, bigquery_client):
    credentials = bigquery_config["CREDENTIALS"]
    project = bigquery_config["PROJECT"]
    dataset = bigquery_config["DATASET"]
    folder = bigquery_config["GCS_FOLDER"]
    table = f"test_output_bigquery_none_table_name_table_{uuid.uuid4().hex[:16]}"
    output_bigquery = output_bigquery_generator(
        folder,
        credentials,
        project,
        dataset,
        None,
        input_table=f"collection/{table}",
    )
    logs_folder = os.path.join(LOCAL_DEV_FOLDER, inspect.currentframe().f_code.co_name)
    context_archive = create_bundle_archive(
        output_bigquery,
        local_packages=LOCAL_PACKAGES_LIST,
        save_location=tmp_path,
    )

    full_table_name = f"{project}.{dataset}.{table}"

    input_yaml_file = os.path.join(tmp_path, REQUEST_FILE_NAME)
    response_folder = os.path.join(tmp_path, RESPONSE_FOLDER)
    os.makedirs(response_folder, exist_ok=True)
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_bigquery", "mock_table.parquet"
    )
    function_data_folder = os.path.join(tmp_path, FUNCTION_DATA_FOLDER)
    write_v2_yaml_file(
        input_yaml_file,
        context_archive,
        [mock_parquet_table],
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
    try:
        assert result == 0
        assert os.path.exists(os.path.join(response_folder, RESPONSE_FILE_NAME))

        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_bigquery",
            "expected_result.json",
        )
        output = download_bigquery_table_as_df(bigquery_client, full_table_name)
        expected_output = read_json_and_clean(expected_output_file)
        assert expected_output.equals(clean_polars_df(output))
    finally:
        # noinspection PyBroadException
        try:
            bigquery_client.delete_table(full_table_name, not_found_ok=True)
        except Exception:
            pass

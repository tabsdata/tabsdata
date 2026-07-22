#
# Copyright 2026 Tabsdata Inc.
#

"""
End-to-end tests for NamedConn resolution in the function runner.

These tests verify that when a source or destination plugin uses a NamedConn,
the backend resolves it to a concrete connection before executing the function.
Uses tdfnrun for full pipeline execution with a real MySQL database.
"""

import datetime
import json
import logging
import os

import polars as pl
import pytest
from azure.core.exceptions import ResourceNotFoundError

from tabsdata._io.connections.named_connection.kind_registry import kind_for
from tabsdata._tabsserver.function.named_connection_utils import (
    CONNECTIONS_FOLDER,
    TD_URI_REPOSITORY,
)
from test_tabsdata.conftest import (
    MOCK_PROJECT_ID,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.fntest import tdfnrun
from test_tabsdata.test_input_new_azure_named_conn.example import (
    testing_tabsdata_function as input_azure_named_conn_generator,
)
from test_tabsdata.test_input_new_gcs_named_conn.example import (
    testing_tabsdata_function as input_gcs_named_conn_generator,
)
from test_tabsdata.test_input_new_mysql_named_conn.example import (
    testing_tabsdata_function as input_named_conn_generator,
)
from test_tabsdata.test_input_new_s3_named_conn.example import (
    testing_tabsdata_function as input_s3_named_conn_generator,
)
from test_tabsdata.test_output_new_azure_named_conn.example import (
    testing_tabsdata_function as output_azure_named_conn_generator,
)
from test_tabsdata.test_output_new_gcs_named_conn.example import (
    testing_tabsdata_function as output_gcs_named_conn_generator,
)
from test_tabsdata.test_output_new_mysql_named_conn.example import (
    testing_tabsdata_function as output_named_conn_generator,
)
from test_tabsdata.test_output_new_s3_named_conn.example import (
    testing_tabsdata_function as output_s3_named_conn_generator,
)

# noinspection PyUnresolvedReferences
from . import pytestmark as _package_pytestmark  # noqa: F401

pytestmark = [_package_pytestmark, pytest.mark.xdist_group("platform-named_conn_e2e_seq")]

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

NAMED_CONN_NAME = "test_mysql_named"
S3_NAMED_CONN_NAME = "test_s3_named"
AZURE_NAMED_CONN_NAME = "test_azure_named"
GCS_NAMED_CONN_NAME = "test_gcs_named"


def _write_named_conn_repository(tmp_path, name, conn_type, conn_configs, conn_mode):
    """Create a temporary repository with a single named-connection JSON."""
    repo_dir = os.path.join(str(tmp_path), "repository")
    connections_dir = os.path.join(repo_dir, CONNECTIONS_FOLDER, MOCK_PROJECT_ID)
    os.makedirs(connections_dir, exist_ok=True)

    cx_dict = {
        "kind": kind_for(conn_type, conn_mode),
        "apiVersion": "1.0",
        "name": name,
        "conn_configs": conn_configs,
    }
    connection_file = os.path.join(connections_dir, f"{name.lower()}.json")
    with open(connection_file, "w") as f:
        json.dump(cx_dict, f)

    return repo_dir


def _setup_named_conn_repository(tmp_path, mysql_config, conn_mode):
    """Create a temporary repository with a MySQL named connection JSON."""
    return _write_named_conn_repository(
        tmp_path,
        NAMED_CONN_NAME,
        "MySQLConn",
        {
            "uri": mysql_config.uri,
            "credentials": {
                "user": mysql_config.user,
                "password": mysql_config.password,
            },
        },
        conn_mode,
    )


@pytest.mark.mysql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_mysql_with_named_conn(mysql_config, tmp_path, monkeypatch):
    repo_dir = _setup_named_conn_repository(tmp_path, mysql_config, "src")
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    testing_function = input_named_conn_generator(conn_name=NAMED_CONN_NAME)

    fnrun = tdfnrun(
        function=testing_function,
        input_tables=None,
    )
    assert fnrun.status == 0

    # Reuse expected results from the regular MySQL input test
    expected_output_file1 = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_input_new_mysql", "expected_result1.json"
    )
    expected_output_file2 = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_input_new_mysql", "expected_result2.json"
    )

    output1 = clean_polars_df(pl.read_parquet(fnrun.output[0].path))
    expected1 = read_json_and_clean(expected_output_file1)
    assert output1.equals(expected1)

    output2 = clean_polars_df(pl.read_parquet(fnrun.output[1].path))
    expected2 = read_json_and_clean(expected_output_file2)
    assert output2.equals(expected2)


@pytest.mark.mysql
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_mysql_with_named_conn(mysql_config, tmp_path, monkeypatch):
    repo_dir = _setup_named_conn_repository(tmp_path, mysql_config, "dest")
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    destination_tables = [
        "output_named_conn_test",
        "second_output_named_conn_test",
    ]
    testing_function = output_named_conn_generator(
        conn_name=NAMED_CONN_NAME,
        destination_table=destination_tables,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_sql_list", "mock_table.parquet"
    )

    fnrun = tdfnrun(
        function=testing_function,
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0

    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_sql_list", "expected_result.json"
    )
    expected_output = read_json_and_clean(expected_output_file)

    output = pl.read_database_uri(
        uri=mysql_config.uri_with_credentials,
        query="SELECT * FROM output_named_conn_test",
    )
    output = clean_polars_df(output)
    assert output.equals(expected_output)

    output2 = pl.read_database_uri(
        uri=mysql_config.uri_with_credentials,
        query="SELECT * FROM second_output_named_conn_test",
    )
    output2 = clean_polars_df(output2)
    assert output2.equals(expected_output)


# --- S3 ---


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_s3_with_named_conn(s3_config, tmp_path, monkeypatch):
    repo_dir = _write_named_conn_repository(
        tmp_path,
        S3_NAMED_CONN_NAME,
        "S3Conn",
        {
            "bucket_uri": s3_config["URI"],
            "region": s3_config["REGION"],
            "credentials": {
                "aws_access_key_id": s3_config["ACCESS_KEY"],
                "aws_secret_access_key": s3_config["SECRET_KEY"],
            },
        },
        "src",
    )
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    testing_function = input_s3_named_conn_generator(
        conn_name=S3_NAMED_CONN_NAME,
        path="testing_nested_import/data.csv",
    )

    fnrun = tdfnrun(function=testing_function)
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = clean_polars_df(pl.read_parquet(output_file))
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_input_new_s3", "expected_result.json"
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_s3_with_named_conn(s3_config, s3_client, tmp_path, monkeypatch):
    repo_dir = _write_named_conn_repository(
        tmp_path,
        S3_NAMED_CONN_NAME,
        "S3Conn",
        {
            "bucket_uri": s3_config["URI"],
            "region": s3_config["REGION"],
            "credentials": {
                "aws_access_key_id": s3_config["ACCESS_KEY"],
                "aws_secret_access_key": s3_config["SECRET_KEY"],
            },
        },
        "dest",
    )
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    bucket_name = s3_config["BUCKET_NAME"]
    file_name = (
        f"testing_output/test_output_s3_named_conn_"
        f"{int(datetime.datetime.now().timestamp())}.parquet"
    )
    testing_function = output_s3_named_conn_generator(
        conn_name=S3_NAMED_CONN_NAME,
        path=file_name,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3", "mock_table.parquet"
    )

    fnrun = tdfnrun(
        function=testing_function,
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0

    try:
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = clean_polars_df(pl.read_parquet(copy_destination))
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER, "test_output_new_s3", "expected_result.json"
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


# --- Azure ---


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_azure_with_named_conn(azure_config, tmp_path, monkeypatch):
    account_name = os.environ.get(azure_config["ACCOUNT_NAME_ENV"])
    account_key = os.environ.get(azure_config["ACCOUNT_KEY_ENV"])
    repo_dir = _write_named_conn_repository(
        tmp_path,
        AZURE_NAMED_CONN_NAME,
        "AzureConn",
        {
            "container_uri": azure_config["URI"],
            "credentials": {
                "account_name": account_name,
                "account_key": account_key,
            },
        },
        "src",
    )
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    testing_function = input_azure_named_conn_generator(
        conn_name=AZURE_NAMED_CONN_NAME,
        path="testing_resources/data.csv",
    )

    fnrun = tdfnrun(function=testing_function)
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = clean_polars_df(pl.read_parquet(output_file))
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_input_new_azure", "expected_result.json"
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_azure_with_named_conn(
    azure_config, azure_client, tmp_path, monkeypatch
):
    account_name = os.environ.get(azure_config["ACCOUNT_NAME_ENV"])
    account_key = os.environ.get(azure_config["ACCOUNT_KEY_ENV"])
    repo_dir = _write_named_conn_repository(
        tmp_path,
        AZURE_NAMED_CONN_NAME,
        "AzureConn",
        {
            "container_uri": azure_config["URI"],
            "credentials": {
                "account_name": account_name,
                "account_key": account_key,
            },
        },
        "dest",
    )
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    container_name = azure_config["CONTAINER_NAME"]
    blob_name = (
        "test_output/test_output_azure_named_conn_"
        f"{int(datetime.datetime.now().timestamp())}.parquet"
    )
    testing_function = output_azure_named_conn_generator(
        conn_name=AZURE_NAMED_CONN_NAME,
        path=blob_name,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_azure", "mock_table.parquet"
    )

    blob_client = None
    fnrun = tdfnrun(
        function=testing_function,
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0

    try:
        blob_client = azure_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        output = clean_polars_df(pl.read_parquet(local_file_path))
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER, "test_output_new_azure", "expected_result.json"
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


# --- GCS ---


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_input_gcs_with_named_conn(gcs_config, tmp_path, monkeypatch):
    service_account_key = os.environ.get(gcs_config["ENV"])
    repo_dir = _write_named_conn_repository(
        tmp_path,
        GCS_NAMED_CONN_NAME,
        "GCSConn",
        {
            "bucket_uri": f"gs://{gcs_config['BUCKET']}",
            "credentials": {
                "service_account_key": service_account_key,
            },
        },
        "src",
    )
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    testing_function = input_gcs_named_conn_generator(
        conn_name=GCS_NAMED_CONN_NAME,
        path="testing_resources/data.csv",
    )

    fnrun = tdfnrun(function=testing_function)
    assert fnrun.status == 0

    output_file = fnrun.output[0].path
    assert os.path.exists(output_file)
    output = clean_polars_df(pl.read_parquet(output_file))
    expected_output_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_input_new_gcs", "expected_result.json"
    )
    expected_output = read_json_and_clean(expected_output_file)
    assert output.equals(expected_output)


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_gcs_with_named_conn(gcs_config, gcs_client, tmp_path, monkeypatch):
    service_account_key = os.environ.get(gcs_config["ENV"])
    repo_dir = _write_named_conn_repository(
        tmp_path,
        GCS_NAMED_CONN_NAME,
        "GCSConn",
        {
            "bucket_uri": f"gs://{gcs_config['BUCKET']}",
            "credentials": {
                "service_account_key": service_account_key,
            },
        },
        "dest",
    )
    monkeypatch.setenv(TD_URI_REPOSITORY, repo_dir)

    bucket_name = gcs_config["BUCKET"]
    blob_name = (
        "test_output/test_output_gcs_named_conn_"
        f"{int(datetime.datetime.now().timestamp())}.parquet"
    )
    testing_function = output_gcs_named_conn_generator(
        conn_name=GCS_NAMED_CONN_NAME,
        path=blob_name,
    )
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_gcs", "mock_table.parquet"
    )

    fnrun = tdfnrun(
        function=testing_function,
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0

    try:
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        output = clean_polars_df(pl.read_parquet(local_file_path))
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER, "test_output_new_gcs", "expected_result.json"
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            bucket = gcs_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception:
            pass

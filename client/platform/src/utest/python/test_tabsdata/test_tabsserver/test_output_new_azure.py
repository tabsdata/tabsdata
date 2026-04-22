#
# Copyright 2026 Tabs Data Inc.
#

import datetime
import logging
import os

import polars as pl
import pytest
from azure.core.exceptions import ResourceNotFoundError

import tabsdata as td
from test_tabsdata.fntest import tdfnrun
from test_tabsdata.conftest import (
    FAKE_EXECUTION_ID,
    FAKE_FUNCTION_RUN_ID,
    FAKE_SCHEDULED_TIME,
    FAKE_TRANSACTION_ID,
    FAKE_TRIGGERED_TIME,
    TESTING_RESOURCES_FOLDER,
    clean_polars_df,
    read_json_and_clean,
)
from test_tabsdata.test_output_new_azure.example import (
    testing_tabsdata_function as output_new_azure,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_azure_parquet(tmp_path, azure_client, azure_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_azure", "mock_table.parquet"
    )
    container_name = azure_config["CONTAINER_NAME"]
    blob_name = (
        "test_output/test_output_new_azure_parquet_"
        f"{int(datetime.datetime.now().timestamp())}.parquet"
    )
    blob_client = None
    fnrun = tdfnrun(
        output_new_azure(
            container_uri=azure_config["URI"],
            path=blob_name,
            credentials=azure_config["CREDENTIALS"],
        ),
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
        output = pl.read_parquet(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_azure",
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


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_azure_csv(tmp_path, azure_client, azure_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_azure", "mock_table.parquet"
    )
    container_name = azure_config["CONTAINER_NAME"]
    blob_name = (
        "test_output/test_output_new_azure_csv_"
        f"{int(datetime.datetime.now().timestamp())}.csv"
    )
    blob_client = None
    fnrun = tdfnrun(
        output_new_azure(
            container_uri=azure_config["URI"],
            path=blob_name,
            credentials=azure_config["CREDENTIALS"],
            format=td.CSVFormat(output_float_precision=4),
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    try:
        blob_client = azure_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        local_file_path = os.path.join(tmp_path, "output.csv")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        output = pl.read_csv(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_azure",
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


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_azure_ndjson(tmp_path, azure_client, azure_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_azure", "mock_table.parquet"
    )
    container_name = azure_config["CONTAINER_NAME"]
    blob_name = (
        "test_output/test_output_new_azure_ndjson_"
        f"{int(datetime.datetime.now().timestamp())}.ndjson"
    )
    blob_client = None
    fnrun = tdfnrun(
        output_new_azure(
            container_uri=azure_config["URI"],
            path=blob_name,
            credentials=azure_config["CREDENTIALS"],
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    try:
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
            "test_output_new_azure",
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


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_azure_avro(tmp_path, azure_client, azure_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_azure", "mock_table.parquet"
    )
    container_name = azure_config["CONTAINER_NAME"]
    blob_name = (
        "test_output/test_output_new_azure_avro_"
        f"{int(datetime.datetime.now().timestamp())}.avro"
    )
    blob_client = None
    fnrun = tdfnrun(
        output_new_azure(
            container_uri=azure_config["URI"],
            path=blob_name,
            credentials=azure_config["CREDENTIALS"],
            format=td.AvroFormat(),
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    try:
        blob_client = azure_client.get_blob_client(
            container=container_name, blob=blob_name
        )
        local_file_path = os.path.join(tmp_path, "output.avro")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        output = pl.read_avro(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_azure",
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


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_azure_parquet_with_transaction_id(
    tmp_path, azure_client, azure_config
):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_azure", "mock_table.parquet"
    )
    container_name = azure_config["CONTAINER_NAME"]
    blob_name = (
        "test_output/test_output_new_azure_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$TRANSACTION_ID.parquet"
    )
    output_fn = output_new_azure(
        container_uri=azure_config["URI"],
        path=blob_name,
        credentials=azure_config["CREDENTIALS"],
    )
    blob_name = blob_name.replace("$TRANSACTION_ID", str(FAKE_TRANSACTION_ID))
    blob_client = None
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
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
            "test_output_new_azure",
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


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_azure_parquet_with_execution_id(
    tmp_path, azure_client, azure_config
):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_azure", "mock_table.parquet"
    )
    container_name = azure_config["CONTAINER_NAME"]
    blob_name = (
        "test_output/test_output_new_azure_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$EXECUTION_ID.parquet"
    )
    output_fn = output_new_azure(
        container_uri=azure_config["URI"],
        path=blob_name,
        credentials=azure_config["CREDENTIALS"],
    )
    blob_name = blob_name.replace("$EXECUTION_ID", str(FAKE_EXECUTION_ID))
    blob_client = None
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
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
            "test_output_new_azure",
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


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_azure_parquet_with_function_run_id(
    tmp_path, azure_client, azure_config
):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_azure", "mock_table.parquet"
    )
    container_name = azure_config["CONTAINER_NAME"]
    blob_name = (
        "test_output/test_output_new_azure_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$FUNCTION_RUN_ID.parquet"
    )
    output_fn = output_new_azure(
        container_uri=azure_config["URI"],
        path=blob_name,
        credentials=azure_config["CREDENTIALS"],
    )
    blob_name = blob_name.replace("$FUNCTION_RUN_ID", str(FAKE_FUNCTION_RUN_ID))
    blob_client = None
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
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
            "test_output_new_azure",
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


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_azure_parquet_with_trigger_timestamp(
    tmp_path, azure_client, azure_config
):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_azure", "mock_table.parquet"
    )
    container_name = azure_config["CONTAINER_NAME"]
    blob_name = (
        "test_output/test_output_new_azure_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$TRIGGER_TIMESTAMP.parquet"
    )
    output_fn = output_new_azure(
        container_uri=azure_config["URI"],
        path=blob_name,
        credentials=azure_config["CREDENTIALS"],
    )
    blob_name = blob_name.replace("$TRIGGER_TIMESTAMP", str(FAKE_TRIGGERED_TIME))
    blob_client = None
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
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
            "test_output_new_azure",
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


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_azure_parquet_with_scheduler_timestamp(
    tmp_path, azure_client, azure_config
):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_azure", "mock_table.parquet"
    )
    container_name = azure_config["CONTAINER_NAME"]
    blob_name = (
        "test_output/test_output_new_azure_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$SCHEDULER_TIMESTAMP.parquet"
    )
    output_fn = output_new_azure(
        container_uri=azure_config["URI"],
        path=blob_name,
        credentials=azure_config["CREDENTIALS"],
    )
    blob_name = blob_name.replace("$SCHEDULER_TIMESTAMP", str(FAKE_SCHEDULED_TIME))
    blob_client = None
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
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
            "test_output_new_azure",
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


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_azure_parquet_with_export_timestamp(
    tmp_path, azure_client, azure_config
):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_azure", "mock_table.parquet"
    )
    container_name = azure_config["CONTAINER_NAME"]
    current_test_indicator = int(datetime.datetime.now().timestamp())
    blob_name = (
        "test_output/test_output_new_azure_parquet_"
        f"{current_test_indicator}_$EXPORT_TIMESTAMP.parquet"
    )
    output_fn = output_new_azure(
        container_uri=azure_config["URI"],
        path=blob_name,
        credentials=azure_config["CREDENTIALS"],
    )
    # $EXPORT_TIMESTAMP is replaced at runtime with round(time.time() * 1000),
    # so we cannot predict the exact value. Instead, search for the blob by prefix.
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    resolved_blob_name = None
    try:
        prefix = (
            "test_output/test_output_new_azure_parquet_"
            f"{current_test_indicator}_"
        )
        container_client = azure_client.get_container_client(container_name)
        blobs = list(container_client.list_blobs(name_starts_with=prefix))
        for b in blobs:
            if b.name.endswith(".parquet"):
                resolved_blob_name = b.name
                break
        assert resolved_blob_name is not None, (
            f"No Azure blob found with prefix '{prefix}'"
        )
        assert "$EXPORT_TIMESTAMP" not in resolved_blob_name
        blob_client = azure_client.get_blob_client(
            container=container_name, blob=resolved_blob_name
        )
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())
        output = pl.read_parquet(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_azure",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        if resolved_blob_name is not None:
            try:
                blob_client = azure_client.get_blob_client(
                    container=container_name, blob=resolved_blob_name
                )
                blob_client.delete_blob()
            except ResourceNotFoundError as e:
                logger.warning(
                    f"The blob to delete wasn't created or was already deleted: {e}"
                )

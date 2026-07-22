#
# Copyright 2025 Tabsdata Inc.
#

import copy
import datetime
import logging
import os

import polars as pl
import pytest

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
from test_tabsdata.test_output_s3.example import output_s3 as output_s3_format_testing
from test_tabsdata.test_output_s3_frame_list.example import (
    output_s3_frame_list,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.s3
@pytest.mark.s3_specific
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_s3_parquet(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    output_s3_parquet = copy.deepcopy(output_s3_format_testing)
    output_file = (
        f"{s3_config['URI']}/testing_output/test_output_s3_parquet_"
        f"{int(datetime.datetime.now().timestamp())}.parquet"
    )
    output_s3_parquet.output.uri = output_file
    output_s3_parquet.output.credentials = s3_config["CREDENTIALS"]
    output_s3_parquet.output.region = s3_config["REGION"]
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    fnrun = tdfnrun(
        output_s3_parquet,
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_s3_parquet_with_transaction_id(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    output_s3_fn = copy.deepcopy(output_s3_format_testing)
    output_file = (
        f"{s3_config['URI']}/testing_output/test_output_s3_parquet_with_data_"
        f"version_{int(datetime.datetime.now().timestamp())}_$TRANSACTION_ID.parquet"
    )
    output_s3_fn.output.uri = output_file
    output_s3_fn.output.credentials = s3_config["CREDENTIALS"]
    output_s3_fn.output.region = s3_config["REGION"]
    output_file = output_file.replace("$TRANSACTION_ID", str(FAKE_TRANSACTION_ID))
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    fnrun = tdfnrun(output_s3_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_s3_parquet_with_function_run_id(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    output_s3_fn = copy.deepcopy(output_s3_format_testing)
    output_file = (
        f"{s3_config['URI']}/testing_output/test_output_s3_parquet_with_data_"
        f"version_{int(datetime.datetime.now().timestamp())}_$FUNCTION_RUN_ID.parquet"
    )
    output_s3_fn.output.uri = output_file
    output_s3_fn.output.credentials = s3_config["CREDENTIALS"]
    output_s3_fn.output.region = s3_config["REGION"]
    output_file = output_file.replace("$FUNCTION_RUN_ID", str(FAKE_FUNCTION_RUN_ID))
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    fnrun = tdfnrun(output_s3_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_s3_parquet_with_execution_id(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    output_s3_fn = copy.deepcopy(output_s3_format_testing)
    output_file = (
        f"{s3_config['URI']}/testing_output/test_output_s3_parquet_with_data_"
        f"version_{int(datetime.datetime.now().timestamp())}_$EXECUTION_ID.parquet"
    )
    output_s3_fn.output.uri = output_file
    output_s3_fn.output.credentials = s3_config["CREDENTIALS"]
    output_s3_fn.output.region = s3_config["REGION"]
    output_file = output_file.replace("$EXECUTION_ID", str(FAKE_EXECUTION_ID))
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    fnrun = tdfnrun(output_s3_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_s3_parquet_with_export_timestamp(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    output_s3_fn = copy.deepcopy(output_s3_format_testing)
    current_test_indicator = int(datetime.datetime.now().timestamp())
    output_file = (
        f"{s3_config['URI']}/testing_output"
        "/test_output_s3_parquet_with_export_timestamp_"
        f"{current_test_indicator}_$EXPORT_TIMESTAMP.parquet"
    )
    output_s3_fn.output.uri = output_file
    output_s3_fn.output.credentials = s3_config["CREDENTIALS"]
    output_s3_fn.output.region = s3_config["REGION"]
    bucket_name = output_file.split("/")[2]
    fnrun = tdfnrun(output_s3_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    # $EXPORT_TIMESTAMP is replaced at runtime with round(time.time() * 1000),
    # so we cannot predict the exact value. Search for the file by prefix.
    file_name = None
    try:
        prefix = (
            "testing_output/test_output_s3_parquet_with_export_timestamp_"
            f"{current_test_indicator}_"
        )
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get("Contents", []):
                if obj["Key"].startswith(prefix):
                    file_name = obj["Key"]
                    break
        assert file_name is not None
        assert "$EXPORT_TIMESTAMP" not in file_name
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        if file_name is not None:
            s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_s3_parquet_with_trigger_timestamp(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    output_s3_fn = copy.deepcopy(output_s3_format_testing)
    current_test_indicator = int(datetime.datetime.now().timestamp())
    output_file = (
        f"{s3_config['URI']}/testing_output"
        "/test_output_s3_parquet_with_trigger_timestamp_"
        f"{current_test_indicator}_$TRIGGER_TIMESTAMP.parquet"
    )
    output_s3_fn.output.uri = output_file
    output_s3_fn.output.credentials = s3_config["CREDENTIALS"]
    output_s3_fn.output.region = s3_config["REGION"]
    output_file = output_file.replace("$TRIGGER_TIMESTAMP", str(FAKE_TRIGGERED_TIME))
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    fnrun = tdfnrun(output_s3_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_s3_parquet_with_scheduler_timestamp(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    output_s3_fn = copy.deepcopy(output_s3_format_testing)
    current_test_indicator = int(datetime.datetime.now().timestamp())
    output_file = (
        f"{s3_config['URI']}/testing_output"
        "/test_output_s3_parquet_with_scheduler_timestamp_"
        f"{current_test_indicator}_$SCHEDULER_TIMESTAMP.parquet"
    )
    output_s3_fn.output.uri = output_file
    output_s3_fn.output.credentials = s3_config["CREDENTIALS"]
    output_s3_fn.output.region = s3_config["REGION"]
    output_file = output_file.replace("$SCHEDULER_TIMESTAMP", str(FAKE_SCHEDULED_TIME))
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    fnrun = tdfnrun(output_s3_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_s3_csv(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    output_s3_csv = copy.deepcopy(output_s3_format_testing)
    output_file = (
        f"{s3_config['URI']}/testing_output/test_output_s3_csv_"
        f"{int(datetime.datetime.now().timestamp())}.csv"
    )
    output_s3_csv.output.uri = output_file
    output_s3_csv.output.format = td.CSVFormat(
        # ToDo: Undo when https://github.com/pola-rs/polars/issues/21802 fix is available
        eol_char="\n",
        separator=",",
        output_float_precision=4,
    )
    output_s3_csv.output.credentials = s3_config["CREDENTIALS"]
    output_s3_csv.output.region = s3_config["REGION"]
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    fnrun = tdfnrun(output_s3_csv, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.csv")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        # ToDo: Undo when https://github.com/pola-rs/polars/issues/21802 fix is available
        output = pl.read_csv(copy_destination, separator=",", eol_char="\n")
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_s3_ndjson(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3", "mock_table.parquet"
    )
    output_s3_ndjson = copy.deepcopy(output_s3_format_testing)
    output_file = (
        f"{s3_config['URI']}/testing_output/test_output_s3_ndjson_"
        f"{int(datetime.datetime.now().timestamp())}.ndjson"
    )
    output_s3_ndjson.output.uri = output_file
    output_s3_ndjson.output.credentials = s3_config["CREDENTIALS"]
    output_s3_ndjson.output.region = s3_config["REGION"]
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    fnrun = tdfnrun(output_s3_ndjson, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.ndjson")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_ndjson(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_s3_frame_list(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_s3_frame_list", "mock_table.parquet"
    )
    output_file = (
        f"{s3_config['URI']}/testing_output/test_output_s3_frame_list_"
        f"{int(datetime.datetime.now().timestamp())}_$FRAGMENT_IDX.parquet"
    )
    output_s3_frame_list.output.uri = output_file
    output_s3_frame_list.output.credentials = s3_config["CREDENTIALS"]
    output_s3_frame_list.output.region = s3_config["REGION"]
    bucket_name = output_file.split("/")[2]
    file_name = "/".join(output_file.split("/")[3:])
    file_name_0 = file_name.replace("$FRAGMENT_IDX", "0")
    file_name_1 = file_name.replace("$FRAGMENT_IDX", "1")
    fnrun = tdfnrun(output_s3_frame_list, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output_0.parquet")
        s3_client.download_file(bucket_name, file_name_0, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3_frame_list",
            "expected_result_0.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)

        copy_destination = os.path.join(tmp_path, "output_1.parquet")
        s3_client.download_file(bucket_name, file_name_1, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_s3_frame_list",
            "expected_result_1.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name_0)
        s3_client.delete_object(Bucket=bucket_name, Key=file_name_1)

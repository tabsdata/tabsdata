#
# Copyright 2026 Tabsdata Inc.
#

import datetime
import logging
import os

import polars as pl
import pytest

import tabsdata as td
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
from test_tabsdata.fntest import tdfnrun

from test_tabsdata.test_output_new_s3.example import (
    testing_tabsdata_function as output_new_s3,
)
from test_tabsdata.test_output_new_s3_frame_list.example import (
    testing_tabsdata_function as output_new_s3_frame_list,
)


# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_s3_parquet(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3", "mock_table.parquet"
    )
    bucket_name = s3_config["BUCKET_NAME"]
    file_name = (
        f"testing_output/test_output_new_s3_parquet_"
        f"{int(datetime.datetime.now().timestamp())}.parquet"
    )
    fnrun = tdfnrun(
        output_new_s3(
            bucket_uri=s3_config["URI"],
            path=file_name,
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
        ),
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
            "test_output_new_s3",
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
def test_output_new_s3_parquet_with_transaction_id(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3", "mock_table.parquet"
    )
    bucket_name = s3_config["BUCKET_NAME"]
    file_name = (
        f"testing_output/test_output_new_s3_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$TRANSACTION_ID.parquet"
    )
    output_fn = output_new_s3(
        bucket_uri=s3_config["URI"],
        path=file_name,
        credentials=s3_config["CREDENTIALS"],
        region=s3_config["REGION"],
    )
    file_name = file_name.replace("$TRANSACTION_ID", str(FAKE_TRANSACTION_ID))
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3",
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
def test_output_new_s3_parquet_with_execution_id(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3", "mock_table.parquet"
    )
    bucket_name = s3_config["BUCKET_NAME"]
    file_name = (
        f"testing_output/test_output_new_s3_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$EXECUTION_ID.parquet"
    )
    output_fn = output_new_s3(
        bucket_uri=s3_config["URI"],
        path=file_name,
        credentials=s3_config["CREDENTIALS"],
        region=s3_config["REGION"],
    )
    file_name = file_name.replace("$EXECUTION_ID", str(FAKE_EXECUTION_ID))
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3",
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
def test_output_new_s3_parquet_with_function_run_id(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3", "mock_table.parquet"
    )
    bucket_name = s3_config["BUCKET_NAME"]
    file_name = (
        f"testing_output/test_output_new_s3_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$FUNCTION_RUN_ID.parquet"
    )
    output_fn = output_new_s3(
        bucket_uri=s3_config["URI"],
        path=file_name,
        credentials=s3_config["CREDENTIALS"],
        region=s3_config["REGION"],
    )
    file_name = file_name.replace("$FUNCTION_RUN_ID", str(FAKE_FUNCTION_RUN_ID))
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3",
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
def test_output_new_s3_parquet_with_trigger_timestamp(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3", "mock_table.parquet"
    )
    bucket_name = s3_config["BUCKET_NAME"]
    file_name = (
        f"testing_output/test_output_new_s3_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$TRIGGER_TIMESTAMP.parquet"
    )
    output_fn = output_new_s3(
        bucket_uri=s3_config["URI"],
        path=file_name,
        credentials=s3_config["CREDENTIALS"],
        region=s3_config["REGION"],
    )
    file_name = file_name.replace("$TRIGGER_TIMESTAMP", str(FAKE_TRIGGERED_TIME))
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3",
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
def test_output_new_s3_parquet_with_scheduler_timestamp(
    tmp_path, s3_client, s3_config
):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3", "mock_table.parquet"
    )
    bucket_name = s3_config["BUCKET_NAME"]
    file_name = (
        f"testing_output/test_output_new_s3_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$SCHEDULER_TIMESTAMP.parquet"
    )
    output_fn = output_new_s3(
        bucket_uri=s3_config["URI"],
        path=file_name,
        credentials=s3_config["CREDENTIALS"],
        region=s3_config["REGION"],
    )
    file_name = file_name.replace("$SCHEDULER_TIMESTAMP", str(FAKE_SCHEDULED_TIME))
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3",
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
def test_output_new_s3_parquet_with_export_timestamp(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3", "mock_table.parquet"
    )
    bucket_name = s3_config["BUCKET_NAME"]
    current_test_indicator = int(datetime.datetime.now().timestamp())
    # The stem must not be shared with the trigger/scheduler-timestamp variants:
    # this test locates (and finally deletes) its object BY PREFIX, and the
    # second-resolution indicator collides when the tests start within the same
    # second on different workers — the sibling's object would be matched first
    # (the fake trigger/scheduler values sort before a real export timestamp)
    # and deleted, making the sibling's exact-name download fail.
    file_name = (
        f"testing_output/test_output_new_s3_parquet_export_"
        f"{current_test_indicator}_$EXPORT_TIMESTAMP.parquet"
    )
    output_fn = output_new_s3(
        bucket_uri=s3_config["URI"],
        path=file_name,
        credentials=s3_config["CREDENTIALS"],
        region=s3_config["REGION"],
    )
    # $EXPORT_TIMESTAMP is replaced at runtime with round(time.time() * 1000),
    # so we cannot predict the exact value. Instead, search for the file by prefix.
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    resolved_file_name = None
    try:
        prefix = (
            f"testing_output/test_output_new_s3_parquet_export_"
            f"{current_test_indicator}_"
        )
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get("Contents", []):
                if obj["Key"].startswith(prefix) and obj["Key"].endswith(".parquet"):
                    resolved_file_name = obj["Key"]
                    break
        assert resolved_file_name is not None, (
            f"No S3 object found with prefix '{prefix}'"
        )
        assert "$EXPORT_TIMESTAMP" not in resolved_file_name
        copy_destination = os.path.join(tmp_path, "output.parquet")
        s3_client.download_file(bucket_name, resolved_file_name, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        if resolved_file_name is not None:
            s3_client.delete_object(Bucket=bucket_name, Key=resolved_file_name)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_s3_csv(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3", "mock_table.parquet"
    )
    bucket_name = s3_config["BUCKET_NAME"]
    file_name = (
        f"testing_output/test_output_new_s3_csv_"
        f"{int(datetime.datetime.now().timestamp())}.csv"
    )
    fnrun = tdfnrun(
        output_new_s3(
            bucket_uri=s3_config["URI"],
            path=file_name,
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            format=td.CSVFormat(
                eol_char="\n",
                separator=",",
                output_float_precision=4,
            ),
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.csv")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_csv(copy_destination, separator=",", eol_char="\n")
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3",
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
def test_output_new_s3_ndjson(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3", "mock_table.parquet"
    )
    bucket_name = s3_config["BUCKET_NAME"]
    file_name = (
        f"testing_output/test_output_new_s3_ndjson_"
        f"{int(datetime.datetime.now().timestamp())}.ndjson"
    )
    fnrun = tdfnrun(
        output_new_s3(
            bucket_uri=s3_config["URI"],
            path=file_name,
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.ndjson")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_ndjson(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3",
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
def test_output_new_s3_frame_list(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3_frame_list", "mock_table.parquet"
    )
    bucket_name = s3_config["BUCKET_NAME"]
    file_name = (
        f"testing_output/test_output_new_s3_frame_list_"
        f"{int(datetime.datetime.now().timestamp())}_$FRAGMENT_IDX.parquet"
    )
    file_name_0 = file_name.replace("$FRAGMENT_IDX", "0")
    file_name_1 = file_name.replace("$FRAGMENT_IDX", "1")
    fnrun = tdfnrun(
        output_new_s3_frame_list(
            bucket_uri=s3_config["URI"],
            path=file_name,
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output_0.parquet")
        s3_client.download_file(bucket_name, file_name_0, copy_destination)
        output = pl.read_parquet(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3_frame_list",
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
            "test_output_new_s3_frame_list",
            "expected_result_1.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name_0)
        s3_client.delete_object(Bucket=bucket_name, Key=file_name_1)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_s3_avro(tmp_path, s3_client, s3_config):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_s3", "mock_table.parquet"
    )
    bucket_name = s3_config["BUCKET_NAME"]
    file_name = (
        f"testing_output/test_output_new_s3_avro_"
        f"{int(datetime.datetime.now().timestamp())}.avro"
    )
    fnrun = tdfnrun(
        output_new_s3(
            bucket_uri=s3_config["URI"],
            path=file_name,
            credentials=s3_config["CREDENTIALS"],
            region=s3_config["REGION"],
            format=td.AvroFormat(),
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    try:
        copy_destination = os.path.join(tmp_path, "output.avro")
        s3_client.download_file(bucket_name, file_name, copy_destination)
        output = pl.read_avro(copy_destination)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_s3",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        s3_client.delete_object(Bucket=bucket_name, Key=file_name)

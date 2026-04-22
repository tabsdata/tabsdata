#
# Copyright 2026 Tabs Data Inc.
#

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
from test_tabsdata.test_output_new_gcs.example import (
    testing_tabsdata_function as output_new_gcs,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_gcs_parquet(tmp_path, gcs_config, gcs_client):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_gcs", "mock_table.parquet"
    )
    container_name = gcs_config["BUCKET"]
    blob_name = (
        "test_output/test_output_new_gcs_parquet_"
        f"{int(datetime.datetime.now().timestamp())}.parquet"
    )
    fnrun = tdfnrun(
        output_new_gcs(
            bucket_uri=f"gs://{container_name}",
            path=blob_name,
            credentials=td.GCPServiceAccountKeyCredentials(
                td.EnvironmentSecret(gcs_config["ENV"])
            ),
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    try:
        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        output = pl.read_parquet(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_gcs",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            bucket = gcs_client.bucket(container_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception:
            pass


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_gcs_csv(tmp_path, gcs_config, gcs_client):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_gcs", "mock_table.parquet"
    )
    container_name = gcs_config["BUCKET"]
    blob_name = (
        "test_output/test_output_new_gcs_csv_"
        f"{int(datetime.datetime.now().timestamp())}.csv"
    )
    fnrun = tdfnrun(
        output_new_gcs(
            bucket_uri=f"gs://{container_name}",
            path=blob_name,
            credentials=td.GCPServiceAccountKeyCredentials(
                td.EnvironmentSecret(gcs_config["ENV"])
            ),
            format=td.CSVFormat(output_float_precision=4),
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    try:
        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.csv")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        output = pl.read_csv(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_gcs",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            bucket = gcs_client.bucket(container_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception:
            pass


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_gcs_ndjson(tmp_path, gcs_config, gcs_client):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_gcs", "mock_table.parquet"
    )
    container_name = gcs_config["BUCKET"]
    blob_name = (
        "test_output/test_output_new_gcs_ndjson_"
        f"{int(datetime.datetime.now().timestamp())}.ndjson"
    )
    fnrun = tdfnrun(
        output_new_gcs(
            bucket_uri=f"gs://{container_name}",
            path=blob_name,
            credentials=td.GCPServiceAccountKeyCredentials(
                td.EnvironmentSecret(gcs_config["ENV"])
            ),
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    try:
        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.ndjson")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        output = pl.read_ndjson(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_gcs",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            bucket = gcs_client.bucket(container_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception:
            pass


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_gcs_avro(tmp_path, gcs_config, gcs_client):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_gcs", "mock_table.parquet"
    )
    container_name = gcs_config["BUCKET"]
    blob_name = (
        "test_output/test_output_new_gcs_avro_"
        f"{int(datetime.datetime.now().timestamp())}.avro"
    )
    fnrun = tdfnrun(
        output_new_gcs(
            bucket_uri=f"gs://{container_name}",
            path=blob_name,
            credentials=td.GCPServiceAccountKeyCredentials(
                td.EnvironmentSecret(gcs_config["ENV"])
            ),
            format=td.AvroFormat(),
        ),
        input_tables=mock_parquet_table,
    )
    assert fnrun.status == 0
    try:
        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.avro")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        output = pl.read_avro(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_gcs",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            bucket = gcs_client.bucket(container_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception:
            pass


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_gcs_parquet_with_transaction_id(tmp_path, gcs_config, gcs_client):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_gcs", "mock_table.parquet"
    )
    container_name = gcs_config["BUCKET"]
    blob_name = (
        "test_output/test_output_new_gcs_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$TRANSACTION_ID.parquet"
    )
    output_fn = output_new_gcs(
        bucket_uri=f"gs://{container_name}",
        path=blob_name,
        credentials=td.GCPServiceAccountKeyCredentials(
            td.EnvironmentSecret(gcs_config["ENV"])
        ),
    )
    blob_name = blob_name.replace("$TRANSACTION_ID", str(FAKE_TRANSACTION_ID))
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        output = pl.read_parquet(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_gcs",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            bucket = gcs_client.bucket(container_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception:
            pass


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_gcs_parquet_with_execution_id(tmp_path, gcs_config, gcs_client):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_gcs", "mock_table.parquet"
    )
    container_name = gcs_config["BUCKET"]
    blob_name = (
        "test_output/test_output_new_gcs_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$EXECUTION_ID.parquet"
    )
    output_fn = output_new_gcs(
        bucket_uri=f"gs://{container_name}",
        path=blob_name,
        credentials=td.GCPServiceAccountKeyCredentials(
            td.EnvironmentSecret(gcs_config["ENV"])
        ),
    )
    blob_name = blob_name.replace("$EXECUTION_ID", str(FAKE_EXECUTION_ID))
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        output = pl.read_parquet(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_gcs",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            bucket = gcs_client.bucket(container_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception:
            pass


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_gcs_parquet_with_function_run_id(tmp_path, gcs_config, gcs_client):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_gcs", "mock_table.parquet"
    )
    container_name = gcs_config["BUCKET"]
    blob_name = (
        "test_output/test_output_new_gcs_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$FUNCTION_RUN_ID.parquet"
    )
    output_fn = output_new_gcs(
        bucket_uri=f"gs://{container_name}",
        path=blob_name,
        credentials=td.GCPServiceAccountKeyCredentials(
            td.EnvironmentSecret(gcs_config["ENV"])
        ),
    )
    blob_name = blob_name.replace("$FUNCTION_RUN_ID", str(FAKE_FUNCTION_RUN_ID))
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        output = pl.read_parquet(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_gcs",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            bucket = gcs_client.bucket(container_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception:
            pass


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_gcs_parquet_with_trigger_timestamp(
    tmp_path, gcs_config, gcs_client
):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_gcs", "mock_table.parquet"
    )
    container_name = gcs_config["BUCKET"]
    blob_name = (
        "test_output/test_output_new_gcs_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$TRIGGER_TIMESTAMP.parquet"
    )
    output_fn = output_new_gcs(
        bucket_uri=f"gs://{container_name}",
        path=blob_name,
        credentials=td.GCPServiceAccountKeyCredentials(
            td.EnvironmentSecret(gcs_config["ENV"])
        ),
    )
    blob_name = blob_name.replace("$TRIGGER_TIMESTAMP", str(FAKE_TRIGGERED_TIME))
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        output = pl.read_parquet(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_gcs",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            bucket = gcs_client.bucket(container_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception:
            pass


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_gcs_parquet_with_scheduler_timestamp(
    tmp_path, gcs_config, gcs_client
):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_gcs", "mock_table.parquet"
    )
    container_name = gcs_config["BUCKET"]
    blob_name = (
        "test_output/test_output_new_gcs_parquet_"
        f"{int(datetime.datetime.now().timestamp())}_$SCHEDULER_TIMESTAMP.parquet"
    )
    output_fn = output_new_gcs(
        bucket_uri=f"gs://{container_name}",
        path=blob_name,
        credentials=td.GCPServiceAccountKeyCredentials(
            td.EnvironmentSecret(gcs_config["ENV"])
        ),
    )
    blob_name = blob_name.replace("$SCHEDULER_TIMESTAMP", str(FAKE_SCHEDULED_TIME))
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    try:
        bucket = gcs_client.bucket(container_name)
        blob = bucket.blob(blob_name)
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_as_bytes())
        output = pl.read_parquet(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_gcs",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            bucket = gcs_client.bucket(container_name)
            blob = bucket.blob(blob_name)
            blob.delete()
        except Exception:
            pass


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
def test_output_new_gcs_parquet_with_export_timestamp(
    tmp_path, gcs_config, gcs_client
):
    mock_parquet_table = os.path.join(
        TESTING_RESOURCES_FOLDER, "test_output_new_gcs", "mock_table.parquet"
    )
    container_name = gcs_config["BUCKET"]
    current_test_indicator = int(datetime.datetime.now().timestamp())
    blob_name = (
        "test_output/test_output_new_gcs_parquet_"
        f"{current_test_indicator}_$EXPORT_TIMESTAMP.parquet"
    )
    output_fn = output_new_gcs(
        bucket_uri=f"gs://{container_name}",
        path=blob_name,
        credentials=td.GCPServiceAccountKeyCredentials(
            td.EnvironmentSecret(gcs_config["ENV"])
        ),
    )
    # $EXPORT_TIMESTAMP is replaced at runtime with round(time.time() * 1000),
    # so we cannot predict the exact value. Instead, search for the blob by prefix.
    fnrun = tdfnrun(output_fn, input_tables=mock_parquet_table)
    assert fnrun.status == 0
    resolved_blob_name = None
    try:
        prefix = (
            "test_output/test_output_new_gcs_parquet_"
            f"{current_test_indicator}_"
        )
        bucket = gcs_client.bucket(container_name)
        blobs = list(bucket.list_blobs(prefix=prefix))
        for b in blobs:
            if b.name.endswith(".parquet"):
                resolved_blob_name = b.name
                break
        assert resolved_blob_name is not None, (
            f"No GCS blob found with prefix '{prefix}'"
        )
        assert "$EXPORT_TIMESTAMP" not in resolved_blob_name
        local_file_path = os.path.join(tmp_path, "output.parquet")
        with open(local_file_path, "wb") as download_file:
            download_file.write(bucket.blob(resolved_blob_name).download_as_bytes())
        output = pl.read_parquet(local_file_path)
        output = clean_polars_df(output)
        expected_output_file = os.path.join(
            TESTING_RESOURCES_FOLDER,
            "test_output_new_gcs",
            "expected_result.json",
        )
        expected_output = read_json_and_clean(expected_output_file)
        assert output.equals(expected_output)
    finally:
        try:
            if resolved_blob_name is not None:
                bucket = gcs_client.bucket(container_name)
                bucket.blob(resolved_blob_name).delete()
        except Exception:
            pass

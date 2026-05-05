#
# Copyright 2026 Tabs Data Inc.
#

import logging
import os
import uuid
from urllib.parse import urlparse
from urllib.request import pathname2url

import polars as pl
import pytest
from mimesis import Field
from mimesis.locales import Locale

import tabsdata as td
from test_tabsdata.fntest import tdfnrun

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

TD_ORIGIN_FILE_PATH = "@td.origin.file.path"
TD_ORIGIN_FILE_LINE = "@td.origin.file.line"

DEFAULT_Q_RECORDS = 2048


def generate_tab_data(q_records: int = DEFAULT_Q_RECORDS) -> pl.DataFrame:
    field = Field(locale=Locale.EN, seed=42)
    return pl.DataFrame({
        "id": list(range(q_records)),
        "name": [
            field("person.full_name")
            for _ in range(q_records)
        ],
        "email": [
            field("person.email")
            for _ in range(q_records)
        ],
        "city": [
            field("address.city")
            for _ in range(q_records)
        ],
        "salary": [
            field("finance.price", minimum=1.0, maximum=99999.99)
            for _ in range(q_records)
        ],
    })


def generate_log_data(q_records: int = DEFAULT_Q_RECORDS) -> list[str]:
    field = Field(locale=Locale.EN, seed=42)
    levels = ["INFO", "WARN", "ERROR", "DEBUG", "TRACE"]
    lines = []
    for i in range(q_records):
        date = field("datetime.date", start=2020, end=2025)
        level = levels[i % len(levels)]
        message = field("text.sentence")
        lines.append(f"{date} {level} {message}")
    return lines


def write_test_data(temp_folder: str, file_format: str) -> str:
    match file_format:
        case "csv":
            df = generate_tab_data()
            path = os.path.join(temp_folder, "data.csv")
            df.write_csv(path)
        case "parquet":
            df = generate_tab_data()
            path = os.path.join(temp_folder, "data.parquet")
            df.write_parquet(path)
        case "ndjson":
            df = generate_tab_data()
            path = os.path.join(temp_folder, "data.ndjson")
            df.write_ndjson(path)
        case "log":
            lines = generate_log_data()
            path = os.path.join(temp_folder, "data.log")
            with open(path, "w") as f:
                for line in lines:
                    f.write(line + "\n")
        case _:
            raise ValueError(f"Unsupported format: {file_format}")
    return path


def upload_to_azure(local_path: str, azure_config, azure_client) -> str:
    uid = uuid.uuid4().hex[:12]
    blob_name = f"test_file_info/{uid}/{os.path.basename(local_path)}"
    container_name = azure_config["CONTAINER_NAME"]
    container_client = azure_client.get_container_client(container_name)
    with open(local_path, "rb") as f:
        container_client.upload_blob(name=blob_name, data=f, overwrite=True)
    return f"az://{container_name}/{blob_name}"


def upload_to_gcs(local_path: str, gcs_config, gcs_client) -> str:
    uid = uuid.uuid4().hex[:12]
    blob_name = f"test_file_info/{uid}/{os.path.basename(local_path)}"
    bucket_name = gcs_config["BUCKET"]
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    return f"gs://{bucket_name}/{blob_name}"


def upload_to_s3(local_path: str, s3_config, s3_client) -> str:
    uid = uuid.uuid4().hex[:12]
    key = f"test_file_info/{uid}/{os.path.basename(local_path)}"
    bucket = s3_config["BUCKET_NAME"]
    s3_client.upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"


def make_source(
        publisher_type: str,
        uri: str,
        file_format: str,
        file_info: str | None,
        s3_config=None,
        azure_config=None,
        gcs_config=None):
    match publisher_type:
        case "local":
            return td.LocalFileSource(
                path=uri,
                format=file_format,
                file_info=file_info)
        case "azure":
            return td.AzureSource(
                uri=uri,
                credentials=azure_config["CREDENTIALS"],
                format=file_format,
                file_info=file_info,
            )
        case "gcs":
            return td.GCSSource(
                uri=uri,
                credentials=td.GCPServiceAccountKeyCredentials(
                    td.EnvironmentSecret(gcs_config["ENV"])
                ),
                format=file_format,
                file_info=file_info,
            )
        case "s3":
            return td.S3Source(
                uri=uri,
                credentials=s3_config["CREDENTIALS"],
                region=s3_config["REGION"],
                format=file_format,
                file_info=file_info,
            )
        case _:
            raise ValueError(f"Unsupported source type: {publisher_type}")


def verify_file_info_columns(
    result_df: pl.DataFrame,
    file_info: str | None,
    file_format: str,
    expected_uri: str,
):
    if file_info is None:
        assert TD_ORIGIN_FILE_PATH not in result_df.columns
        assert TD_ORIGIN_FILE_LINE not in result_df.columns
        return

    assert TD_ORIGIN_FILE_PATH in result_df.columns
    assert TD_ORIGIN_FILE_LINE in result_df.columns

    line_values = result_df[TD_ORIGIN_FILE_LINE].to_list()
    assert line_values == list(range(1, len(line_values) + 1))

    path_values = result_df[TD_ORIGIN_FILE_PATH].unique().to_list()
    assert len(path_values) == 1, (
        f"Expected single unique path value, got: {path_values}"
    )
    path_value = path_values[0]

    expected_filename = urlparse(expected_uri).path.rsplit("/", 1)[-1]
    match file_info:
        case "name":
            assert path_value == expected_filename, (
                f"Expected filename '{expected_filename}', got: '{path_value}'"
            )
        case "uri":
            assert path_value == expected_uri, (
                f"Expected URI '{expected_uri}', got: '{path_value}'"
            )

    if file_format == "log":
        assert "file" in result_df.columns
        assert "message" in result_df.columns


@pytest.mark.localfile
@pytest.mark.tdfnrun
@pytest.mark.parametrize("file_format", ["csv", "parquet", "ndjson", "log"])
@pytest.mark.parametrize("file_info", ["name", "uri", None])
def test_file_info_local(tmp_path, file_format, file_info):
    data_path = write_test_data(str(tmp_path), file_format)
    source = make_source(
        publisher_type="local",
        uri=data_path,
        file_format=file_format,
        file_info=file_info,)

    @td.publisher(
        name=f"test_file_info_local_{file_format}_{file_info}",
        source=source,
        tables="output",
    )
    def publisher_fn(df: td.TableFrame) -> td.TableFrame:
        return df

    fnrun = tdfnrun(function=publisher_fn)
    assert fnrun.status == 0

    output = fnrun.output
    assert len(output) == 1

    result_df = output[0].as_pl_df(with_sys_columns=True)
    expected_uri = "file://" + pathname2url(os.path.abspath(data_path))
    verify_file_info_columns(result_df, file_info, file_format, expected_uri)


@pytest.mark.azure
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.parametrize("file_format", ["csv", "parquet", "ndjson", "log"])
@pytest.mark.parametrize("file_info", ["name", "uri", None])
def test_file_info_azure(tmp_path, file_format, file_info, azure_config, azure_client):
    data_path = write_test_data(str(tmp_path), file_format)
    remote_uri = upload_to_azure(data_path, azure_config, azure_client)
    source = make_source(
        publisher_type="azure",
        uri=remote_uri,
        file_format=file_format,
        file_info=file_info,
        azure_config=azure_config,
    )

    @td.publisher(
        name=f"test_file_info_azure_{file_format}_{file_info}",
        source=source,
        tables="output",
    )
    def publisher_fn(df: td.TableFrame) -> td.TableFrame:
        return df

    fnrun = tdfnrun(function=publisher_fn)
    assert fnrun.status == 0

    result_df = fnrun.output[0].as_pl_df(with_sys_columns=True)
    verify_file_info_columns(result_df, file_info, file_format, remote_uri)


@pytest.mark.gcs
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.parametrize("file_format", ["csv", "parquet", "ndjson", "log"])
@pytest.mark.parametrize("file_info", ["name", "uri", None])
def test_file_info_gcs(tmp_path, file_format, file_info, gcs_config, gcs_client):
    data_path = write_test_data(str(tmp_path), file_format)
    remote_uri = upload_to_gcs(data_path, gcs_config, gcs_client)
    source = make_source(
        publisher_type="gcs",
        uri=remote_uri,
        file_format=file_format,
        file_info=file_info,
        gcs_config=gcs_config,
    )

    @td.publisher(
        name=f"test_file_info_gcs_{file_format}_{file_info}",
        source=source,
        tables="output",
    )
    def publisher_fn(df: td.TableFrame) -> td.TableFrame:
        return df

    fnrun = tdfnrun(function=publisher_fn)
    assert fnrun.status == 0

    result_df = fnrun.output[0].as_pl_df(with_sys_columns=True)
    verify_file_info_columns(result_df, file_info, file_format, remote_uri)


@pytest.mark.s3
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.parametrize("file_format", ["csv", "parquet", "ndjson", "log"])
@pytest.mark.parametrize("file_info", ["name", "uri", None])
def test_file_info_s3(tmp_path, file_format, file_info, s3_config, s3_client):
    data_path = write_test_data(str(tmp_path), file_format)
    remote_uri = upload_to_s3(data_path, s3_config, s3_client)
    source = make_source(
        publisher_type="s3",
        uri=remote_uri,
        file_format=file_format,
        file_info=file_info,
        s3_config=s3_config,
    )

    @td.publisher(
        name=f"test_file_info_s3_{file_format}_{file_info}",
        source=source,
        tables="output",
    )
    def publisher_fn(df: td.TableFrame) -> td.TableFrame:
        return df

    fnrun = tdfnrun(function=publisher_fn)
    assert fnrun.status == 0

    result_df = fnrun.output[0].as_pl_df(with_sys_columns=True)
    verify_file_info_columns(result_df, file_info, file_format, remote_uri)
#
# Copyright 2026 Tabsdata Inc.
#

import datetime

import pytest

from tabsdata._credentials import GCPServiceAccountKeyCredentials
from tabsdata._format import (
    AvroFormat,
    CSVFormat,
    LogFormat,
    NDJSONFormat,
    ParquetFormat,
)
from tabsdata._info import FileInfo
from tabsdata._io.connections.file_connections.gcp_connection import GCSConn
from tabsdata._io.inputs.file.gcs_input import GCSSrc
from tabsdata._io.plugin import SourcePlugin
from tabsdata.exceptions import (
    FormatConfigurationError,
    ParameterValidationError,
    SourceConfigurationError,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401

GCS_CREDENTIALS = GCPServiceAccountKeyCredentials(
    service_account_key="test_service_account_key",
)

VALID_BUCKET = "gs://mybucket"


@pytest.fixture
def valid_conn():
    return GCSConn(bucket_uri=VALID_BUCKET, credentials=GCS_CREDENTIALS)


# --- GCSSrc constructor tests ---


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_properties(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet", format=ParquetFormat())
    assert src.conn == valid_conn
    assert src.path == ["data/input.parquet"]
    assert isinstance(src.format, ParquetFormat)
    assert src.file_info is None
    assert src.infer_schema_length == 100
    assert isinstance(src, SourcePlugin)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_path_list(valid_conn):
    paths = ["data/file1.parquet", "data/file2.parquet"]
    src = GCSSrc(conn=valid_conn, path=paths, format=ParquetFormat())
    assert src.path == paths


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_uri_list_computed(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/output.parquet", format=ParquetFormat())
    assert src._uri_list == ["gs://mybucket/data/output.parquet"]


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_uri_list_strips_leading_slash(valid_conn):
    src = GCSSrc(conn=valid_conn, path="/data/output.parquet", format=ParquetFormat())
    assert src._uri_list == ["gs://mybucket/data/output.parquet"]


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_uri_list_multiple(valid_conn):
    paths = ["data/a.parquet", "data/b.parquet"]
    src = GCSSrc(conn=valid_conn, path=paths, format=ParquetFormat())
    assert src._uri_list == [
        "gs://mybucket/data/a.parquet",
        "gs://mybucket/data/b.parquet",
    ]


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_format_inferred_csv(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.csv")
    assert isinstance(src.format, CSVFormat)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_format_inferred_parquet(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet")
    assert isinstance(src.format, ParquetFormat)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_format_inferred_ndjson(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.ndjson")
    assert isinstance(src.format, NDJSONFormat)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_format_inferred_avro(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.avro")
    assert isinstance(src.format, AvroFormat)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_format_explicit_string(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet", format="csv")
    assert isinstance(src.format, CSVFormat)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_format_explicit_object(valid_conn):
    src = GCSSrc(
        conn=valid_conn, path="data/input.parquet", format=CSVFormat(separator=";")
    )
    assert isinstance(src.format, CSVFormat)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_invalid_format_string(valid_conn):
    with pytest.raises(FormatConfigurationError):
        GCSSrc(conn=valid_conn, path="data/input.parquet", format="not_a_format")


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_invalid_format_type(valid_conn):
    with pytest.raises(ParameterValidationError):
        GCSSrc(conn=valid_conn, path="data/input.parquet", format=42)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_invalid_conn_type():
    with pytest.raises(ParameterValidationError):
        GCSSrc(conn="not_a_conn", path="data/input.parquet")


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_supported_formats(valid_conn):
    formats = GCSSrc.SupportedFormats
    assert formats.avro.value == AvroFormat
    assert formats.csv.value == CSVFormat
    assert formats.ndjson.value == NDJSONFormat
    assert formats.log.value == LogFormat
    assert formats.parquet.value == ParquetFormat


# --- initial_last_modified tests ---


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_initial_last_modified_string(valid_conn):
    src = GCSSrc(
        conn=valid_conn,
        path="data/input.parquet",
        initial_last_modified="2024-01-01T00:00:00Z",
    )
    assert src.initial_last_modified is not None
    assert "2024-01-01" in src.initial_last_modified


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_initial_last_modified_datetime(valid_conn):
    dt = datetime.datetime(2024, 6, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    src = GCSSrc(
        conn=valid_conn,
        path="data/input.parquet",
        initial_last_modified=dt,
    )
    assert src.initial_last_modified is not None
    assert "2024-06-15" in src.initial_last_modified


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_initial_last_modified_none(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet")
    assert src.initial_last_modified is None or src.initial_last_modified == ""


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_initial_last_modified_invalid_string(valid_conn):
    with pytest.raises(SourceConfigurationError):
        GCSSrc(
            conn=valid_conn,
            path="data/input.parquet",
            initial_last_modified="not-a-date",
        )


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_initial_last_modified_no_timezone_raises(valid_conn):
    with pytest.raises(SourceConfigurationError):
        GCSSrc(
            conn=valid_conn,
            path="data/input.parquet",
            initial_last_modified="2024-01-01T00:00:00",
        )


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_initial_values_with_last_modified(valid_conn):
    src = GCSSrc(
        conn=valid_conn,
        path="data/input.parquet",
        initial_last_modified="2024-01-01T00:00:00Z",
    )
    initial_values = src.initial_values
    assert isinstance(initial_values, dict)
    assert len(initial_values) > 0


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_initial_values_without_last_modified(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet")
    initial_values = src.initial_values
    assert isinstance(initial_values, dict)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_initial_values_setter(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet")
    src.initial_values = {"key": "value"}
    assert src.initial_values == {"key": "value"}


# --- file_info tests ---


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_file_info_name(valid_conn):
    src = GCSSrc(
        conn=valid_conn,
        path="data/input.parquet",
        file_info="name",
    )
    assert src.file_info == FileInfo.NAME


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_file_info_uri(valid_conn):
    src = GCSSrc(
        conn=valid_conn,
        path="data/input.parquet",
        file_info="uri",
    )
    assert src.file_info == FileInfo.URI


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_file_info_enum(valid_conn):
    src = GCSSrc(
        conn=valid_conn,
        path="data/input.parquet",
        file_info=FileInfo.NAME,
    )
    assert src.file_info == FileInfo.NAME


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_file_info_none(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet")
    assert src.file_info is None


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_file_info_invalid(valid_conn):
    with pytest.raises(FormatConfigurationError):
        GCSSrc(
            conn=valid_conn,
            path="data/input.parquet",
            file_info="invalid_value",
        )


# --- setter tests ---


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_set_conn(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet", format=ParquetFormat())
    conn2 = GCSConn(bucket_uri="gs://otherbucket", credentials=GCS_CREDENTIALS)
    src.conn = conn2
    assert src.conn == conn2


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_set_conn_invalid(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet", format=ParquetFormat())
    with pytest.raises(ParameterValidationError):
        src.conn = "not_a_conn"


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_set_path(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/old.parquet", format=ParquetFormat())
    src.path = "data/new.parquet"
    assert src.path == ["data/new.parquet"]
    assert src._uri_list == ["gs://mybucket/data/new.parquet"]


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_set_path_list(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/old.parquet", format=ParquetFormat())
    new_paths = ["data/a.parquet", "data/b.parquet"]
    src.path = new_paths
    assert src.path == new_paths
    assert src._uri_list == [
        "gs://mybucket/data/a.parquet",
        "gs://mybucket/data/b.parquet",
    ]


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_set_format(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet", format=ParquetFormat())
    src.format = CSVFormat()
    assert isinstance(src.format, CSVFormat)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_set_format_string(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet", format=ParquetFormat())
    src.format = "csv"
    assert isinstance(src.format, CSVFormat)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_set_format_invalid(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet", format=ParquetFormat())
    with pytest.raises(FormatConfigurationError):
        src.format = "not_a_format"


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_set_format_invalid_type(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet", format=ParquetFormat())
    with pytest.raises(ParameterValidationError):
        src.format = 42


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_set_file_info(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet")
    assert src.file_info is None
    src.file_info = "name"
    assert src.file_info == FileInfo.NAME
    src.file_info = "uri"
    assert src.file_info == FileInfo.URI
    src.file_info = None
    assert src.file_info is None


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_set_file_info_invalid(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet")
    with pytest.raises(FormatConfigurationError):
        src.file_info = "invalid"


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_set_initial_last_modified(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet")
    src.initial_last_modified = "2025-03-01T00:00:00Z"
    assert "2025-03-01" in src.initial_last_modified


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_set_initial_last_modified_invalid(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.parquet")
    with pytest.raises(SourceConfigurationError):
        src.initial_last_modified = "not-a-date"


# --- wildcard path tests ---


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_wildcard_path(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/*.parquet", format=ParquetFormat())
    assert src.path == ["data/*.parquet"]
    assert src._uri_list == ["gs://mybucket/data/*.parquet"]


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_wildcard_path_question_mark(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/file_?.parquet", format=ParquetFormat())
    assert src.path == ["data/file_?.parquet"]


# --- infer_schema_length tests ---


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_infer_schema_length_default(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.csv")
    assert src.infer_schema_length == 100


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_infer_schema_length_explicit(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.csv", infer_schema_length=100)
    assert src.infer_schema_length == 100


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_infer_schema_length_one(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.csv", infer_schema_length=1)
    assert src.infer_schema_length == 1


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_infer_schema_length_zero_rejected(valid_conn):
    with pytest.raises(ParameterValidationError):
        GCSSrc(conn=valid_conn, path="data/input.csv", infer_schema_length=0)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_infer_schema_length_negative_rejected(valid_conn):
    with pytest.raises(ParameterValidationError):
        GCSSrc(conn=valid_conn, path="data/input.csv", infer_schema_length=-1)


@pytest.mark.gcs
@pytest.mark.unit
def test_gcssrc_set_infer_schema_length(valid_conn):
    src = GCSSrc(conn=valid_conn, path="data/input.csv")
    assert src.infer_schema_length == 100
    src.infer_schema_length = 500
    assert src.infer_schema_length == 500
    src.infer_schema_length = None
    assert src.infer_schema_length is None

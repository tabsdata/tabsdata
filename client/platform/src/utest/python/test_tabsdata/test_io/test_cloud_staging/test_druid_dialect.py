#
# Copyright 2026 Tabs Data Inc.
#

import json
import logging
import sys
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
import requests

from tabsdata._credentials import (
    AzureAccountKeyCredentials,
    GCPServiceAccountKeyCredentials,
    HMACCredentials,
    S3AccessKeyCredentials,
)
from tabsdata._io.cloud_staging.common.modes import (
    LoadMode,
    SchemaEvolutionMode,
    WriteMode,
)
from tabsdata._io.cloud_staging.common.statuses import BulkLoadStatus
from tabsdata._io.cloud_staging.common.storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)
from tabsdata._io.cloud_staging.druid.cloud_storage import (
    AzureDruidStorage,
    DruidCloudStorage,
    GCSDruidStorage,
    S3DruidStorage,
)
from tabsdata._io.cloud_staging.druid.config import DruidConfig
from tabsdata._io.cloud_staging.druid.dialect import (
    DruidDialect,
    _pyarrow_type_to_druid,
)



pytestmark = [pytest.mark.xdist_group("platform-druid_dialect_seq")]

# ── Fixtures ──────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_requests_module():
    """Patch requests so the dialect constructor works without the package."""
    mock_module = MagicMock()
    mock_session = MagicMock()
    mock_session.auth = None
    mock_module.Session.return_value = mock_session

    # Use real exception classes so try/except works with isinstance
    mock_module.ConnectionError = requests.ConnectionError
    mock_module.Timeout = requests.Timeout
    mock_module.HTTPError = requests.HTTPError
    mock_module.RequestException = requests.RequestException

    with patch.dict(sys.modules, {"requests": mock_module}):
        yield mock_module, mock_session


@pytest.fixture
def s3_config():
    return S3Config(
        bucket="my-bucket",
        region="us-east-1",
        base_path="staging/druid",
        credentials=S3AccessKeyCredentials("AKIAIOSFODNN7EXAMPLE", "wJalrXSecret"),
    )


@pytest.fixture
def azure_config():
    return AzureConfig(
        container="my-container",
        base_path="staging/druid",
        credentials=AzureAccountKeyCredentials("myaccount", "base64key=="),
    )


@pytest.fixture
def gcs_config():
    return GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging/druid",
        upload_credentials=GCPServiceAccountKeyCredentials(
            json.dumps({
                "type": "service_account",
                "project_id": "test",
                "private_key": "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n",
                "client_email": "test@test.iam.gserviceaccount.com",
            })
        ),
        hmac_credentials=HMACCredentials("hmac-access-key", "hmac-secret-key"),
    )


@pytest.fixture
def druid_config():
    return DruidConfig(
        overlord_url="http://druid-overlord:8090",
        time_column="event_ts",
        segment_granularity="DAY",
        max_concurrent_subtasks=4,
        max_rows_per_segment=5_000_000,
        max_rows_in_memory=1_000_000,
    )


@pytest.fixture
def druid_config_no_time():
    return DruidConfig(
        overlord_url="http://druid-overlord:8090",
    )


@pytest.fixture
def druid_config_with_auth():
    return DruidConfig(
        overlord_url="http://druid-overlord:8090",
        auth_user="admin",
        auth_password="secret",
    )


@pytest.fixture
def mock_cloud_storage():
    storage = MagicMock(spec=DruidCloudStorage)
    storage.druid_input_source.return_value = {
        "type": "s3",
        "uris": ["s3://my-bucket/staging/druid/test-id/events.parquet"],
        "properties": {
            "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
            "secretAccessKey": "wJalrXSecret",
        },
    }
    return storage


@pytest.fixture
def dialect(mock_requests_module, druid_config, mock_cloud_storage):
    """Build a DruidDialect while the requests mock is active."""
    return DruidDialect(druid_config, mock_cloud_storage)


@pytest.fixture
def session(dialect):
    """Returns the mock requests.Session stored inside the dialect."""
    return dialect._session


@pytest.fixture
def load_mode_append():
    return LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id="test-upload-id",
    )


@pytest.fixture
def load_mode_replace():
    return LoadMode(
        write_mode=WriteMode.REPLACE,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id="test-upload-id",
    )


# ── PyArrow → Druid type mapping ─────────────────────────────────────────────────────


@pytest.mark.druid
class TestPyarrowTypeToDruid:
    def test_bool(self):
        assert _pyarrow_type_to_druid(pa.bool_()) == "LONG"

    def test_int8(self):
        assert _pyarrow_type_to_druid(pa.int8()) == "LONG"

    def test_int16(self):
        assert _pyarrow_type_to_druid(pa.int16()) == "LONG"

    def test_int32(self):
        assert _pyarrow_type_to_druid(pa.int32()) == "LONG"

    def test_int64(self):
        assert _pyarrow_type_to_druid(pa.int64()) == "LONG"

    def test_uint8(self):
        assert _pyarrow_type_to_druid(pa.uint8()) == "LONG"

    def test_uint16(self):
        assert _pyarrow_type_to_druid(pa.uint16()) == "LONG"

    def test_uint32(self):
        assert _pyarrow_type_to_druid(pa.uint32()) == "LONG"

    def test_uint64(self):
        assert _pyarrow_type_to_druid(pa.uint64()) == "LONG"

    def test_uint64_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            _pyarrow_type_to_druid(pa.uint64())
        assert "uint64" in caplog.text
        assert "overflow" in caplog.text

    def test_float16(self):
        assert _pyarrow_type_to_druid(pa.float16()) == "FLOAT"

    def test_float32(self):
        assert _pyarrow_type_to_druid(pa.float32()) == "FLOAT"

    def test_float64(self):
        assert _pyarrow_type_to_druid(pa.float64()) == "DOUBLE"

    def test_decimal128(self):
        assert _pyarrow_type_to_druid(pa.decimal128(18, 6)) == "DOUBLE"

    def test_decimal128_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            _pyarrow_type_to_druid(pa.decimal128(18, 6))
        assert "precision" in caplog.text

    def test_string(self):
        assert _pyarrow_type_to_druid(pa.string()) == "STRING"

    def test_utf8(self):
        assert _pyarrow_type_to_druid(pa.utf8()) == "STRING"

    def test_large_string(self):
        assert _pyarrow_type_to_druid(pa.large_string()) == "STRING"

    def test_binary(self):
        assert _pyarrow_type_to_druid(pa.binary()) == "STRING"

    def test_large_binary(self):
        assert _pyarrow_type_to_druid(pa.large_binary()) == "STRING"

    def test_date32(self):
        assert _pyarrow_type_to_druid(pa.date32()) == "LONG"

    def test_timestamp_s(self):
        assert _pyarrow_type_to_druid(pa.timestamp("s")) == "LONG"

    def test_timestamp_ms(self):
        assert _pyarrow_type_to_druid(pa.timestamp("ms")) == "LONG"

    def test_timestamp_us(self):
        assert _pyarrow_type_to_druid(pa.timestamp("us")) == "LONG"

    def test_timestamp_ns(self):
        assert _pyarrow_type_to_druid(pa.timestamp("ns")) == "LONG"

    def test_timestamp_with_tz(self):
        assert _pyarrow_type_to_druid(pa.timestamp("us", tz="UTC")) == "LONG"

    def test_list_string(self):
        assert _pyarrow_type_to_druid(pa.list_(pa.string())) == "ARRAY<STRING>"

    def test_list_int32(self):
        assert _pyarrow_type_to_druid(pa.list_(pa.int32())) == "ARRAY<LONG>"

    def test_list_int64(self):
        assert _pyarrow_type_to_druid(pa.list_(pa.int64())) == "ARRAY<LONG>"

    def test_list_float64(self):
        assert _pyarrow_type_to_druid(pa.list_(pa.float64())) == "ARRAY<DOUBLE>"

    def test_list_bool(self):
        assert _pyarrow_type_to_druid(pa.list_(pa.bool_())) == "ARRAY<LONG>"

    def test_list_decimal(self):
        assert _pyarrow_type_to_druid(pa.list_(pa.decimal128(10, 2))) == "ARRAY<DOUBLE>"

    def test_list_struct_falls_back_to_json(self):
        inner = pa.struct([pa.field("x", pa.int32())])
        assert _pyarrow_type_to_druid(pa.list_(inner)) == "COMPLEX<json>"

    def test_large_list_string(self):
        assert _pyarrow_type_to_druid(pa.large_list(pa.string())) == "ARRAY<STRING>"

    def test_map_string_int(self):
        assert _pyarrow_type_to_druid(pa.map_(pa.string(), pa.int32())) == "COMPLEX<json>"

    def test_struct(self):
        st = pa.struct([pa.field("a", pa.int32()), pa.field("b", pa.string())])
        assert _pyarrow_type_to_druid(st) == "COMPLEX<json>"

    def test_unsupported_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported PyArrow type"):
            _pyarrow_type_to_druid(pa.duration("s"))


# ── DruidDialect capability flags ─────────────────────────────────────────────────────


@pytest.mark.druid
class TestDruidDialectCapabilities:
    def test_supports_tabsdata_schema_evolution(self, dialect):
        assert dialect.supports_tabsdata_schema_evolution() is False

    def test_supports_native_schema_evolution(self, dialect):
        assert dialect.supports_native_schema_evolution() is False

    def test_supports_transactions(self, dialect):
        assert dialect.supports_transactions() is False

    def test_supports_auto_cleanup(self, dialect):
        assert dialect.supports_auto_cleanup() is False

    def test_is_bulk_load_async(self, dialect):
        assert dialect.is_bulk_load_async() is True


# ── DruidDialect schema operations ────────────────────────────────────────────────────


@pytest.mark.druid
class TestDruidDialectSchemaOps:
    def test_table_exists_returns_false(self, dialect, load_mode_append):
        assert dialect.table_exists(load_mode_append, "events") is False

    def test_get_table_schema_returns_empty(self, dialect, load_mode_append):
        result = dialect.get_table_schema(load_mode_append, "events")
        assert result == pa.schema([])

    def test_create_table_is_noop(self, dialect, load_mode_append):
        arrow_schema = pa.schema([pa.field("col1", pa.int64())])
        dialect.create_table(load_mode_append, "events", arrow_schema)

    def test_verify_schema_always_true(self, dialect, load_mode_append):
        arrow_schema = pa.schema([pa.field("col1", pa.int64())])
        assert dialect.verify_schema(load_mode_append, "events", arrow_schema) is True

    def test_truncate_table_is_noop(self, dialect, load_mode_replace):
        dialect.truncate_table(load_mode_replace, "events")


# ── DruidDialect data operations ──────────────────────────────────────────────────────


@pytest.mark.druid
class TestDruidDialectBulkLoad:
    def test_bulk_load_returns_pending(self, dialect, session, load_mode_append):
        mock_response = MagicMock()
        mock_response.json.return_value = {"task": "task-abc-123"}
        session.post.return_value = mock_response

        result = dialect.bulk_load(
            load_mode_append,
            "events",
            ["s3://my-bucket/staging/druid/test-id/events.parquet"],
        )
        assert result.status == BulkLoadStatus.PENDING
        assert result.job_handle == "task-abc-123"

    def test_bulk_load_posts_to_overlord(self, dialect, session, load_mode_append):
        mock_response = MagicMock()
        mock_response.json.return_value = {"task": "task-abc-123"}
        session.post.return_value = mock_response

        dialect.bulk_load(
            load_mode_append,
            "events",
            ["s3://my-bucket/staging/druid/test-id/events.parquet"],
        )
        session.post.assert_called()
        call_url = session.post.call_args[0][0]
        assert "/druid/indexer/v1/task" in call_url

    def test_poll_bulk_load_running(self, dialect, session, load_mode_append):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": {"statusCode": "RUNNING"}
        }
        session.get.return_value = mock_response

        result = dialect.poll_bulk_load(load_mode_append, "task-abc-123")
        assert result.status == BulkLoadStatus.PENDING
        assert result.job_handle == "task-abc-123"

    def test_poll_bulk_load_success(self, dialect, session, load_mode_append):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": {"statusCode": "SUCCESS"}
        }
        session.get.return_value = mock_response

        result = dialect.poll_bulk_load(load_mode_append, "task-abc-123")
        assert result.status == BulkLoadStatus.COMPLETED

    def test_poll_bulk_load_failed(self, dialect, session, load_mode_append):
        status_response = MagicMock()
        status_response.json.return_value = {
            "status": {"statusCode": "FAILED"}
        }
        reports_response = MagicMock()
        reports_response.json.return_value = {
            "ingestionStatsAndErrors": {
                "payload": {"errorMsg": "Out of memory"}
            }
        }
        session.get.side_effect = [status_response, reports_response]

        result = dialect.poll_bulk_load(load_mode_append, "task-abc-123")
        assert result.status == BulkLoadStatus.FAILED
        assert "Out of memory" in result.error


# ── Ingestion spec building ───────────────────────────────────────────────────────────


@pytest.mark.druid
class TestBuildIngestionSpec:
    def test_append_mode_spec(self, dialect):
        spec = dialect._build_ingestion_spec(
            "events",
            ["s3://my-bucket/staging/druid/test-id/events.parquet"],
            is_replace=False,
        )
        assert spec["type"] == "index_parallel"
        io_config = spec["spec"]["ioConfig"]
        assert io_config["appendToExisting"] is True
        assert io_config["dropExisting"] is False
        assert io_config["inputFormat"]["type"] == "parquet"

    def test_replace_mode_spec(self, dialect):
        spec = dialect._build_ingestion_spec(
            "events",
            ["s3://my-bucket/staging/druid/test-id/events.parquet"],
            is_replace=True,
        )
        io_config = spec["spec"]["ioConfig"]
        assert io_config["appendToExisting"] is False
        assert io_config["dropExisting"] is True

    def test_spec_with_time_column(self, dialect):
        spec = dialect._build_ingestion_spec("events", [], is_replace=False)
        ts_spec = spec["spec"]["dataSchema"]["timestampSpec"]
        assert ts_spec["column"] == "event_ts"
        assert ts_spec["format"] == "auto"
        assert "missingValue" not in ts_spec

    def test_spec_without_time_column(
        self, mock_requests_module, druid_config_no_time, mock_cloud_storage
    ):
        dialect = DruidDialect(druid_config_no_time, mock_cloud_storage)
        spec = dialect._build_ingestion_spec("events", [], is_replace=False)
        ts_spec = spec["spec"]["dataSchema"]["timestampSpec"]
        assert ts_spec["column"] == "__td_dummy_time"
        assert ts_spec["missingValue"] == "2000-01-01T00:00:00.000Z"

    def test_spec_data_schema(self, dialect):
        spec = dialect._build_ingestion_spec("events", [], is_replace=False)
        data_schema = spec["spec"]["dataSchema"]
        assert data_schema["dataSource"] == "events"
        assert data_schema["dimensionsSpec"]["useSchemaDiscovery"] is True
        assert data_schema["granularitySpec"]["segmentGranularity"] == "DAY"
        assert data_schema["granularitySpec"]["rollup"] is False

    def test_spec_tuning_config(self, dialect):
        spec = dialect._build_ingestion_spec("events", [], is_replace=False)
        tuning = spec["spec"]["tuningConfig"]
        assert tuning["type"] == "index_parallel"
        assert tuning["maxNumConcurrentSubTasks"] == 4
        assert tuning["maxRowsInMemory"] == 1_000_000
        assert tuning["partitionsSpec"]["type"] == "dynamic"
        assert tuning["partitionsSpec"]["maxRowsPerSegment"] == 5_000_000


# ── DruidDialect with authentication ──────────────────────────────────────────────────


@pytest.mark.druid
class TestDruidDialectAuth:
    def test_basic_auth_configured(
        self, mock_requests_module, druid_config_with_auth, mock_cloud_storage
    ):
        dialect = DruidDialect(druid_config_with_auth, mock_cloud_storage)
        assert dialect._session.auth == ("admin", "secret")

    def test_no_auth_when_not_configured(self, dialect, session):
        assert session.auth is None


# ── Cloud storage input source builders ───────────────────────────────────────────────


@pytest.mark.druid
class TestS3DruidStorage:
    def test_https_to_s3_uri(self, s3_config):
        storage = S3DruidStorage(s3_config)
        https_url = (
            "https://my-bucket.s3.us-east-1.amazonaws.com/"
            "staging/druid/id/file.parquet"
        )
        assert storage._https_to_s3_uri(https_url) == (
            "s3://my-bucket/staging/druid/id/file.parquet"
        )

    def test_https_to_s3_uri_dotted_bucket(self):
        config = S3Config(
            bucket="my.data.bucket",
            region="us-east-1",
            base_path="staging/druid",
            credentials=S3AccessKeyCredentials(
                "AKIAIOSFODNN7EXAMPLE", "wJalrXSecret"
            ),
        )
        storage = S3DruidStorage(config)
        https_url = (
            "https://my.data.bucket.s3.us-east-1.amazonaws.com/"
            "staging/druid/id/file.parquet"
        )
        assert storage._https_to_s3_uri(https_url) == (
            "s3://my.data.bucket/staging/druid/id/file.parquet"
        )

    def test_https_to_s3_uri_bad_format_raises(self, s3_config):
        storage = S3DruidStorage(s3_config)
        unknown_url = "https://unknown-format.example.com/file.parquet"
        with pytest.raises(ValueError, match="Cannot convert S3 URL"):
            storage._https_to_s3_uri(unknown_url)

    def test_druid_input_source(self, s3_config):
        storage = S3DruidStorage(s3_config)
        refs = [
            "https://my-bucket.s3.us-east-1.amazonaws.com/"
            "staging/druid/id/file1.parquet",
            "https://my-bucket.s3.us-east-1.amazonaws.com/"
            "staging/druid/id/file2.parquet",
        ]
        result = storage.druid_input_source(refs)
        assert result["type"] == "s3"
        assert len(result["uris"]) == 2
        assert result["uris"][0] == "s3://my-bucket/staging/druid/id/file1.parquet"
        assert result["uris"][1] == "s3://my-bucket/staging/druid/id/file2.parquet"
        assert "accessKeyId" in result["properties"]
        assert "secretAccessKey" in result["properties"]


@pytest.mark.druid
class TestGCSDruidStorage:
    def test_convert_gcs_url_gs(self, gcs_config):
        storage = GCSDruidStorage(gcs_config)
        https_url = (
            "https://storage.googleapis.com/my-gcs-bucket/"
            "staging/druid/id/file.parquet"
        )
        assert storage._convert_gcs_url(https_url, "gs") == (
            "gs://my-gcs-bucket/staging/druid/id/file.parquet"
        )

    def test_convert_gcs_url_gs_bad_format_raises(self, gcs_config):
        storage = GCSDruidStorage(gcs_config)
        unknown_url = "https://unknown-format.example.com/file.parquet"
        with pytest.raises(ValueError, match="Cannot convert GCS URL"):
            storage._convert_gcs_url(unknown_url, "gs")

    def test_convert_gcs_url_s3(self, gcs_config):
        storage = GCSDruidStorage(gcs_config)
        https_url = (
            "https://storage.googleapis.com/my-gcs-bucket/"
            "staging/druid/id/file.parquet"
        )
        assert storage._convert_gcs_url(https_url, "s3") == (
            "s3://my-gcs-bucket/staging/druid/id/file.parquet"
        )

    def test_convert_gcs_url_s3_bad_format_raises(self, gcs_config):
        storage = GCSDruidStorage(gcs_config)
        unknown_url = "https://unknown-format.example.com/file.parquet"
        with pytest.raises(ValueError, match="Cannot convert GCS URL"):
            storage._convert_gcs_url(unknown_url, "s3")

    def test_druid_input_source_with_hmac(self, gcs_config):
        storage = GCSDruidStorage(gcs_config)
        refs = [
            "https://storage.googleapis.com/my-gcs-bucket/"
            "staging/druid/id/file.parquet",
        ]
        result = storage.druid_input_source(refs)
        assert result["type"] == "s3"
        assert result["uris"][0] == (
            "s3://my-gcs-bucket/staging/druid/id/file.parquet"
        )
        assert result["endpointConfig"]["url"] == "https://storage.googleapis.com"
        assert result["properties"]["accessKeyId"] == "hmac-access-key"
        assert result["properties"]["secretAccessKey"] == "hmac-secret-key"

    def test_druid_input_source_without_hmac(self):
        config = GCSConfig(bucket="my-bucket", base_path="staging/druid")
        storage = GCSDruidStorage(config)
        refs = [
            "https://storage.googleapis.com/my-bucket/"
            "staging/druid/id/file.parquet",
        ]
        result = storage.druid_input_source(refs)
        assert result["type"] == "google"
        assert result["uris"][0] == (
            "gs://my-bucket/staging/druid/id/file.parquet"
        )
        assert "properties" not in result


@pytest.mark.druid
class TestAzureDruidStorage:
    def test_druid_input_source(self, azure_config):
        storage = AzureDruidStorage(azure_config)
        refs = ["staging/druid/id/file.parquet"]
        result = storage.druid_input_source(refs)
        assert result["type"] == "azureStorage"
        assert len(result["uris"]) == 1
        assert "azureStorage://myaccount/my-container/" in result["uris"][0]
        assert "key" in result["properties"]


# ── Replace mode: intervals in granularitySpec ─────────────────────────────────────


@pytest.mark.druid
class TestReplaceModIntervals:
    def test_replace_mode_has_intervals(self, dialect):
        spec = dialect._build_ingestion_spec("events", [], is_replace=True)
        granularity = spec["spec"]["dataSchema"]["granularitySpec"]
        assert "intervals" in granularity
        assert granularity["intervals"] == ["1970-01-01/2100-01-01"]

    def test_append_mode_has_no_intervals(self, dialect):
        spec = dialect._build_ingestion_spec("events", [], is_replace=False)
        granularity = spec["spec"]["dataSchema"]["granularitySpec"]
        assert "intervals" not in granularity


# ── WAITING status in poll_bulk_load ─────────────────────────────────────────────────


@pytest.mark.druid
class TestPollBulkLoadWaiting:
    def test_poll_bulk_load_waiting(self, dialect, session, load_mode_append):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": {"statusCode": "WAITING"}
        }
        session.get.return_value = mock_response

        result = dialect.poll_bulk_load(load_mode_append, "task-abc-123")
        assert result.status == BulkLoadStatus.PENDING
        assert result.job_handle == "task-abc-123"


# ── HTTP/JSON error handling ─────────────────────────────────────────────────────────


@pytest.mark.druid
class TestBulkLoadErrorHandling:
    def test_bulk_load_http_error_raises_runtime(
        self, dialect, session, load_mode_append
    ):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError(
            "503 Server Error", response=mock_response
        )
        session.post.return_value = mock_response

        with pytest.raises(RuntimeError, match="Failed to submit Druid"):
            dialect.bulk_load(
                load_mode_append,
                "events",
                ["s3://bucket/file.parquet"],
            )

    def test_bulk_load_missing_task_key_raises(
        self, dialect, session, load_mode_append
    ):
        mock_response = MagicMock()
        mock_response.json.return_value = {"unexpected": "response"}
        mock_response.text = '{"unexpected": "response"}'
        session.post.return_value = mock_response

        with pytest.raises(RuntimeError, match="missing 'task' field"):
            dialect.bulk_load(
                load_mode_append,
                "events",
                ["s3://bucket/file.parquet"],
            )

    def test_poll_http_error_raises_runtime(
        self, dialect, session, load_mode_append
    ):
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("503 Server Error")
        session.get.return_value = mock_response

        with pytest.raises(RuntimeError, match="Failed to poll Druid"):
            dialect.poll_bulk_load(load_mode_append, "task-abc-123")

    def test_poll_missing_status_key_raises(
        self, dialect, session, load_mode_append
    ):
        mock_response = MagicMock()
        mock_response.json.return_value = {"unexpected": "response"}
        mock_response.text = '{"unexpected": "response"}'
        session.get.return_value = mock_response

        with pytest.raises(RuntimeError, match="missing 'status.statusCode'"):
            dialect.poll_bulk_load(load_mode_append, "task-abc-123")


# ── Partial auth raises ValueError ─────────────────────────────────────────────────


@pytest.mark.druid
class TestDruidDialectPartialAuth:
    def test_partial_auth_user_only_raises(
        self, mock_requests_module, mock_cloud_storage
    ):
        config = DruidConfig(
            overlord_url="http://druid-overlord:8090",
            auth_user="admin",
            auth_password=None,
        )
        with pytest.raises(ValueError, match="partially configured"):
            DruidDialect(config, mock_cloud_storage)

    def test_partial_auth_password_only_raises(
        self, mock_requests_module, mock_cloud_storage
    ):
        config = DruidConfig(
            overlord_url="http://druid-overlord:8090",
            auth_user=None,
            auth_password="secret",
        )
        with pytest.raises(ValueError, match="partially configured"):
            DruidDialect(config, mock_cloud_storage)


# ── DruidConfig __repr__ masks password ──────────────────────────────────────────────


@pytest.mark.druid
class TestDruidConfigRepr:
    def test_repr_masks_password(self):
        config = DruidConfig(
            overlord_url="http://druid-overlord:8090",
            auth_user="admin",
            auth_password="my-secret-password",
        )
        r = repr(config)
        assert "my-secret-password" not in r
        assert "***" in r
        assert "admin" in r
        assert "DruidConfig" in r

    def test_repr_shows_none_when_no_password(self):
        config = DruidConfig(
            overlord_url="http://druid-overlord:8090",
        )
        r = repr(config)
        assert "None" in r
        assert "***" not in r

    def test_repr_contains_all_fields(self):
        config = DruidConfig(
            overlord_url="http://druid-overlord:8090",
            time_column="event_ts",
            segment_granularity="DAY",
            max_concurrent_subtasks=4,
            max_rows_per_segment=10_000_000,
            max_rows_in_memory=500_000,
            http_timeout=600,
            auth_user="admin",
            auth_password="secret",
        )
        r = repr(config)
        assert "druid-overlord" in r
        assert "event_ts" in r
        assert "DAY" in r
        assert "4" in r
        assert "10000000" in r
        assert "500000" in r
        assert "600" in r


# ── HTTP timeout ───────────────────────────────────────────────────────────────────


@pytest.mark.druid
class TestDruidDialectTimeout:
    def test_bulk_load_passes_timeout(self, dialect, session, load_mode_append):
        mock_response = MagicMock()
        mock_response.json.return_value = {"task": "task-abc-123"}
        session.post.return_value = mock_response

        dialect.bulk_load(
            load_mode_append,
            "events",
            ["s3://my-bucket/staging/druid/test-id/events.parquet"],
        )
        _, kwargs = session.post.call_args
        assert kwargs["timeout"] == 300  # default DruidConfig.http_timeout

    def test_poll_bulk_load_passes_timeout(
        self, dialect, session, load_mode_append
    ):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": {"statusCode": "RUNNING"}
        }
        session.get.return_value = mock_response

        dialect.poll_bulk_load(load_mode_append, "task-abc-123")
        _, kwargs = session.get.call_args
        assert kwargs["timeout"] == 300  # default DruidConfig.http_timeout

    def test_custom_http_timeout(
        self, mock_requests_module, mock_cloud_storage
    ):
        config = DruidConfig(
            overlord_url="http://druid-overlord:8090",
            http_timeout=600,
        )
        dialect = DruidDialect(config, mock_cloud_storage)
        session = dialect._session

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "status": {"statusCode": "RUNNING"}
        }
        session.get.return_value = mock_response

        load_mode = LoadMode(
            write_mode=WriteMode.APPEND,
            schema_evolution=SchemaEvolutionMode.NONE,
            upload_id="test",
        )
        dialect.poll_bulk_load(load_mode, "task-abc")
        _, kwargs = session.get.call_args
        assert kwargs["timeout"] == 600


# ── Constructor ────────────────────────────────────────────────────────────────────


@pytest.mark.druid
class TestDruidDialectConstructor:
    def test_constructor_sets_content_type_header(self, dialect):
        dialect._session.headers.__setitem__.assert_called_with(
            "Content-Type", "application/json"
        )


# ── Session close / context manager ────────────────────────────────────────────────


@pytest.mark.druid
class TestDruidDialectSessionClose:
    def test_close_closes_session(self, dialect, session):
        dialect.close()
        session.close.assert_called_once()

    def test_context_manager(self, mock_requests_module, druid_config, mock_cloud_storage):
        with DruidDialect(druid_config, mock_cloud_storage) as d:
            assert d is not None
        d._session.close.assert_called_once()


# ── Retry logic ────────────────────────────────────────────────────────────────────


@pytest.mark.druid
class TestDruidDialectRetry:
    @patch("tabsdata._io.cloud_staging.druid.dialect.time.sleep")
    def test_retry_on_connection_error(
        self, mock_sleep, dialect, session, load_mode_append
    ):
        """Connection error on first attempt, success on second."""
        success_response = MagicMock()
        success_response.json.return_value = {"task": "task-retry-ok"}
        session.post.side_effect = [
            requests.ConnectionError("Connection refused"),
            success_response,
        ]

        result = dialect.bulk_load(
            load_mode_append, "events", ["s3://bucket/file.parquet"]
        )
        assert result.status == BulkLoadStatus.PENDING
        assert result.job_handle == "task-retry-ok"
        assert session.post.call_count == 2
        mock_sleep.assert_called_once()

    @patch("tabsdata._io.cloud_staging.druid.dialect.time.sleep")
    def test_retry_on_timeout(
        self, mock_sleep, dialect, session, load_mode_append
    ):
        """Timeout on first attempt, success on second."""
        success_response = MagicMock()
        success_response.json.return_value = {
            "status": {"statusCode": "RUNNING"}
        }
        session.get.side_effect = [
            requests.Timeout("Read timed out"),
            success_response,
        ]

        result = dialect.poll_bulk_load(load_mode_append, "task-abc-123")
        assert result.status == BulkLoadStatus.PENDING
        assert session.get.call_count == 2

    @patch("tabsdata._io.cloud_staging.druid.dialect.time.sleep")
    def test_retry_exhausted_raises(
        self, mock_sleep, dialect, session, load_mode_append
    ):
        """All retry attempts fail — exception propagates as RuntimeError."""
        session.post.side_effect = requests.ConnectionError("Connection refused")

        with pytest.raises(RuntimeError, match="Failed to submit Druid"):
            dialect.bulk_load(
                load_mode_append, "events", ["s3://bucket/file.parquet"]
            )
        assert session.post.call_count == 3  # DEFAULT_RETRY_ATTEMPTS

    def test_http_error_not_retried(self, dialect, session, load_mode_append):
        """HTTPError is not retried — raises immediately."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError(
            "400 Bad Request", response=mock_response
        )
        session.post.return_value = mock_response

        with pytest.raises(RuntimeError):
            dialect.bulk_load(
                load_mode_append, "events", ["s3://bucket/file.parquet"]
            )
        assert session.post.call_count == 1


# ── DruidConfig URL validation ──────────────────────────────────────────────────


@pytest.mark.druid
class TestDruidConfigURLValidation:
    def test_valid_http_url(self):
        config = DruidConfig(overlord_url="http://druid-overlord:8090")
        assert config.overlord_url == "http://druid-overlord:8090"

    def test_valid_https_url(self):
        config = DruidConfig(overlord_url="https://druid.example.com:8090")
        assert config.overlord_url == "https://druid.example.com:8090"

    def test_valid_https_with_path(self):
        config = DruidConfig(overlord_url="https://druid.example.com/prefix")
        assert config.overlord_url == "https://druid.example.com/prefix"

    def test_ftp_scheme_raises(self):
        with pytest.raises(ValueError, match="http or https"):
            DruidConfig(overlord_url="ftp://druid-overlord:8090")

    def test_no_scheme_raises(self):
        with pytest.raises(ValueError, match="http or https"):
            DruidConfig(overlord_url="druid-overlord:8090")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            DruidConfig(overlord_url="")

    def test_missing_hostname_raises(self):
        with pytest.raises(ValueError, match="hostname"):
            DruidConfig(overlord_url="http://")

    def test_embedded_credentials_raises(self):
        with pytest.raises(ValueError, match="embedded credentials"):
            DruidConfig(overlord_url="http://admin:secret@druid:8090")

    def test_embedded_user_only_raises(self):
        with pytest.raises(ValueError, match="embedded credentials"):
            DruidConfig(overlord_url="http://admin@druid:8090")

    def test_query_string_raises(self):
        with pytest.raises(ValueError, match="query string"):
            DruidConfig(overlord_url="http://druid:8090?cluster=prod")

    def test_fragment_raises(self):
        with pytest.raises(ValueError, match="fragment"):
            DruidConfig(overlord_url="http://druid:8090#admin")


# ── DruidConfig numeric range validation ─────────────────────────────────────────


@pytest.mark.druid
class TestDruidConfigNumericValidation:
    def test_max_concurrent_subtasks_zero_raises(self):
        with pytest.raises(ValueError, match="max_concurrent_subtasks must be >= 1"):
            DruidConfig(
                overlord_url="http://druid:8090", max_concurrent_subtasks=0
            )

    def test_max_concurrent_subtasks_negative_raises(self):
        with pytest.raises(ValueError, match="max_concurrent_subtasks must be >= 1"):
            DruidConfig(
                overlord_url="http://druid:8090", max_concurrent_subtasks=-1
            )

    def test_max_rows_per_segment_zero_raises(self):
        with pytest.raises(ValueError, match="max_rows_per_segment must be >= 1"):
            DruidConfig(
                overlord_url="http://druid:8090", max_rows_per_segment=0
            )

    def test_max_rows_in_memory_zero_raises(self):
        with pytest.raises(ValueError, match="max_rows_in_memory must be >= 1"):
            DruidConfig(
                overlord_url="http://druid:8090", max_rows_in_memory=0
            )

    def test_http_timeout_zero_raises(self):
        with pytest.raises(ValueError, match="http_timeout must be >= 1"):
            DruidConfig(
                overlord_url="http://druid:8090", http_timeout=0
            )

    def test_http_timeout_negative_raises(self):
        with pytest.raises(ValueError, match="http_timeout must be >= 1"):
            DruidConfig(
                overlord_url="http://druid:8090", http_timeout=-10
            )

    def test_valid_minimums(self):
        config = DruidConfig(
            overlord_url="http://druid:8090",
            max_concurrent_subtasks=1,
            max_rows_per_segment=1,
            max_rows_in_memory=1,
            http_timeout=1,
        )
        assert config.max_concurrent_subtasks == 1
        assert config.max_rows_per_segment == 1
        assert config.max_rows_in_memory == 1
        assert config.http_timeout == 1


# ── URL sanitization in error messages ────────────────────────────────────────────


@pytest.mark.druid
class TestURLSanitization:
    def test_sanitize_url_no_credentials(self):
        from tabsdata._io.cloud_staging.druid.config import _sanitize_url

        assert _sanitize_url("http://druid:8090") == "http://druid:8090"

    def test_sanitize_url_with_user_and_password(self):
        from tabsdata._io.cloud_staging.druid.config import _sanitize_url

        result = _sanitize_url("http://admin:secret@druid:8090/path")
        assert "admin" not in result
        assert "secret" not in result
        assert "***:***@" in result
        assert "druid:8090" in result

    def test_sanitize_url_with_user_only(self):
        from tabsdata._io.cloud_staging.druid.config import _sanitize_url

        result = _sanitize_url("http://admin@druid:8090")
        assert "admin" not in result
        assert "***:***@" in result

    def test_safe_overlord_url_used_in_error(
        self, mock_requests_module, mock_cloud_storage
    ):
        """Verify that _safe_overlord_url strips credentials from paths."""
        config = DruidConfig(overlord_url="http://druid:8090")
        dialect = DruidDialect(config, mock_cloud_storage)
        safe = dialect._safe_overlord_url("/druid/indexer/v1/task")
        assert safe == "http://druid:8090/druid/indexer/v1/task"


# ── _get_task_error uses retry and richer extraction ──────────────────────────────


@pytest.mark.druid
class TestGetTaskErrorRetryAndDetails:
    @patch("tabsdata._io.cloud_staging.druid.dialect.time.sleep")
    def test_get_task_error_uses_retry(
        self, mock_sleep, dialect, session, load_mode_append
    ):
        """_get_task_error retries on transient failures."""
        # First poll returns FAILED status
        status_response = MagicMock()
        status_response.json.return_value = {
            "status": {"statusCode": "FAILED"}
        }
        # Error report: first attempt fails, second succeeds
        report_response = MagicMock()
        report_response.json.return_value = {
            "ingestionStatsAndErrors": {
                "payload": {"errorMsg": "Disk full"}
            }
        }
        session.get.side_effect = [
            status_response,  # poll status
            requests.ConnectionError("transient"),  # 1st error report attempt
            report_response,  # 2nd error report attempt (retry)
        ]

        result = dialect.poll_bulk_load(load_mode_append, "task-abc-123")
        assert result.status == BulkLoadStatus.FAILED
        assert "Disk full" in result.error

    def test_rich_error_with_error_msgs(
        self, dialect, session, load_mode_append
    ):
        """_get_task_error includes errorMsgs list in details."""
        status_response = MagicMock()
        status_response.json.return_value = {
            "status": {"statusCode": "FAILED"}
        }
        report_response = MagicMock()
        report_response.json.return_value = {
            "ingestionStatsAndErrors": {
                "payload": {
                    "errorMsg": "Encountered exception",
                    "errorMsgs": [
                        "Parse error in row 42",
                        "Invalid value for column 'age'",
                    ],
                }
            }
        }
        session.get.side_effect = [status_response, report_response]

        result = dialect.poll_bulk_load(load_mode_append, "task-abc-123")
        assert result.status == BulkLoadStatus.FAILED
        assert "Encountered exception" in result.error
        assert "Parse error in row 42" in result.error
        assert "Invalid value for column 'age'" in result.error

    def test_rich_error_with_row_stats(
        self, dialect, session, load_mode_append
    ):
        """_get_task_error includes rows processed count."""
        status_response = MagicMock()
        status_response.json.return_value = {
            "status": {"statusCode": "FAILED"}
        }
        report_response = MagicMock()
        report_response.json.return_value = {
            "ingestionStatsAndErrors": {
                "payload": {
                    "errorMsg": "Out of memory",
                    "rowStats": {
                        "totals": {"processed": 1234567}
                    },
                }
            }
        }
        session.get.side_effect = [status_response, report_response]

        result = dialect.poll_bulk_load(load_mode_append, "task-abc-123")
        assert "Out of memory" in result.error
        assert "rows processed before failure: 1234567" in result.error

    def test_get_task_error_total_failure_includes_exception(
        self, dialect, session, load_mode_append
    ):
        """When report fetch completely fails, error message includes the reason."""
        status_response = MagicMock()
        status_response.json.return_value = {
            "status": {"statusCode": "FAILED"}
        }
        # All retry attempts for error report fail
        session.get.side_effect = [
            status_response,
            requests.ConnectionError("host down"),
            requests.ConnectionError("host down"),
            requests.ConnectionError("host down"),
        ]

        result = dialect.poll_bulk_load(load_mode_append, "task-abc-123")
        assert result.status == BulkLoadStatus.FAILED
        assert "Unable to fetch error details" in result.error


# ── Timezone loss warnings ────────────────────────────────────────────────────────


@pytest.mark.druid
class TestTimezoneWarnings:
    def test_timestamp_with_tz_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = _pyarrow_type_to_druid(pa.timestamp("us", tz="UTC"))
        assert result == "LONG"
        assert "timezone" in caplog.text.lower()
        assert "UTC" in caplog.text

    def test_timestamp_with_nonstandard_tz_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = _pyarrow_type_to_druid(
                pa.timestamp("ns", tz="America/New_York")
            )
        assert result == "LONG"
        assert "America/New_York" in caplog.text

    def test_timestamp_without_tz_no_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = _pyarrow_type_to_druid(pa.timestamp("us"))
        assert result == "LONG"
        assert "timezone" not in caplog.text.lower()

    def test_date_no_tz_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = _pyarrow_type_to_druid(pa.date32())
        assert result == "LONG"
        assert "timezone" not in caplog.text.lower()


# ── Multi-file bulk load ──────────────────────────────────────────────────────────


@pytest.mark.druid
class TestMultiFileBulkLoad:
    def test_bulk_load_with_multiple_files(
        self, dialect, session, load_mode_append
    ):
        mock_response = MagicMock()
        mock_response.json.return_value = {"task": "task-multi-file"}
        session.post.return_value = mock_response

        files = [
            "s3://bucket/staging/druid/id/file1.parquet",
            "s3://bucket/staging/druid/id/file2.parquet",
            "s3://bucket/staging/druid/id/file3.parquet",
        ]
        result = dialect.bulk_load(load_mode_append, "events", files)
        assert result.status == BulkLoadStatus.PENDING
        assert result.job_handle == "task-multi-file"

        # Verify all 3 files were passed to the cloud_storage input source
        dialect._cloud_storage.druid_input_source.assert_called_once_with(files)

    def test_ingestion_spec_passes_all_files_to_input_source(self, dialect):
        files = [
            "s3://bucket/f1.parquet",
            "s3://bucket/f2.parquet",
            "s3://bucket/f3.parquet",
            "s3://bucket/f4.parquet",
        ]
        dialect._build_ingestion_spec("events", files, is_replace=False)
        dialect._cloud_storage.druid_input_source.assert_called_once_with(files)

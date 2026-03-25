#
# Copyright 2026 Tabs Data Inc.
#

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tabsdata import UserPasswordCredentials
from tabsdata._io.cloud_staging.common.bulk_loader import CloudStagedBulkLoader
from tabsdata._io.cloud_staging.common.errors import SchemaError
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
from tabsdata._io.cloud_staging.starrocks.cloud_storage import (
    AzureStarRocksStorage,
    GCSStarRocksStorage,
    S3StarRocksStorage,
    StarRocksCloudStorage,
    _common_directory,
)
from tabsdata._io.cloud_staging.starrocks.config import StarRocksConfig
from tabsdata._io.cloud_staging.starrocks.dialect import (
    StarRocksDialect,
    _arrow_type_to_starrocks,
    _normalize_column_type,
    _starrocks_type_to_arrow,
    schema_to_starrocks_columns,
)
from tabsdata._utils.id import encode_id
from test_tabsdata.conftest import TESTING_RESOURCES_FOLDER

import tabsdata as td


# ── Fixtures ──────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_pymysql():
    """Patch pymysql so the dialect constructor works without the package."""
    mock_module = MagicMock()
    mock_conn = MagicMock()
    mock_conn.open = True
    mock_module.connect.return_value = mock_conn
    with patch.dict(sys.modules, {"pymysql": mock_module}):
        yield mock_module, mock_conn


@pytest.fixture
def cloud_storage():
    return MagicMock(spec=StarRocksCloudStorage)


@pytest.fixture
def dialect(mock_pymysql, cloud_storage):
    mock_pymysql  # ensure the sys.modules patch is active during construction
    config = StarRocksConfig(host="localhost", port=9030, database="testdb")
    return StarRocksDialect(config=config, cloud_storage=cloud_storage)


@pytest.fixture
def cursor(dialect):
    """Returns the mock pymysql cursor used inside the dialect."""
    conn = dialect._get_connection()
    return conn.cursor().__enter__()


@pytest.fixture
def load_mode():
    return LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id="test-upload-001",
    )


# ── Constructor ───────────────────────────────────────────────────────────────────────


@pytest.mark.starrocks
def test_constructor_defaults_to_root_user(mock_pymysql, cloud_storage):
    config = StarRocksConfig(host="myhost", port=9030, database="mydb")
    d = StarRocksDialect(config=config, cloud_storage=cloud_storage)
    conn = d._get_connection()
    mock_module, _ = mock_pymysql
    mock_module.connect.assert_called_once_with(
        host="myhost",
        port=9030,
        database="mydb",
        user="root",
        password="",
        autocommit=True,
    )


@pytest.mark.starrocks
def test_constructor_uses_credentials_when_provided(mock_pymysql, cloud_storage):
    creds = UserPasswordCredentials("alice", "s3cr3t")
    config = StarRocksConfig(
        host="localhost", port=9030, database="testdb", credentials=creds
    )
    d = StarRocksDialect(config=config, cloud_storage=cloud_storage)
    conn = d._get_connection()
    mock_module, _ = mock_pymysql
    mock_module.connect.assert_called_once_with(
        host="localhost",
        port=9030,
        database="testdb",
        user="alice",
        password="s3cr3t",
        autocommit=True,
    )


@pytest.mark.starrocks
def test_constructor_raises_if_pymysql_missing(cloud_storage):
    config = StarRocksConfig(host="localhost")
    with patch.dict(sys.modules, {"pymysql": None}):
        with pytest.raises(ImportError, match="pymysql"):
            StarRocksDialect(config=config, cloud_storage=cloud_storage)


# ── Capability flags ──────────────────────────────────────────────────────────────────


@pytest.mark.starrocks
def test_supports_tabsdata_schema_evolution(dialect):
    assert dialect.supports_tabsdata_schema_evolution() is True


@pytest.mark.starrocks
def test_does_not_support_native_schema_evolution(dialect):
    assert dialect.supports_native_schema_evolution() is False


@pytest.mark.starrocks
def test_does_not_support_transactions(dialect):
    assert dialect.supports_transactions() is False


@pytest.mark.starrocks
def test_does_not_support_auto_cleanup(dialect):
    assert dialect.supports_auto_cleanup() is False


@pytest.mark.starrocks
def test_bulk_load_is_not_async(dialect):
    assert dialect.is_bulk_load_async() is False


# ── _arrow_type_to_starrocks ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "arrow_type,expected",
    [
        (pa.bool_(), "BOOLEAN"),
        (pa.int8(), "TINYINT"),
        (pa.int16(), "SMALLINT"),
        (pa.int32(), "INT"),
        (pa.int64(), "BIGINT"),
        # Unsigned types are widened to next signed type
        (pa.uint8(), "SMALLINT"),
        (pa.uint16(), "INT"),
        (pa.uint32(), "BIGINT"),
        (pa.uint64(), "LARGEINT"),
        (pa.float16(), "FLOAT"),
        (pa.float32(), "FLOAT"),
        (pa.float64(), "DOUBLE"),
        (pa.string(), "STRING"),
        (pa.utf8(), "STRING"),
        (pa.large_string(), "STRING"),
        (pa.large_utf8(), "STRING"),
        (pa.binary(), "VARBINARY"),
        (pa.date32(), "DATE"),
        # All timestamps → DATETIME (second precision)
        (pa.timestamp("s"), "DATETIME"),
        (pa.timestamp("ms"), "DATETIME"),
        (pa.timestamp("us"), "DATETIME"),
        (pa.timestamp("ns"), "DATETIME"),
        (pa.timestamp("us", tz="UTC"), "DATETIME"),
        (pa.timestamp("ns", tz="America/New_York"), "DATETIME"),
        # Decimal
        (pa.decimal128(10, 3), "DECIMAL(10, 3)"),
        (pa.decimal128(38, 10), "DECIMAL(38, 10)"),
        # Lists
        (pa.list_(pa.int32()), "ARRAY<INT>"),
        (pa.list_(pa.string()), "ARRAY<STRING>"),
        (pa.large_list(pa.float64()), "ARRAY<DOUBLE>"),
    ],
)
@pytest.mark.starrocks
def test_arrow_type_to_starrocks(arrow_type, expected):
    assert _arrow_type_to_starrocks(arrow_type) == expected


@pytest.mark.starrocks
def test_arrow_type_to_starrocks_nested_list():
    assert (
        _arrow_type_to_starrocks(pa.list_(pa.list_(pa.string())))
        == "ARRAY<ARRAY<STRING>>"
    )


@pytest.mark.starrocks
def test_arrow_type_to_starrocks_map():
    assert (
        _arrow_type_to_starrocks(pa.map_(pa.string(), pa.int64()))
        == "MAP<STRING, BIGINT>"
    )


@pytest.mark.starrocks
def test_arrow_type_to_starrocks_map_with_nested_value():
    result = _arrow_type_to_starrocks(pa.map_(pa.string(), pa.list_(pa.int32())))
    assert result == "MAP<STRING, ARRAY<INT>>"


@pytest.mark.starrocks
def test_arrow_type_to_starrocks_struct():
    arrow_type = pa.struct(
        [pa.field("x", pa.int32()), pa.field("y", pa.float64())]
    )
    assert _arrow_type_to_starrocks(arrow_type) == "STRUCT<x INT, y DOUBLE>"


@pytest.mark.starrocks
def test_arrow_type_to_starrocks_unsupported_raises():
    with pytest.raises(TypeError, match="Unsupported PyArrow type for StarRocks"):
        _arrow_type_to_starrocks(pa.time32("s"))


# ── schema_to_starrocks_columns ──────────────────────────────────────────────────────


@pytest.mark.starrocks
def test_schema_to_starrocks_columns_nullable():
    schema = pa.schema([pa.field("id", pa.int64(), nullable=True)])
    assert schema_to_starrocks_columns(schema) == [("id", "BIGINT", True)]


@pytest.mark.starrocks
def test_schema_to_starrocks_columns_non_nullable():
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    assert schema_to_starrocks_columns(schema) == [("id", "BIGINT", False)]


@pytest.mark.starrocks
def test_schema_to_starrocks_columns_container_types():
    schema = pa.schema(
        [
            pa.field("arr", pa.list_(pa.int32()), nullable=True),
            pa.field("m", pa.map_(pa.string(), pa.int64()), nullable=True),
            pa.field(
                "s", pa.struct([pa.field("a", pa.int8())]), nullable=True
            ),
        ]
    )
    cols = schema_to_starrocks_columns(schema)
    assert cols == [
        ("arr", "ARRAY<INT>", True),
        ("m", "MAP<STRING, BIGINT>", True),
        ("s", "STRUCT<a TINYINT>", True),
    ]


@pytest.mark.starrocks
def test_schema_to_starrocks_columns_mixed():
    schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("tags", pa.list_(pa.string()), nullable=True),
            pa.field("score", pa.float64(), nullable=True),
        ]
    )
    cols = schema_to_starrocks_columns(schema)
    assert cols == [
        ("id", "INT", False),
        ("name", "STRING", True),
        ("tags", "ARRAY<STRING>", True),
        ("score", "DOUBLE", True),
    ]


# ── _starrocks_type_to_arrow ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "sr_type,expected",
    [
        # Simple types
        ("BOOLEAN", pa.bool_()),
        ("TINYINT", pa.int8()),
        ("SMALLINT", pa.int16()),
        ("INT", pa.int32()),
        ("BIGINT", pa.int64()),
        ("LARGEINT", pa.int64()),
        ("FLOAT", pa.float32()),
        ("DOUBLE", pa.float64()),
        ("STRING", pa.string()),
        ("DATE", pa.date32()),
        ("DATETIME", pa.timestamp("s")),
        ("VARBINARY", pa.binary()),
        # Parameterised types
        ("VARCHAR(255)", pa.string()),
        ("VARCHAR(1048576)", pa.string()),
        ("CHAR(10)", pa.string()),
        # Decimal
        ("DECIMAL(10, 3)", pa.decimal128(10, 3)),
        ("DECIMAL(38, 10)", pa.decimal128(38, 10)),
        # Array
        ("ARRAY<INT>", pa.list_(pa.int32())),
        ("ARRAY<STRING>", pa.list_(pa.string())),
        ("ARRAY<DOUBLE>", pa.list_(pa.float64())),
        # Map
        ("MAP<STRING, BIGINT>", pa.map_(pa.string(), pa.int64())),
        ("MAP<STRING, DOUBLE>", pa.map_(pa.string(), pa.float64())),
    ],
)
@pytest.mark.starrocks
def test_starrocks_type_to_arrow(sr_type, expected):
    assert _starrocks_type_to_arrow(sr_type) == expected


@pytest.mark.starrocks
def test_starrocks_type_to_arrow_whitespace_trimmed():
    assert _starrocks_type_to_arrow("  INT  ") == pa.int32()


@pytest.mark.starrocks
def test_starrocks_type_to_arrow_nested_array():
    assert _starrocks_type_to_arrow("ARRAY<ARRAY<INT>>") == pa.list_(
        pa.list_(pa.int32())
    )


@pytest.mark.starrocks
def test_starrocks_type_to_arrow_map_nested_value():
    result = _starrocks_type_to_arrow("MAP<STRING, ARRAY<INT>>")
    assert result == pa.map_(pa.string(), pa.list_(pa.int32()))


@pytest.mark.starrocks
def test_starrocks_type_to_arrow_struct():
    result = _starrocks_type_to_arrow("STRUCT<x INT, y DOUBLE>")
    expected = pa.struct([pa.field("x", pa.int32()), pa.field("y", pa.float64())])
    assert result == expected


@pytest.mark.starrocks
def test_starrocks_type_to_arrow_struct_nested():
    result = _starrocks_type_to_arrow("STRUCT<a INT, b ARRAY<STRING>>")
    expected = pa.struct(
        [pa.field("a", pa.int32()), pa.field("b", pa.list_(pa.string()))]
    )
    assert result == expected


@pytest.mark.starrocks
def test_starrocks_type_to_arrow_unsupported_raises():
    with pytest.raises(TypeError, match="Unsupported StarRocks type"):
        _starrocks_type_to_arrow("BITMAP")


# ── _common_directory ────────────────────────────────────────────────────────────────


@pytest.mark.starrocks
def test_common_directory_single_path():
    assert (
        _common_directory(["s3://bucket/staging/abc123/file.parquet"])
        == "s3://bucket/staging/abc123"
    )


@pytest.mark.starrocks
def test_common_directory_multiple_same_dir():
    paths = [
        "s3://bucket/staging/abc123/file1.parquet",
        "s3://bucket/staging/abc123/file2.parquet",
    ]
    assert _common_directory(paths) == "s3://bucket/staging/abc123"


@pytest.mark.starrocks
def test_common_directory_different_dirs():
    paths = [
        "s3://bucket/staging/abc/file1.parquet",
        "s3://bucket/staging/def/file2.parquet",
    ]
    assert _common_directory(paths) == "s3://bucket/staging"


# ── _normalize_column_type ─────────────────────────────────────────────────────────


@pytest.mark.starrocks
@pytest.mark.parametrize(
    "raw, expected",
    [
        ("tinyint(1)", "BOOLEAN"),
        ("tinyint(4)", "TINYINT"),
        ("smallint(6)", "SMALLINT"),
        ("int(11)", "INT"),
        ("bigint(20)", "BIGINT"),
        ("largeint(40)", "LARGEINT"),
        ("float", "FLOAT"),
        ("double", "DOUBLE"),
        ("varchar(65533)", "STRING"),
        ("varchar(255)", "VARCHAR(255)"),
        ("char(10)", "CHAR(10)"),
        ("date", "DATE"),
        ("datetime", "DATETIME"),
        ("boolean", "BOOLEAN"),
        ("varbinary", "VARBINARY"),
        ("decimal(10, 3)", "DECIMAL(10, 3)"),
        ("array<int(11)>", "ARRAY<INT>"),
        ("array<varchar(65533)>", "ARRAY<STRING>"),
        ("map<varchar(65533),int(11)>", "MAP<STRING, INT>"),
        # Already-canonical types pass through unchanged
        ("INT", "INT"),
        ("BIGINT", "BIGINT"),
        ("STRING", "STRING"),
        ("BOOLEAN", "BOOLEAN"),
        ("ARRAY<INT>", "ARRAY<INT>"),
        ("MAP<STRING, INT>", "MAP<STRING, INT>"),
        ("DECIMAL(10, 3)", "DECIMAL(10, 3)"),
    ],
    ids=lambda v: v,
)
def test_normalize_column_type(raw, expected):
    assert _normalize_column_type(raw) == expected


# ── table_exists ──────────────────────────────────────────────────────────────────────


@pytest.mark.starrocks
def test_table_exists_returns_true_when_count_nonzero(dialect, cursor, load_mode):
    cursor.fetchall.return_value = [(1,)]
    assert dialect.table_exists(load_mode, "my_table") is True


@pytest.mark.starrocks
def test_table_exists_returns_false_when_count_zero(dialect, cursor, load_mode):
    cursor.fetchall.return_value = [(0,)]
    assert dialect.table_exists(load_mode, "my_table") is False


@pytest.mark.starrocks
def test_table_exists_passes_correct_parameters(dialect, cursor, load_mode):
    cursor.fetchall.return_value = [(1,)]
    dialect.table_exists(load_mode, "my_table")
    cursor.execute.assert_called_with(
        "SELECT COUNT(*) FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        ("testdb", "my_table"),
    )


# ── get_table_schema ──────────────────────────────────────────────────────────────────


@pytest.mark.starrocks
def test_get_table_schema_returns_correct_schema(dialect, cursor, load_mode):
    cursor.fetchall.return_value = [
        ("id", "BIGINT", "NO"),
        ("name", "STRING", "YES"),
        ("score", "DOUBLE", "NO"),
    ]
    result = dialect.get_table_schema(load_mode, "my_table")
    assert result == pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("score", pa.float64(), nullable=False),
        ]
    )


@pytest.mark.starrocks
def test_get_table_schema_passes_correct_parameters(dialect, cursor, load_mode):
    cursor.fetchall.return_value = []
    dialect.get_table_schema(load_mode, "my_table")
    cursor.execute.assert_called_with(
        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
        "ORDER BY ORDINAL_POSITION",
        ("testdb", "my_table"),
    )


@pytest.mark.starrocks
def test_get_table_schema_empty_table_returns_empty_schema(dialect, cursor, load_mode):
    cursor.fetchall.return_value = []
    assert dialect.get_table_schema(load_mode, "empty_table") == pa.schema([])


# ── create_table ──────────────────────────────────────────────────────────────────────


@pytest.mark.starrocks
def test_create_table_generates_correct_ddl(dialect, cursor, load_mode):
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    dialect.create_table(load_mode, "my_table", arrow_schema)
    ddl = cursor.execute.call_args[0][0]
    assert "CREATE TABLE `testdb`.`my_table`" in ddl
    assert "`id` BIGINT NOT NULL" in ddl
    assert "`name` STRING NULL" in ddl
    assert "DUPLICATE KEY(`id`)" in ddl
    assert "DISTRIBUTED BY HASH(`id`)" in ddl
    assert '"fast_schema_evolution" = "true"' in ddl


@pytest.mark.starrocks
def test_create_table_nullable_columns(dialect, cursor, load_mode):
    arrow_schema = pa.schema(
        [
            pa.field("pk", pa.int32(), nullable=False),
            pa.field("val", pa.float64(), nullable=True),
            pa.field("tags", pa.list_(pa.string()), nullable=True),
        ]
    )
    dialect.create_table(load_mode, "events", arrow_schema)
    ddl = cursor.execute.call_args[0][0]
    assert "`pk` INT NOT NULL" in ddl
    assert "`val` DOUBLE NULL" in ddl
    assert "`tags` ARRAY<STRING> NULL" in ddl


@pytest.mark.starrocks
def test_create_table_timestamp_column(dialect, cursor, load_mode):
    arrow_schema = pa.schema(
        [pa.field("ts", pa.timestamp("us", tz="UTC"), nullable=True)]
    )
    dialect.create_table(load_mode, "events", arrow_schema)
    ddl = cursor.execute.call_args[0][0]
    assert "`ts` DATETIME NULL" in ddl


@pytest.mark.starrocks
def test_create_table_decimal_column(dialect, cursor, load_mode):
    arrow_schema = pa.schema(
        [pa.field("amount", pa.decimal128(18, 4), nullable=False)]
    )
    dialect.create_table(load_mode, "payments", arrow_schema)
    ddl = cursor.execute.call_args[0][0]
    assert "`amount` DECIMAL(18, 4) NOT NULL" in ddl


# ── verify_schema ─────────────────────────────────────────────────────────────────────


@pytest.mark.starrocks
def test_verify_schema_returns_true_when_schemas_match(dialect, cursor, load_mode):
    cursor.fetchall.return_value = [
        ("id", "BIGINT", "NO"),
        ("name", "STRING", "YES"),
    ]
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    assert dialect.verify_schema(load_mode, "my_table", arrow_schema) is True


@pytest.mark.starrocks
def test_verify_schema_raises_for_extra_columns(dialect, cursor, load_mode):
    cursor.fetchall.return_value = [
        ("id", "BIGINT", "NO"),
        ("name", "STRING", "YES"),
        ("extra", "DOUBLE", "NO"),
    ]
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    with pytest.raises(SchemaError):
        dialect.verify_schema(load_mode, "my_table", arrow_schema)


@pytest.mark.starrocks
def test_verify_schema_raises_for_missing_column(dialect, cursor, load_mode):
    cursor.fetchall.return_value = [("id", "BIGINT", "NO")]
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    with pytest.raises(SchemaError):
        dialect.verify_schema(load_mode, "my_table", arrow_schema)


@pytest.mark.starrocks
def test_verify_schema_raises_for_type_mismatch(dialect, cursor, load_mode):
    cursor.fetchall.return_value = [("id", "INT", "NO")]
    arrow_schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    with pytest.raises(SchemaError):
        dialect.verify_schema(load_mode, "my_table", arrow_schema)


@pytest.mark.starrocks
def test_verify_schema_error_message_includes_table_info(dialect, cursor, load_mode):
    cursor.fetchall.return_value = [("id", "INT", "NO")]
    arrow_schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    with pytest.raises(SchemaError, match="my_table"):
        dialect.verify_schema(load_mode, "my_table", arrow_schema)


# ── truncate_table ────────────────────────────────────────────────────────────────────


@pytest.mark.starrocks
def test_truncate_table_issues_correct_command(dialect, cursor, load_mode):
    dialect.truncate_table(load_mode, "my_table")
    cursor.execute.assert_called_with(
        "TRUNCATE TABLE `testdb`.`my_table`", None
    )


@pytest.mark.starrocks
def test_truncate_table_uses_qualified_name(mock_pymysql, cloud_storage, load_mode):
    config = StarRocksConfig(host="localhost", database="prod")
    d = StarRocksDialect(config=config, cloud_storage=cloud_storage)
    d.truncate_table(load_mode, "orders")
    conn = d._get_connection()
    conn.cursor().__enter__().execute.assert_called_with(
        "TRUNCATE TABLE `prod`.`orders`", None
    )


# ── bulk_load ─────────────────────────────────────────────────────────────────────────


@pytest.mark.starrocks
def test_bulk_load_returns_completed_status(
    dialect, cursor, cloud_storage, load_mode
):
    cloud_storage.starrocks_files_function.return_value = (
        'FILES("path" = "s3://bucket/staging/*", "format" = "parquet")'
    )
    result = dialect.bulk_load(load_mode, "my_table", ["ref1"])
    assert result.status == BulkLoadStatus.COMPLETED


@pytest.mark.starrocks
def test_bulk_load_calls_files_function_once_for_all_refs(
    dialect, cursor, cloud_storage, load_mode
):
    cloud_storage.starrocks_files_function.return_value = (
        'FILES("path" = "s3://bucket/staging/*", "format" = "parquet")'
    )
    refs = ["ref1", "ref2", "ref3"]
    dialect.bulk_load(load_mode, "my_table", refs)
    # StarRocks batches all refs into one FILES() call (unlike ClickHouse)
    cloud_storage.starrocks_files_function.assert_called_once_with(refs)


@pytest.mark.starrocks
def test_bulk_load_issues_insert_select_with_files(
    dialect, cursor, cloud_storage, load_mode
):
    files_fn = 'FILES("path" = "s3://bucket/staging/*", "format" = "parquet")'
    cloud_storage.starrocks_files_function.return_value = files_fn
    dialect.bulk_load(load_mode, "my_table", ["ref1", "ref2"])
    expected_sql = f"INSERT INTO `testdb`.`my_table` SELECT * FROM {files_fn}"
    cursor.execute.assert_called_with(expected_sql, None)


# ── get_type_widening_map ─────────────────────────────────────────────────────────────


@pytest.mark.starrocks
def test_get_type_widening_map_integer_widening(dialect):
    widening = dialect.get_type_widening_map()
    assert widening["SMALLINT"] == {"TINYINT"}
    assert widening["INT"] == {"TINYINT", "SMALLINT"}
    assert widening["BIGINT"] == {"TINYINT", "SMALLINT", "INT"}
    assert widening["LARGEINT"] == {"TINYINT", "SMALLINT", "INT", "BIGINT"}


@pytest.mark.starrocks
def test_get_type_widening_map_float_widening(dialect):
    widening = dialect.get_type_widening_map()
    assert widening["DOUBLE"] == {"FLOAT"}


@pytest.mark.starrocks
def test_get_type_widening_map_does_not_include_string_or_date(dialect):
    widening = dialect.get_type_widening_map()
    assert "STRING" not in widening
    assert "DATE" not in widening
    assert "BOOLEAN" not in widening


# ── StarRocksConfig ──────────────────────────────────────────────────────────────────


@pytest.mark.starrocks
def test_starrocks_config_defaults():
    config = StarRocksConfig(host="myhost")
    assert config.host == "myhost"
    assert config.port == 9030
    assert config.database == "default"
    assert config.credentials is None


@pytest.mark.starrocks
def test_starrocks_config_with_credentials():
    creds = UserPasswordCredentials("user", "pass")
    config = StarRocksConfig(
        host="myhost", port=9031, database="mydb", credentials=creds
    )
    assert config.port == 9031
    assert config.database == "mydb"
    assert config.credentials is creds


# ── Cloud Storage: S3StarRocksStorage ────────────────────────────────────────────────


@pytest.mark.starrocks
def test_s3_storage_files_function_single_ref():
    s3_config = S3Config(
        bucket="my-bucket",
        region="us-east-1",
        base_path="staging/starrocks",
        credentials=td.S3AccessKeyCredentials("AKID", "SECRET"),
    )
    storage = S3StarRocksStorage(s3_config)
    ref = "https://my-bucket.s3.us-east-1.amazonaws.com/staging/starrocks/upload1/f.parquet"
    result = storage.starrocks_files_function([ref])
    assert '"path" = "s3://my-bucket/staging/starrocks/upload1/f.parquet"' in result
    assert '"format" = "parquet"' in result
    assert '"aws.s3.access_key" = "AKID"' in result
    assert '"aws.s3.secret_key" = "SECRET"' in result
    assert '"aws.s3.region" = "us-east-1"' in result


@pytest.mark.starrocks
def test_s3_storage_files_function_multiple_refs_uses_wildcard():
    s3_config = S3Config(
        bucket="my-bucket",
        region="us-east-1",
        base_path="staging/starrocks",
        credentials=td.S3AccessKeyCredentials("AKID", "SECRET"),
    )
    storage = S3StarRocksStorage(s3_config)
    refs = [
        "https://my-bucket.s3.us-east-1.amazonaws.com/staging/starrocks/upload1/f1.parquet",
        "https://my-bucket.s3.us-east-1.amazonaws.com/staging/starrocks/upload1/f2.parquet",
    ]
    result = storage.starrocks_files_function(refs)
    # Multiple files in same upload_id dir → wildcard
    assert '"path" = "s3://my-bucket/staging/starrocks/upload1/*"' in result


# ── Cloud Storage: AzureStarRocksStorage ─────────────────────────────────────────────


@pytest.mark.starrocks
def test_azure_storage_files_function_single_ref():
    from tabsdata._io.cloud_staging.common.storage_config import AzureConfig

    azure_config = AzureConfig(
        container="mycontainer",
        base_path="staging/starrocks",
        credentials=td.AzureAccountKeyCredentials("myaccount", "mykey"),
    )
    storage = AzureStarRocksStorage(azure_config)
    ref = "staging/starrocks/upload1/f.parquet"
    result = storage.starrocks_files_function([ref])
    assert (
        '"path" = "wasbs://mycontainer@myaccount.blob.core.windows.net/'
        'staging/starrocks/upload1/f.parquet"' in result
    )
    assert '"format" = "parquet"' in result
    assert '"azure.blob.shared_key" = "mykey"' in result


@pytest.mark.starrocks
def test_azure_storage_files_function_multiple_refs_uses_wildcard():
    from tabsdata._io.cloud_staging.common.storage_config import AzureConfig

    azure_config = AzureConfig(
        container="mycontainer",
        base_path="staging/starrocks",
        credentials=td.AzureAccountKeyCredentials("myaccount", "mykey"),
    )
    storage = AzureStarRocksStorage(azure_config)
    refs = [
        "staging/starrocks/upload1/f1.parquet",
        "staging/starrocks/upload1/f2.parquet",
    ]
    result = storage.starrocks_files_function(refs)
    assert (
        '"path" = "wasbs://mycontainer@myaccount.blob.core.windows.net/'
        'staging/starrocks/upload1/*"' in result
    )


# ── Cloud Storage: GCSStarRocksStorage ───────────────────────────────────────────────


@pytest.mark.starrocks
def test_gcs_storage_files_function_single_ref():
    from tabsdata._io.cloud_staging.common.storage_config import GCSConfig

    gcs_config = GCSConfig(
        bucket="my-gcs-bucket",
        base_path="staging/starrocks",
        hmac_credentials=td.HMACCredentials("gcs_access_key", "gcs_secret_key"),
    )
    storage = GCSStarRocksStorage(gcs_config)
    ref = "https://storage.googleapis.com/my-gcs-bucket/staging/starrocks/upload1/f.parquet"
    result = storage.starrocks_files_function([ref])
    assert (
        '"path" = "s3a://my-gcs-bucket/staging/starrocks/upload1/f.parquet"' in result
    )
    assert '"format" = "parquet"' in result
    assert '"aws.s3.access_key" = "gcs_access_key"' in result
    assert '"aws.s3.secret_key" = "gcs_secret_key"' in result
    assert '"aws.s3.endpoint" = "https://storage.googleapis.com"' in result


# ── Integration test helpers ──────────────────────────────────────────────────────────


def _assert_starrocks_data_matches(
    starrocks_client, database, table, input_file, expected_copies=1
):
    """Verify ALL columns in the StarRocks table match *expected_copies* of
    the source parquet, including complex types (ARRAY, MAP)."""
    import json
    from decimal import Decimal

    import polars as pl

    base_df = pl.read_parquet(input_file).sort("col_int32")
    if expected_copies > 1:
        expected_df = pl.concat([base_df] * expected_copies).sort("col_int32")
    else:
        expected_df = base_df

    _schema = pq.read_schema(input_file)
    all_cols = [f.name for f in _schema]

    with starrocks_client.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(f'`{c}`' for c in all_cols)} "
            f"FROM `{database}`.`{table}` "
            f"ORDER BY col_int32"
        )
        rows = cur.fetchall()

    assert len(rows) == len(expected_df), (
        f"Row count mismatch: expected {len(expected_df)}, got {len(rows)}"
    )

    for row_idx, row in enumerate(rows):
        for col_idx, field in enumerate(_schema):
            actual = row[col_idx]
            expected = expected_df[field.name][row_idx]
            arrow_type = field.type
            col = field.name
            msg = f"Row {row_idx}, col '{col}'"

            # ── Nulls ────────────────────────────────────────────────────
            if expected is None:
                assert actual is None, f"{msg}: expected None, got {actual!r}"
                continue
            if actual is None:
                assert False, f"{msg}: expected {expected!r}, got None"

            # ── Scalar types ─────────────────────────────────────────────
            if pa.types.is_boolean(arrow_type):
                assert bool(actual) == bool(expected), f"{msg}: {actual!r} != {expected!r}"
            elif pa.types.is_floating(arrow_type):
                assert abs(float(actual) - float(expected)) < 1e-5, (
                    f"{msg}: {actual!r} != {expected!r}"
                )
            elif pa.types.is_decimal(arrow_type):
                assert Decimal(str(actual)) == Decimal(str(expected)), (
                    f"{msg}: {actual!r} != {expected!r}"
                )
            elif pa.types.is_date(arrow_type):
                actual_date = actual.date() if hasattr(actual, "date") else actual
                assert actual_date == expected, f"{msg}: {actual_date!r} != {expected!r}"
            elif pa.types.is_timestamp(arrow_type):
                # StarRocks DATETIME has second precision
                expected_ts = expected.replace(microsecond=0) if hasattr(expected, "replace") else expected
                assert actual == expected_ts, f"{msg}: {actual!r} != {expected_ts!r}"

            # ── Complex types ────────────────────────────────────────────
            elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
                actual_list = json.loads(actual) if isinstance(actual, str) else list(actual)
                expected_list = expected.to_list() if hasattr(expected, "to_list") else list(expected)
                assert actual_list == expected_list, (
                    f"{msg}: {actual_list!r} != {expected_list!r}"
                )
            elif pa.types.is_map(arrow_type):
                actual_map = json.loads(actual) if isinstance(actual, str) else actual
                if isinstance(actual_map, list):
                    actual_map = {item[0]: item[1] for item in actual_map}
                # Polars returns map as Series of structs → list of dicts
                expected_items = expected.to_list() if hasattr(expected, "to_list") else list(expected)
                expected_map = {item["key"]: item["value"] for item in expected_items}
                assert actual_map == expected_map, (
                    f"{msg}: {actual_map!r} != {expected_map!r}"
                )

            # ── Integer / String / fallback ──────────────────────────────
            else:
                assert actual == expected, f"{msg}: {actual!r} != {expected!r}"


# ── Schema evolution integration tests ────────────────────────────────────────────────


def _make_s3_sr_dialect(starrocks_config, s3_config, run_id):
    """Build a StarRocksDialect backed by S3 cloud storage."""
    from tabsdata._io.cloud_staging.common.storage_config import S3Config
    from tabsdata._io.cloud_staging.starrocks.cloud_storage import S3StarRocksStorage
    from tabsdata._io.cloud_staging.starrocks.config import StarRocksConfig

    bucket = s3_config["BUCKET_NAME"]
    region = s3_config["REGION"]
    base_path = f"staging/starrocks/schema_evo_{run_id}"
    s3_config_obj = S3Config(
        bucket=bucket,
        region=region,
        base_path=base_path,
        credentials=td.S3AccessKeyCredentials(
            s3_config["ACCESS_KEY"], s3_config["SECRET_KEY"]
        ),
    )
    cloud_storage = S3StarRocksStorage(s3_config_obj)
    sr_config = StarRocksConfig(
        host=starrocks_config.host,
        port=starrocks_config.port,
        database=starrocks_config.database,
        credentials=UserPasswordCredentials(
            starrocks_config.user, starrocks_config.password
        ),
    )
    return StarRocksDialect(config=sr_config, cloud_storage=cloud_storage)


def _sr_create_table(starrocks_client, db, table, col_defs_sql):
    with starrocks_client.cursor() as cur:
        cur.execute(
            f"CREATE TABLE `{db}`.`{table}` ({col_defs_sql}) "
            f"DUPLICATE KEY(`{col_defs_sql.split('`')[1]}`) "
            f"DISTRIBUTED BY HASH(`{col_defs_sql.split('`')[1]}`) "
            f'PROPERTIES ("fast_schema_evolution" = "true")'
        )


def _sr_insert(starrocks_client, db, table, values_sql):
    with starrocks_client.cursor() as cur:
        cur.execute(f"INSERT INTO `{db}`.`{table}` VALUES {values_sql}")


def _sr_get_columns(starrocks_client, db, table):
    with starrocks_client.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE "
            "FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
            "ORDER BY ORDINAL_POSITION",
            (db, table),
        )
        return cur.fetchall()


def _sr_row_count(starrocks_client, db, table):
    with starrocks_client.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM `{db}`.`{table}`")
        return cur.fetchone()[0]


@pytest.mark.starrocks
@pytest.mark.s3
def test_schema_evolution_add_nullable_column(
    starrocks_config, starrocks_client, s3_config
):
    """Add a nullable column to an existing table."""
    from tabsdata.connector.schema.evolution.core import compute_delta
    from tabsdata.connector.schema.evolution.models import ColumnDef, TableSchema
    from tabsdata.connector.schema.evolution.renderers.starrocks import (
        STARROCKS_TYPE_WIDENING_MAP,
    )

    _, run_id = encode_id()
    table = f"test_sr_evo_add_col_{run_id}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table,
        "`id` BIGINT NOT NULL, `name` STRING NOT NULL",
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice'), (2, 'bob')")

    dialect = _make_s3_sr_dialect(starrocks_config, s3_config, run_id)
    current = dialect.get_evolution_table_schema(table)
    desired = TableSchema(
        table_name=current.table_name,
        columns=[
            ColumnDef(name="id", type="BIGINT", nullable=False),
            ColumnDef(name="name", type="STRING", nullable=False),
            ColumnDef(name="email", type="STRING", nullable=True),
        ],
    )
    delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )
    dialect.apply_schema_delta(load_mode, table, delta)

    cols = _sr_get_columns(starrocks_client, db, table)
    col_names = [c[0] for c in cols]
    assert "email" in col_names
    email_nullable = next(c[2] for c in cols if c[0] == "email")
    assert email_nullable == "YES"

    assert _sr_row_count(starrocks_client, db, table) == 2


@pytest.mark.starrocks
@pytest.mark.s3
def test_schema_evolution_drop_column(
    starrocks_config, starrocks_client, s3_config
):
    """Drop a column from an existing table."""
    from tabsdata.connector.schema.evolution.core import compute_delta
    from tabsdata.connector.schema.evolution.models import ColumnDef, TableSchema
    from tabsdata.connector.schema.evolution.renderers.starrocks import (
        STARROCKS_TYPE_WIDENING_MAP,
    )

    _, run_id = encode_id()
    table = f"test_sr_evo_drop_col_{run_id}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table,
        "`id` BIGINT NOT NULL, `name` STRING NOT NULL, `old_col` STRING NULL",
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice', 'legacy')")

    dialect = _make_s3_sr_dialect(starrocks_config, s3_config, run_id)
    current = dialect.get_evolution_table_schema(table)
    desired = TableSchema(
        table_name=current.table_name,
        columns=[
            ColumnDef(name="id", type="BIGINT", nullable=False),
            ColumnDef(name="name", type="STRING", nullable=False),
        ],
    )
    delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )
    dialect.apply_schema_delta(load_mode, table, delta)

    cols = _sr_get_columns(starrocks_client, db, table)
    col_names = [c[0] for c in cols]
    assert "old_col" not in col_names
    assert _sr_row_count(starrocks_client, db, table) == 1


@pytest.mark.starrocks
@pytest.mark.s3
def test_schema_evolution_widen_type(
    starrocks_config, starrocks_client, s3_config
):
    """Widen INT -> BIGINT and FLOAT -> DOUBLE."""
    from tabsdata.connector.schema.evolution.core import compute_delta, validate_delta
    from tabsdata.connector.schema.evolution.models import ColumnDef, TableSchema
    from tabsdata.connector.schema.evolution.renderers.starrocks import (
        STARROCKS_TYPE_WIDENING_MAP,
    )

    _, run_id = encode_id()
    table = f"test_sr_evo_widen_{run_id}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table,
        "`key_col` BIGINT NOT NULL, `id` INT NOT NULL, `value` FLOAT NOT NULL",
    )
    _sr_insert(starrocks_client, db, table, "(1, 42, 3.14)")

    dialect = _make_s3_sr_dialect(starrocks_config, s3_config, run_id)
    current = dialect.get_evolution_table_schema(table)
    desired = TableSchema(
        table_name=current.table_name,
        columns=[
            ColumnDef(name="key_col", type="BIGINT", nullable=False),
            ColumnDef(name="id", type="BIGINT", nullable=False),
            ColumnDef(name="value", type="DOUBLE", nullable=False),
        ],
    )
    delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
    assert validate_delta(delta, strict=True) is True

    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )
    dialect.apply_schema_delta(load_mode, table, delta)

    cols = _sr_get_columns(starrocks_client, db, table)
    from tabsdata._io.cloud_staging.starrocks.dialect import _normalize_column_type

    id_type = _normalize_column_type(next(c[1] for c in cols if c[0] == "id"))
    value_type = _normalize_column_type(next(c[1] for c in cols if c[0] == "value"))
    assert id_type == "BIGINT"
    assert value_type == "DOUBLE"


@pytest.mark.starrocks
@pytest.mark.s3
def test_schema_evolution_set_nullable(
    starrocks_config, starrocks_client, s3_config
):
    """Make a NOT NULL column nullable."""
    from tabsdata.connector.schema.evolution.core import compute_delta
    from tabsdata.connector.schema.evolution.models import ColumnDef, TableSchema
    from tabsdata.connector.schema.evolution.renderers.starrocks import (
        STARROCKS_TYPE_WIDENING_MAP,
    )

    _, run_id = encode_id()
    table = f"test_sr_evo_set_nullable_{run_id}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table,
        "`id` BIGINT NOT NULL, `name` STRING NOT NULL",
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice')")

    dialect = _make_s3_sr_dialect(starrocks_config, s3_config, run_id)
    current = dialect.get_evolution_table_schema(table)
    desired = TableSchema(
        table_name=current.table_name,
        columns=[
            ColumnDef(name="id", type="BIGINT", nullable=False),
            ColumnDef(name="name", type="STRING", nullable=True),
        ],
    )
    delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )
    dialect.apply_schema_delta(load_mode, table, delta)

    cols = _sr_get_columns(starrocks_client, db, table)
    name_nullable = next(c[2] for c in cols if c[0] == "name")
    assert name_nullable == "YES"

    # Can now insert NULLs
    _sr_insert(starrocks_client, db, table, "(2, NULL)")
    assert _sr_row_count(starrocks_client, db, table) == 2


@pytest.mark.starrocks
@pytest.mark.s3
def test_schema_evolution_combined(
    starrocks_config, starrocks_client, s3_config
):
    """Combined evolution: add column, drop column, widen type, set nullable."""
    from tabsdata.connector.schema.evolution.core import compute_delta, validate_delta
    from tabsdata.connector.schema.evolution.models import ColumnDef, TableSchema
    from tabsdata.connector.schema.evolution.renderers.starrocks import (
        STARROCKS_TYPE_WIDENING_MAP,
    )

    _, run_id = encode_id()
    table = f"test_sr_evo_combined_{run_id}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table,
        "`key_col` BIGINT NOT NULL, `id` INT NOT NULL, `name` STRING NOT NULL, `value` FLOAT NOT NULL, `old_col` STRING NULL",
    )
    _sr_insert(starrocks_client, db, table, "(1, 1, 'alice', 1.5, 'legacy')")

    dialect = _make_s3_sr_dialect(starrocks_config, s3_config, run_id)
    current = dialect.get_evolution_table_schema(table)
    desired = TableSchema(
        table_name=current.table_name,
        columns=[
            ColumnDef(name="key_col", type="BIGINT", nullable=False),
            ColumnDef(name="id", type="BIGINT", nullable=False),
            ColumnDef(name="name", type="STRING", nullable=True),
            ColumnDef(name="value", type="DOUBLE", nullable=False),
            ColumnDef(name="category", type="STRING", nullable=True),
        ],
    )
    delta = compute_delta(current, desired, STARROCKS_TYPE_WIDENING_MAP)
    assert validate_delta(delta, strict=True) is True

    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )
    dialect.apply_schema_delta(load_mode, table, delta)

    cols = _sr_get_columns(starrocks_client, db, table)
    col_names = [c[0] for c in cols]
    assert "old_col" not in col_names
    assert "category" in col_names

    from tabsdata._io.cloud_staging.starrocks.dialect import _normalize_column_type

    id_type = _normalize_column_type(next(c[1] for c in cols if c[0] == "id"))
    value_type = _normalize_column_type(next(c[1] for c in cols if c[0] == "value"))
    name_nullable = next(c[2] for c in cols if c[0] == "name")
    assert id_type == "BIGINT"
    assert value_type == "DOUBLE"
    assert name_nullable == "YES"


@pytest.mark.starrocks
@pytest.mark.s3
def test_schema_evolution_no_changes(
    starrocks_config, starrocks_client, s3_config
):
    """When schemas match, no ALTER statements should be executed."""
    from tabsdata.connector.schema.evolution.core import compute_delta
    from tabsdata.connector.schema.evolution.renderers.starrocks import (
        STARROCKS_TYPE_WIDENING_MAP,
    )

    _, run_id = encode_id()
    table = f"test_sr_evo_noop_{run_id}"
    db = starrocks_config.database

    _sr_create_table(
        starrocks_client, db, table,
        "`id` BIGINT NOT NULL, `name` STRING NULL",
    )
    _sr_insert(starrocks_client, db, table, "(1, 'alice')")

    dialect = _make_s3_sr_dialect(starrocks_config, s3_config, run_id)
    current = dialect.get_evolution_table_schema(table)
    delta = compute_delta(current, current, STARROCKS_TYPE_WIDENING_MAP)
    assert delta.changes == []

    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )
    dialect.apply_schema_delta(load_mode, table, delta)

    assert _sr_row_count(starrocks_client, db, table) == 1


# ── _detect_nullability tests ────────────────────────────────────────────────────────


@pytest.mark.starrocks
def test_detect_nullability_no_nulls(tmp_path):
    """All columns have zero nulls in parquet stats."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "name": pa.array(["a", "b", "c"], type=pa.string()),
    })
    path = tmp_path / "no_nulls.parquet"
    pq.write_table(table, path)

    result = CloudStagedBulkLoader._detect_nullability([path])
    assert result["id"] is False
    assert result["name"] is False


@pytest.mark.starrocks
def test_detect_nullability_with_nulls(tmp_path):
    """Some columns have nulls."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "name": pa.array(["a", None, "c"], type=pa.string()),
    })
    path = tmp_path / "has_nulls.parquet"
    pq.write_table(table, path)

    result = CloudStagedBulkLoader._detect_nullability([path])
    assert result["id"] is False
    assert result["name"] is True


@pytest.mark.starrocks
def test_detect_nullability_multiple_files(tmp_path):
    """Null counts aggregated across multiple parquet files."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    # File 1: no nulls
    t1 = pa.table({"val": pa.array([1, 2], type=pa.int64())})
    p1 = tmp_path / "f1.parquet"
    pq.write_table(t1, p1)

    # File 2: has nulls
    t2 = pa.table({"val": pa.array([3, None], type=pa.int64())})
    p2 = tmp_path / "f2.parquet"
    pq.write_table(t2, p2)

    result = CloudStagedBulkLoader._detect_nullability([p1, p2])
    assert result["val"] is True  # file2 has nulls


# ── S3 Integration Tests ─────────────────────────────────────────────────────────────


@pytest.mark.starrocks
@pytest.mark.s3
def test_upload_with_s3_append(
    starrocks_config, s3_config, starrocks_client, s3_client
):
    _, run_id = encode_id()
    table = f"test_sr_s3_append_{run_id}"

    # Verify table does not exist
    with starrocks_client.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (starrocks_config.database, table),
        )
        assert cur.fetchone()[0] == 0

    bucket = s3_config["BUCKET_NAME"]
    region = s3_config["REGION"]
    base_path = f"staging/starrocks/test_sr_s3_append_{run_id}"
    s3_starrocks_config = S3Config(
        bucket=bucket,
        region=region,
        base_path=base_path,
        credentials=td.S3AccessKeyCredentials(
            s3_config["ACCESS_KEY"], s3_config["SECRET_KEY"]
        ),
    )
    cloud_storage = S3StarRocksStorage(s3_starrocks_config)
    sr_config = StarRocksConfig(
        host=starrocks_config.host,
        port=starrocks_config.port,
        database=starrocks_config.database,
        credentials=UserPasswordCredentials(
            starrocks_config.user, starrocks_config.password
        ),
    )
    dialect = StarRocksDialect(config=sr_config, cloud_storage=cloud_storage)
    loader = CloudStagedBulkLoader(
        cloud_storage=cloud_storage,
        dialect=dialect,
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id=run_id,
    )

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "starrocks_test_input", "mock_table.parquet"
    )

    for i in range(3):
        loader.load({table: [Path(input_file)]})

        _assert_starrocks_data_matches(
            starrocks_client, starrocks_config.database, table,
            input_file, expected_copies=i + 1,
        )

        # Verify staging files are cleaned up
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=base_path)
        assert response.get("KeyCount", 0) == 0, (
            f"Expected staging path '{base_path}' to be empty, "
            f"but found {response.get('KeyCount')} object(s)"
        )


@pytest.mark.starrocks
@pytest.mark.s3
def test_upload_with_s3_replace(
    starrocks_config, s3_config, starrocks_client, s3_client
):
    _, run_id = encode_id()
    table = f"test_sr_s3_replace_{run_id}"

    bucket = s3_config["BUCKET_NAME"]
    region = s3_config["REGION"]
    base_path = f"staging/starrocks/test_sr_s3_replace_{run_id}"
    s3_starrocks_config = S3Config(
        bucket=bucket,
        region=region,
        base_path=base_path,
        credentials=td.S3AccessKeyCredentials(
            s3_config["ACCESS_KEY"], s3_config["SECRET_KEY"]
        ),
    )
    cloud_storage = S3StarRocksStorage(s3_starrocks_config)
    sr_config = StarRocksConfig(
        host=starrocks_config.host,
        port=starrocks_config.port,
        database=starrocks_config.database,
        credentials=UserPasswordCredentials(
            starrocks_config.user, starrocks_config.password
        ),
    )
    dialect = StarRocksDialect(config=sr_config, cloud_storage=cloud_storage)
    loader = CloudStagedBulkLoader(
        cloud_storage=cloud_storage,
        dialect=dialect,
        write_mode=WriteMode.REPLACE,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id=run_id,
    )

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "starrocks_test_input", "mock_table.parquet"
    )

    for _ in range(3):
        loader.load({table: [Path(input_file)]})

        _assert_starrocks_data_matches(
            starrocks_client, starrocks_config.database, table,
            input_file, expected_copies=1,
        )

        # Verify staging files are cleaned up
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=base_path)
        assert response.get("KeyCount", 0) == 0, (
            f"Expected staging path '{base_path}' to be empty, "
            f"but found {response.get('KeyCount')} object(s)"
        )


@pytest.mark.starrocks
@pytest.mark.s3
def test_upload_with_s3_data_integrity(
    starrocks_config, s3_config, starrocks_client, s3_client
):
    """Verify that data loaded into StarRocks matches the source parquet."""
    _, run_id = encode_id()
    table = f"test_sr_s3_integrity_{run_id}"

    bucket = s3_config["BUCKET_NAME"]
    region = s3_config["REGION"]
    base_path = f"staging/starrocks/test_sr_s3_integrity_{run_id}"
    s3_starrocks_config = S3Config(
        bucket=bucket,
        region=region,
        base_path=base_path,
        credentials=td.S3AccessKeyCredentials(
            s3_config["ACCESS_KEY"], s3_config["SECRET_KEY"]
        ),
    )
    cloud_storage = S3StarRocksStorage(s3_starrocks_config)
    sr_config = StarRocksConfig(
        host=starrocks_config.host,
        port=starrocks_config.port,
        database=starrocks_config.database,
        credentials=UserPasswordCredentials(
            starrocks_config.user, starrocks_config.password
        ),
    )
    dialect = StarRocksDialect(config=sr_config, cloud_storage=cloud_storage)
    loader = CloudStagedBulkLoader(
        cloud_storage=cloud_storage,
        dialect=dialect,
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id=run_id,
    )

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "starrocks_test_input", "mock_table.parquet"
    )
    loader.load({table: [Path(input_file)]})

    _assert_starrocks_data_matches(
        starrocks_client, starrocks_config.database, table, input_file,
    )


# ── GCS integration tests ─────────────────────────────────────────────────────────────


def _delete_gcs_base_path_folder(bucket: str, base_path: str) -> None:
    """Delete the base_path folder from a GCS HNS bucket using storage_control_v2."""
    from google.cloud import storage_control_v2

    sc_client = storage_control_v2.StorageControlClient()
    folder_resource = sc_client.folder_path(
        project="_",
        bucket=bucket,
        folder=base_path.rstrip("/"),
    )
    try:
        sc_client.delete_folder(
            request=storage_control_v2.DeleteFolderRequest(name=folder_resource)
        )
    except Exception:
        pass


def _make_gcs_sr_loader(
    starrocks_config, bucket, base_path, run_id, write_mode, gcs_config
):
    gcs_starrocks_config = GCSConfig(
        bucket=bucket,
        base_path=base_path,
        upload_credentials=td.GCPServiceAccountKeyCredentials(
            td.EnvironmentSecret(gcs_config["ENV"])
        ),
        hmac_credentials=td.HMACCredentials(
            gcs_config["ACCESS_KEY"], gcs_config["SECRET_KEY"]
        ),
    )
    cloud_storage = GCSStarRocksStorage(gcs_starrocks_config)
    sr_config = StarRocksConfig(
        host=starrocks_config.host,
        port=starrocks_config.port,
        database=starrocks_config.database,
        credentials=UserPasswordCredentials(
            starrocks_config.user, starrocks_config.password
        ),
    )
    dialect = StarRocksDialect(config=sr_config, cloud_storage=cloud_storage)
    loader = CloudStagedBulkLoader(
        cloud_storage=cloud_storage,
        dialect=dialect,
        write_mode=write_mode,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id=run_id,
    )
    return loader


@pytest.mark.starrocks
@pytest.mark.gcs
def test_upload_with_gcs_append(
    starrocks_config, gcs_config, gcs_client, starrocks_client
):
    _, run_id = encode_id()
    table = f"test_sr_gcs_append_{run_id}"

    bucket = gcs_config["BUCKET"]
    base_path = f"staging/starrocks/test_sr_gcs_append_{run_id}"
    loader = _make_gcs_sr_loader(
        starrocks_config, bucket, base_path, run_id, WriteMode.APPEND, gcs_config
    )

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "starrocks_test_input", "mock_table.parquet"
    )
    gcs_bucket = gcs_client.bucket(bucket)

    for i in range(3):
        loader.load({table: [Path(input_file)]})

        _assert_starrocks_data_matches(
            starrocks_client, starrocks_config.database, table,
            input_file, expected_copies=i + 1,
        )

        remaining = list(gcs_bucket.list_blobs(prefix=base_path + "/"))
        assert len(remaining) == 0, (
            f"Expected staging path '{base_path}' to be empty, "
            f"but found {len(remaining)} object(s)"
        )

    _delete_gcs_base_path_folder(bucket, base_path)
    remaining = list(gcs_bucket.list_blobs(prefix=base_path))
    assert len(remaining) == 0, (
        f"Expected base path '{base_path}' to be fully cleaned up, "
        f"but found {len(remaining)} object(s)"
    )


@pytest.mark.starrocks
@pytest.mark.gcs
def test_upload_with_gcs_replace(
    starrocks_config, gcs_config, gcs_client, starrocks_client
):
    _, run_id = encode_id()
    table = f"test_sr_gcs_replace_{run_id}"

    bucket = gcs_config["BUCKET"]
    base_path = f"staging/starrocks/test_sr_gcs_replace_{run_id}"
    loader = _make_gcs_sr_loader(
        starrocks_config, bucket, base_path, run_id, WriteMode.REPLACE, gcs_config
    )

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "starrocks_test_input", "mock_table.parquet"
    )
    gcs_bucket = gcs_client.bucket(bucket)

    for _ in range(3):
        loader.load({table: [Path(input_file)]})

        _assert_starrocks_data_matches(
            starrocks_client, starrocks_config.database, table,
            input_file, expected_copies=1,
        )

        remaining = list(gcs_bucket.list_blobs(prefix=base_path + "/"))
        assert len(remaining) == 0, (
            f"Expected staging path '{base_path}' to be empty, "
            f"but found {len(remaining)} object(s)"
        )

    _delete_gcs_base_path_folder(bucket, base_path)
    remaining = list(gcs_bucket.list_blobs(prefix=base_path))
    assert len(remaining) == 0, (
        f"Expected base path '{base_path}' to be fully cleaned up, "
        f"but found {len(remaining)} object(s)"
    )


@pytest.mark.starrocks
@pytest.mark.gcs
def test_upload_with_gcs_data_integrity(
    starrocks_config, gcs_config, gcs_client, starrocks_client
):
    """Verify that data loaded into StarRocks via GCS matches the source parquet."""
    _, run_id = encode_id()
    table = f"test_sr_gcs_integrity_{run_id}"

    bucket = gcs_config["BUCKET"]
    base_path = f"staging/starrocks/test_sr_gcs_integrity_{run_id}"
    loader = _make_gcs_sr_loader(
        starrocks_config, bucket, base_path, run_id, WriteMode.APPEND, gcs_config
    )

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "starrocks_test_input", "mock_table.parquet"
    )
    loader.load({table: [Path(input_file)]})

    _assert_starrocks_data_matches(
        starrocks_client, starrocks_config.database, table, input_file,
    )

    gcs_bucket = gcs_client.bucket(bucket)
    _delete_gcs_base_path_folder(bucket, base_path)
    remaining = list(gcs_bucket.list_blobs(prefix=base_path))
    assert len(remaining) == 0, (
        f"Expected base path '{base_path}' to be fully cleaned up, "
        f"but found {len(remaining)} object(s)"
    )


# ── Azure integration tests ───────────────────────────────────────────────────────────


def _make_azure_sr_loader(
    starrocks_config, container, base_path, run_id, write_mode, azure_credentials
):
    azure_starrocks_config = AzureConfig(
        container=container,
        base_path=base_path,
        credentials=azure_credentials,
    )
    cloud_storage = AzureStarRocksStorage(azure_starrocks_config)
    sr_config = StarRocksConfig(
        host=starrocks_config.host,
        port=starrocks_config.port,
        database=starrocks_config.database,
        credentials=UserPasswordCredentials(
            starrocks_config.user, starrocks_config.password
        ),
    )
    dialect = StarRocksDialect(config=sr_config, cloud_storage=cloud_storage)
    loader = CloudStagedBulkLoader(
        cloud_storage=cloud_storage,
        dialect=dialect,
        write_mode=write_mode,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id=run_id,
    )
    return loader


@pytest.mark.starrocks
@pytest.mark.azure
def test_upload_with_azure_append(
    starrocks_config, azure_config, azure_client, starrocks_client
):
    _, run_id = encode_id()
    table = f"test_sr_azure_append_{run_id}"

    container = azure_config["CONTAINER_NAME"]
    base_path = f"staging/starrocks/test_sr_azure_append_{run_id}"
    loader = _make_azure_sr_loader(
        starrocks_config,
        container,
        base_path,
        run_id,
        WriteMode.APPEND,
        azure_config["CREDENTIALS"],
    )

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "starrocks_test_input", "mock_table.parquet"
    )
    az_container = azure_client.get_container_client(container)

    for i in range(3):
        loader.load({table: [Path(input_file)]})

        _assert_starrocks_data_matches(
            starrocks_client, starrocks_config.database, table,
            input_file, expected_copies=i + 1,
        )

        remaining = list(az_container.list_blobs(name_starts_with=base_path + "/"))
        assert len(remaining) == 0, (
            f"Expected staging path '{base_path}' to be empty, "
            f"but found {len(remaining)} object(s)"
        )


@pytest.mark.starrocks
@pytest.mark.azure
def test_upload_with_azure_replace(
    starrocks_config, azure_config, azure_client, starrocks_client
):
    _, run_id = encode_id()
    table = f"test_sr_azure_replace_{run_id}"

    container = azure_config["CONTAINER_NAME"]
    base_path = f"staging/starrocks/test_sr_azure_replace_{run_id}"
    loader = _make_azure_sr_loader(
        starrocks_config,
        container,
        base_path,
        run_id,
        WriteMode.REPLACE,
        azure_config["CREDENTIALS"],
    )

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "starrocks_test_input", "mock_table.parquet"
    )
    az_container = azure_client.get_container_client(container)

    for _ in range(3):
        loader.load({table: [Path(input_file)]})

        _assert_starrocks_data_matches(
            starrocks_client, starrocks_config.database, table,
            input_file, expected_copies=1,
        )

        remaining = list(az_container.list_blobs(name_starts_with=base_path + "/"))
        assert len(remaining) == 0, (
            f"Expected staging path '{base_path}' to be empty, "
            f"but found {len(remaining)} object(s)"
        )


@pytest.mark.starrocks
@pytest.mark.azure
def test_upload_with_azure_data_integrity(
    starrocks_config, azure_config, azure_client, starrocks_client
):
    """Verify that data loaded into StarRocks via Azure matches the source parquet."""
    _, run_id = encode_id()
    table = f"test_sr_azure_integrity_{run_id}"

    container = azure_config["CONTAINER_NAME"]
    base_path = f"staging/starrocks/test_sr_azure_integrity_{run_id}"
    loader = _make_azure_sr_loader(
        starrocks_config,
        container,
        base_path,
        run_id,
        WriteMode.APPEND,
        azure_config["CREDENTIALS"],
    )

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "starrocks_test_input", "mock_table.parquet"
    )
    loader.load({table: [Path(input_file)]})

    _assert_starrocks_data_matches(
        starrocks_client, starrocks_config.database, table, input_file,
    )

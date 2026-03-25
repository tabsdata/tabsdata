#
# Copyright 2026 Tabs Data Inc.
#

import sys
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import polars as pl
import os
from tabsdata import UserPasswordCredentials
from tabsdata._utils.id import encode_id
from test_tabsdata.conftest import TESTING_RESOURCES_FOLDER
from tabsdata._io.cloud_staging.common.bulk_loader import CloudStagedBulkLoader
from tabsdata._io.cloud_staging.clickhouse.cloud_storage import (
    AzureClickHouseStorage,
    GCSClickHouseStorage,
    S3ClickHouseStorage,
)
from tabsdata._io.cloud_staging.common.storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)
import tabsdata as td
from pathlib import Path

from tabsdata._io.cloud_staging.clickhouse.cloud_storage import ClickHouseCloudStorage
from tabsdata._io.cloud_staging.clickhouse.config import ClickHouseConfig
from tabsdata._io.cloud_staging.clickhouse.dialect import (
    ClickHouseDialect,
    _arrow_type_to_clickhouse,
    _clickhouse_type_to_arrow,
    schema_to_clickhouse_columns,
)
from tabsdata._io.cloud_staging.common.errors import SchemaError
from tabsdata._io.cloud_staging.common.modes import LoadMode, SchemaEvolutionMode, WriteMode
from tabsdata._io.cloud_staging.common.statuses import BulkLoadStatus


# ── Fixtures ──────────────────────────────────────────────────────────────────────────


@pytest.fixture
def mock_ch_module():
    """Patch clickhouse_connect so the dialect constructor works without the package."""
    mock_module = MagicMock()
    mock_client = MagicMock()
    mock_module.get_client.return_value = mock_client
    with patch.dict(sys.modules, {"clickhouse_connect": mock_module}):
        yield mock_module, mock_client


@pytest.fixture
def cloud_storage():
    return MagicMock(spec=ClickHouseCloudStorage)


@pytest.fixture
def dialect(mock_ch_module, cloud_storage):
    mock_ch_module  # ensure the sys.modules patch is active during construction
    config = ClickHouseConfig(host="localhost", port=8123, database="testdb")
    return ClickHouseDialect(config=config, cloud_storage=cloud_storage)


@pytest.fixture
def client(dialect):
    """Returns the mock clickhouse_connect client stored inside the dialect."""
    return dialect._client


@pytest.fixture
def load_mode():
    return LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id="test-upload-001",
    )


# ── Constructor ───────────────────────────────────────────────────────────────────────

@pytest.mark.clickhouse
def test_constructor_calls_get_client_with_correct_args(mock_ch_module, cloud_storage):
    mock_module, _ = mock_ch_module
    config = ClickHouseConfig(host="myhost", port=9000, database="mydb", secure=True)
    ClickHouseDialect(config=config, cloud_storage=cloud_storage)
    mock_module.get_client.assert_called_once_with(
        host="myhost",
        port=9000,
        database="mydb",
        username="default",
        password="",
        secure=True,
    )

@pytest.mark.clickhouse
def test_constructor_uses_credentials_when_provided(mock_ch_module, cloud_storage):
    from tabsdata._credentials import UserPasswordCredentials

    mock_module, _ = mock_ch_module
    creds = UserPasswordCredentials("alice", "s3cr3t")
    config = ClickHouseConfig(
        host="localhost", port=8123, database="testdb", credentials=creds
    )
    ClickHouseDialect(config=config, cloud_storage=cloud_storage)
    mock_module.get_client.assert_called_once_with(
        host="localhost",
        port=8123,
        database="testdb",
        username="alice",
        password="s3cr3t",
        secure=False,
    )

@pytest.mark.clickhouse
def test_constructor_raises_if_clickhouse_connect_missing(cloud_storage):
    config = ClickHouseConfig(host="localhost")
    # Remove clickhouse_connect from sys.modules so the import fails
    with patch.dict(sys.modules, {"clickhouse_connect": None}):
        with pytest.raises(ImportError, match="clickhouse-connect"):
            ClickHouseDialect(config=config, cloud_storage=cloud_storage)


# ── Capability flags ──────────────────────────────────────────────────────────────────

@pytest.mark.clickhouse
def test_supports_tabsdata_schema_evolution(dialect):
    assert dialect.supports_tabsdata_schema_evolution() is True

@pytest.mark.clickhouse
def test_does_not_support_native_schema_evolution(dialect):
    assert dialect.supports_native_schema_evolution() is False

@pytest.mark.clickhouse
def test_does_not_support_transactions(dialect):
    assert dialect.supports_transactions() is False

@pytest.mark.clickhouse
def test_does_not_support_auto_cleanup(dialect):
    assert dialect.supports_auto_cleanup() is False

@pytest.mark.clickhouse
def test_bulk_load_is_not_async(dialect):
    assert dialect.is_bulk_load_async() is False


# ── _arrow_type_to_clickhouse ──────────────────────────────────────────────────────────
# Returns the raw ClickHouse type string WITHOUT a Nullable() wrapper.


@pytest.mark.parametrize(
    "arrow_type,expected",
    [
        (pa.bool_(), "Bool"),
        (pa.int8(), "Int8"),
        (pa.int16(), "Int16"),
        (pa.int32(), "Int32"),
        (pa.int64(), "Int64"),
        (pa.uint8(), "UInt8"),
        (pa.uint16(), "UInt16"),
        (pa.uint32(), "UInt32"),
        (pa.uint64(), "UInt64"),
        (pa.float16(), "Float32"),
        (pa.float32(), "Float32"),
        (pa.float64(), "Float64"),
        (pa.string(), "String"),
        (pa.utf8(), "String"),
        (pa.large_string(), "String"),
        (pa.large_utf8(), "String"),
        (pa.binary(), "String"),
        (pa.date32(), "Date32"),
        # Timestamp: precision is derived from the time unit
        (pa.timestamp("s"), "DateTime64(0)"),
        (pa.timestamp("ms"), "DateTime64(3)"),
        (pa.timestamp("us"), "DateTime64(6)"),
        (pa.timestamp("ns"), "DateTime64(9)"),
        (pa.timestamp("us", tz="UTC"), "DateTime64(6, 'UTC')"),
        (pa.timestamp("ns", tz="America/New_York"), "DateTime64(9, 'America/New_York')"),
        # Decimal
        (pa.decimal128(10, 3), "Decimal(10, 3)"),
        (pa.decimal128(38, 10), "Decimal(38, 10)"),
        # Lists (no Nullable wrapping even when the element type would be nullable)
        (pa.list_(pa.int32()), "Array(Int32)"),
        (pa.list_(pa.string()), "Array(String)"),
        (pa.large_list(pa.float64()), "Array(Float64)"),
    ],
)
@pytest.mark.clickhouse
def test_arrow_type_to_clickhouse(arrow_type, expected):
    assert _arrow_type_to_clickhouse(arrow_type) == expected

@pytest.mark.clickhouse
def test_arrow_type_to_clickhouse_nested_list():
    assert _arrow_type_to_clickhouse(pa.list_(pa.list_(pa.string()))) == "Array(Array(String))"

@pytest.mark.clickhouse
def test_arrow_type_to_clickhouse_map():
    assert _arrow_type_to_clickhouse(pa.map_(pa.string(), pa.int64())) == "Map(String, Int64)"

@pytest.mark.clickhouse
def test_arrow_type_to_clickhouse_map_with_nested_value():
    result = _arrow_type_to_clickhouse(pa.map_(pa.string(), pa.list_(pa.int32())))
    assert result == "Map(String, Array(Int32))"

@pytest.mark.clickhouse
def test_arrow_type_to_clickhouse_struct():
    arrow_type = pa.struct(
        [pa.field("x", pa.int32()), pa.field("y", pa.float64())]
    )
    assert _arrow_type_to_clickhouse(arrow_type) == "Tuple(x Int32, y Float64)"

@pytest.mark.clickhouse
def test_arrow_type_to_clickhouse_unsupported_raises():
    with pytest.raises(TypeError, match="Unsupported PyArrow type for ClickHouse"):
        _arrow_type_to_clickhouse(pa.time32("s"))


# ── schema_to_clickhouse_columns ──────────────────────────────────────────────────────

@pytest.mark.clickhouse
def test_schema_to_clickhouse_columns_nullable_non_container():
    schema = pa.schema([pa.field("id", pa.int64(), nullable=True)])
    assert schema_to_clickhouse_columns(schema) == [("id", "Nullable(Int64)")]

@pytest.mark.clickhouse
def test_schema_to_clickhouse_columns_non_nullable_no_wrapper():
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    assert schema_to_clickhouse_columns(schema) == [("id", "Int64")]

@pytest.mark.clickhouse
def test_schema_to_clickhouse_columns_container_never_wrapped():
    # Array, Map, and Struct are never wrapped in Nullable even when nullable=True
    schema = pa.schema(
        [
            pa.field("arr", pa.list_(pa.int32()), nullable=True),
            pa.field("m", pa.map_(pa.string(), pa.int64()), nullable=True),
            pa.field("tup", pa.struct([pa.field("a", pa.int8())]), nullable=True),
        ]
    )
    cols = schema_to_clickhouse_columns(schema)
    assert cols == [
        ("arr", "Array(Int32)"),
        ("m", "Map(String, Int64)"),
        ("tup", "Tuple(a Int8)"),
    ]

@pytest.mark.clickhouse
def test_schema_to_clickhouse_columns_mixed():
    schema = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("tags", pa.list_(pa.string()), nullable=True),
            pa.field("score", pa.float64(), nullable=True),
        ]
    )
    cols = schema_to_clickhouse_columns(schema)
    assert cols == [
        ("id", "Int32"),
        ("name", "Nullable(String)"),
        ("tags", "Array(String)"),
        ("score", "Nullable(Float64)"),
    ]


# ── _clickhouse_type_to_arrow ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "ch_type,expected",
    [
        # Simple types
        ("Int8", pa.int8()),
        ("Int16", pa.int16()),
        ("Int32", pa.int32()),
        ("Int64", pa.int64()),
        ("UInt8", pa.uint8()),
        ("UInt16", pa.uint16()),
        ("UInt32", pa.uint32()),
        ("UInt64", pa.uint64()),
        ("Float32", pa.float32()),
        ("Float64", pa.float64()),
        ("Bool", pa.bool_()),
        ("String", pa.string()),
        ("Date", pa.date32()),
        ("Date32", pa.date32()),
        ("DateTime", pa.timestamp("s")),
        # Nullable wrapper is transparently stripped
        ("Nullable(Int64)", pa.int64()),
        ("Nullable(String)", pa.string()),
        ("Nullable(Float32)", pa.float32()),
        # LowCardinality wrapper is transparently stripped
        ("LowCardinality(String)", pa.string()),
        ("LowCardinality(Nullable(String))", pa.string()),
        # FixedString maps to String
        ("FixedString(16)", pa.string()),
        ("FixedString(256)", pa.string()),
        # DateTime64: precision maps to exact unit
        ("DateTime64(0)", pa.timestamp("s")),
        ("DateTime64(3)", pa.timestamp("ms")),
        ("DateTime64(6)", pa.timestamp("us")),
        ("DateTime64(9)", pa.timestamp("ns")),
        ("DateTime64(6, 'UTC')", pa.timestamp("us", tz="UTC")),
        ("DateTime64(9, 'Europe/Berlin')", pa.timestamp("ns", tz="Europe/Berlin")),
        # DateTime64 with unknown precision falls back to microseconds
        ("DateTime64(7)", pa.timestamp("us")),
        # Decimal variants
        ("Decimal(10, 3)", pa.decimal128(10, 3)),
        ("Decimal(38, 10)", pa.decimal128(38, 10)),
        ("Decimal32(4)", pa.decimal128(9, 4)),
        ("Decimal64(6)", pa.decimal128(18, 6)),
        ("Decimal128(8)", pa.decimal128(38, 8)),
        # Array
        ("Array(Int32)", pa.list_(pa.int32())),
        ("Array(String)", pa.list_(pa.string())),
        ("Array(Nullable(String))", pa.list_(pa.string())),
        ("Array(Float64)", pa.list_(pa.float64())),
        # Map
        ("Map(String, Int64)", pa.map_(pa.string(), pa.int64())),
        ("Map(String, Nullable(Float64))", pa.map_(pa.string(), pa.float64())),
    ],
)
@pytest.mark.clickhouse
def test_clickhouse_type_to_arrow(ch_type, expected):
    assert _clickhouse_type_to_arrow(ch_type) == expected

@pytest.mark.clickhouse
def test_clickhouse_type_to_arrow_whitespace_trimmed():
    assert _clickhouse_type_to_arrow("  Int32  ") == pa.int32()

@pytest.mark.clickhouse
def test_clickhouse_type_to_arrow_nested_array():
    assert _clickhouse_type_to_arrow("Array(Array(Int32))") == pa.list_(pa.list_(pa.int32()))

@pytest.mark.clickhouse
def test_clickhouse_type_to_arrow_map_nested_value():
    result = _clickhouse_type_to_arrow("Map(String, Array(Int32))")
    assert result == pa.map_(pa.string(), pa.list_(pa.int32()))

@pytest.mark.clickhouse
def test_clickhouse_type_to_arrow_unsupported_raises():
    with pytest.raises(TypeError, match="Unsupported ClickHouse type"):
        _clickhouse_type_to_arrow("IPv4")


# ── table_exists ──────────────────────────────────────────────────────────────────────

@pytest.mark.clickhouse
def test_table_exists_returns_true_when_count_nonzero(dialect, client, load_mode):
    client.query.return_value.result_rows = [(1,)]
    assert dialect.table_exists(load_mode, "my_table") is True

@pytest.mark.clickhouse
def test_table_exists_returns_false_when_count_zero(dialect, client, load_mode):
    client.query.return_value.result_rows = [(0,)]
    assert dialect.table_exists(load_mode, "my_table") is False

@pytest.mark.clickhouse
def test_table_exists_passes_correct_parameters(dialect, client, load_mode):
    client.query.return_value.result_rows = [(1,)]
    dialect.table_exists(load_mode, "my_table")
    client.query.assert_called_once_with(
        "SELECT count() FROM system.tables "
        "WHERE database = {db:String} AND name = {tbl:String}",
        parameters={"db": "testdb", "tbl": "my_table"},
    )


# ── get_table_schema ──────────────────────────────────────────────────────────────────

@pytest.mark.clickhouse
def test_get_table_schema_returns_correct_schema(dialect, client, load_mode):
    client.query.return_value.result_rows = [
        ("id", "Int64"),
        ("name", "Nullable(String)"),
        ("score", "Float64"),
    ]
    result = dialect.get_table_schema(load_mode, "my_table")
    assert result == pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("score", pa.float64()),
        ]
    )

@pytest.mark.clickhouse
def test_get_table_schema_passes_correct_parameters(dialect, client, load_mode):
    client.query.return_value.result_rows = []
    dialect.get_table_schema(load_mode, "my_table")
    client.query.assert_called_once_with(
        "SELECT name, type FROM system.columns "
        "WHERE database = {db:String} AND table = {tbl:String} "
        "ORDER BY position",
        parameters={"db": "testdb", "tbl": "my_table"},
    )

@pytest.mark.clickhouse
def test_get_table_schema_empty_table_returns_empty_schema(dialect, client, load_mode):
    client.query.return_value.result_rows = []
    assert dialect.get_table_schema(load_mode, "empty_table") == pa.schema([])


# ── create_table ──────────────────────────────────────────────────────────────────────

@pytest.mark.clickhouse
def test_create_table_generates_correct_ddl(dialect, client, load_mode):
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    dialect.create_table(load_mode, "my_table", arrow_schema)
    ddl = client.command.call_args[0][0]
    assert "CREATE TABLE `testdb`.`my_table`" in ddl
    assert "`id` Int64" in ddl
    assert "`name` Nullable(String)" in ddl
    assert "ENGINE = MergeTree()" in ddl
    assert "ORDER BY tuple()" in ddl

@pytest.mark.clickhouse
def test_create_table_container_columns_not_wrapped(dialect, client, load_mode):
    arrow_schema = pa.schema(
        [
            pa.field("tags", pa.list_(pa.string()), nullable=True),
            pa.field("meta", pa.map_(pa.string(), pa.int64()), nullable=True),
        ]
    )
    dialect.create_table(load_mode, "events", arrow_schema)
    ddl = client.command.call_args[0][0]
    assert "`tags` Array(String)" in ddl
    assert "`meta` Map(String, Int64)" in ddl
    assert "Nullable(Array" not in ddl
    assert "Nullable(Map" not in ddl

@pytest.mark.clickhouse
def test_create_table_timestamp_column(dialect, client, load_mode):
    arrow_schema = pa.schema(
        [pa.field("ts", pa.timestamp("us", tz="UTC"), nullable=True)]
    )
    dialect.create_table(load_mode, "events", arrow_schema)
    ddl = client.command.call_args[0][0]
    assert "`ts` Nullable(DateTime64(6, 'UTC'))" in ddl

@pytest.mark.clickhouse
def test_create_table_decimal_column(dialect, client, load_mode):
    arrow_schema = pa.schema(
        [pa.field("amount", pa.decimal128(18, 4), nullable=False)]
    )
    dialect.create_table(load_mode, "payments", arrow_schema)
    ddl = client.command.call_args[0][0]
    assert "`amount` Decimal(18, 4)" in ddl


# ── create_table with column_nulls ────────────────────────────────────────────────────


@pytest.mark.clickhouse
def test_create_table_with_column_nulls_no_nulls(dialect, client, load_mode):
    """Parquet stats say no nulls → columns are NOT NULL in DDL."""
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=True),   # Arrow says nullable
            pa.field("name", pa.string(), nullable=True),  # Arrow says nullable
        ]
    )
    column_nulls = {"id": False, "name": False}
    dialect.create_table(load_mode, "t", arrow_schema, column_nulls)
    ddl = client.command.call_args[0][0]
    assert "`id` Int64" in ddl
    assert "`name` String" in ddl
    assert "Nullable" not in ddl


@pytest.mark.clickhouse
def test_create_table_with_column_nulls_has_nulls(dialect, client, load_mode):
    """Parquet stats say has nulls → columns are Nullable in DDL."""
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=True),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    column_nulls = {"id": False, "name": True}
    dialect.create_table(load_mode, "t", arrow_schema, column_nulls)
    ddl = client.command.call_args[0][0]
    assert "`id` Int64" in ddl
    assert "`name` Nullable(String)" in ddl


@pytest.mark.clickhouse
def test_create_table_with_column_nulls_none_is_nullable(dialect, client, load_mode):
    """Missing stats (None) → treated as nullable (safe default)."""
    arrow_schema = pa.schema(
        [pa.field("id", pa.int64(), nullable=True)]
    )
    column_nulls = {"id": None}
    dialect.create_table(load_mode, "t", arrow_schema, column_nulls)
    ddl = client.command.call_args[0][0]
    assert "`id` Nullable(Int64)" in ddl


@pytest.mark.clickhouse
def test_create_table_with_column_nulls_containers_never_nullable(
    dialect, client, load_mode
):
    """Container types never get Nullable even if parquet stats say has nulls."""
    arrow_schema = pa.schema(
        [
            pa.field("tags", pa.list_(pa.string()), nullable=True),
            pa.field("id", pa.int64(), nullable=True),
        ]
    )
    column_nulls = {"tags": True, "id": False}
    dialect.create_table(load_mode, "t", arrow_schema, column_nulls)
    ddl = client.command.call_args[0][0]
    assert "`tags` Array(String)" in ddl
    assert "Nullable(Array" not in ddl
    assert "`id` Int64" in ddl


@pytest.mark.clickhouse
def test_create_table_with_column_nulls_mixed(dialect, client, load_mode):
    """Mixed nullability: some null, some not, some missing stats."""
    arrow_schema = pa.schema(
        [
            pa.field("a", pa.int64(), nullable=True),
            pa.field("b", pa.string(), nullable=True),
            pa.field("c", pa.float64(), nullable=True),
        ]
    )
    column_nulls = {"a": False, "b": True, "c": None}
    dialect.create_table(load_mode, "t", arrow_schema, column_nulls)
    ddl = client.command.call_args[0][0]
    assert "`a` Int64" in ddl
    assert "`b` Nullable(String)" in ddl
    assert "`c` Nullable(Float64)" in ddl


@pytest.mark.clickhouse
def test_create_table_without_column_nulls_uses_arrow_flag(
    dialect, client, load_mode
):
    """Without column_nulls, Arrow's nullable flag is used (backward compat)."""
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    dialect.create_table(load_mode, "t", arrow_schema)
    ddl = client.command.call_args[0][0]
    assert "`id` Int64" in ddl
    assert "`name` Nullable(String)" in ddl


# ── verify_schema ─────────────────────────────────────────────────────────────────────

@pytest.mark.clickhouse
def test_verify_schema_returns_true_when_schemas_match_exactly(
    dialect, client, load_mode
):
    # Non-nullable id → raw "Int64"; nullable name → "Nullable(String)"
    client.query.return_value.result_rows = [
        ("id", "Int64"),
        ("name", "Nullable(String)"),
    ]
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    assert dialect.verify_schema(load_mode, "my_table", arrow_schema) is True

@pytest.mark.clickhouse
def test_verify_schema_raises_for_extra_columns_in_existing_table(
    dialect, client, load_mode
):
    # Existing table has an extra column — exact match required, so this must fail
    client.query.return_value.result_rows = [
        ("id", "Int64"),
        ("name", "Nullable(String)"),
        ("extra", "Float32"),
    ]
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    with pytest.raises(SchemaError):
        dialect.verify_schema(load_mode, "my_table", arrow_schema)

@pytest.mark.clickhouse
def test_verify_schema_raises_for_missing_column(dialect, client, load_mode):
    client.query.return_value.result_rows = [("id", "Int64")]
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
        ]
    )
    with pytest.raises(SchemaError):
        dialect.verify_schema(load_mode, "my_table", arrow_schema)

@pytest.mark.clickhouse
def test_verify_schema_raises_for_type_mismatch(dialect, client, load_mode):
    # Existing: Int32, Expected from non-nullable int64 field: Int64
    client.query.return_value.result_rows = [("id", "Int32")]
    arrow_schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    with pytest.raises(SchemaError):
        dialect.verify_schema(load_mode, "my_table", arrow_schema)

@pytest.mark.clickhouse
def test_verify_schema_raises_for_nullable_mismatch(dialect, client, load_mode):
    # Existing: Nullable(Int64), but expected from non-nullable field: Int64
    client.query.return_value.result_rows = [("id", "Nullable(Int64)")]
    arrow_schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    with pytest.raises(SchemaError):
        dialect.verify_schema(load_mode, "my_table", arrow_schema)

@pytest.mark.clickhouse
def test_verify_schema_error_message_includes_table_info(dialect, client, load_mode):
    client.query.return_value.result_rows = [("id", "Int32")]
    arrow_schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    with pytest.raises(SchemaError, match="my_table"):
        dialect.verify_schema(load_mode, "my_table", arrow_schema)

@pytest.mark.clickhouse
def test_verify_schema_queries_system_columns(dialect, client, load_mode):
    client.query.return_value.result_rows = [("id", "Int64")]
    arrow_schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    dialect.verify_schema(load_mode, "my_table", arrow_schema)
    client.query.assert_called_once_with(
        "SELECT name, type FROM system.columns "
        "WHERE database = {db:String} AND table = {tbl:String} "
        "ORDER BY position",
        parameters={"db": "testdb", "tbl": "my_table"},
    )


# ── truncate_table ────────────────────────────────────────────────────────────────────

@pytest.mark.clickhouse
def test_truncate_table_issues_correct_command(dialect, client, load_mode):
    dialect.truncate_table(load_mode, "my_table")
    client.command.assert_called_once_with("TRUNCATE TABLE `testdb`.`my_table`")

@pytest.mark.clickhouse
def test_truncate_table_uses_qualified_name(mock_ch_module, cloud_storage, load_mode):
    config = ClickHouseConfig(host="localhost", database="prod")
    dialect = ClickHouseDialect(config=config, cloud_storage=cloud_storage)
    dialect.truncate_table(load_mode, "orders")
    dialect._client.command.assert_called_once_with("TRUNCATE TABLE `prod`.`orders`")


# ── bulk_load ─────────────────────────────────────────────────────────────────────────

@pytest.mark.clickhouse
def test_bulk_load_returns_completed_status(dialect, client, cloud_storage, load_mode):
    cloud_storage.clickhouse_table_function.return_value = "s3('https://bucket/f.parquet', 'Parquet')"
    result = dialect.bulk_load(load_mode, "my_table", ["ref1"])
    assert result.status == BulkLoadStatus.COMPLETED

@pytest.mark.clickhouse
def test_bulk_load_calls_table_function_for_each_ref(
    dialect, client, cloud_storage, load_mode
):
    cloud_storage.clickhouse_table_function.return_value = "s3('https://bucket/f.parquet', 'Parquet')"
    refs = ["ref1", "ref2", "ref3"]
    dialect.bulk_load(load_mode, "my_table", refs)
    assert cloud_storage.clickhouse_table_function.call_count == 3
    cloud_storage.clickhouse_table_function.assert_any_call("ref1")
    cloud_storage.clickhouse_table_function.assert_any_call("ref2")
    cloud_storage.clickhouse_table_function.assert_any_call("ref3")

@pytest.mark.clickhouse
def test_bulk_load_issues_insert_select_for_each_ref(
    dialect, client, cloud_storage, load_mode
):
    table_fn = "s3('https://bucket/file.parquet', 'key', 'secret', 'Parquet')"
    cloud_storage.clickhouse_table_function.return_value = table_fn
    dialect.bulk_load(load_mode, "my_table", ["ref1", "ref2"])
    assert client.command.call_count == 2
    expected_sql = f"INSERT INTO `testdb`.`my_table` SELECT * FROM {table_fn}"
    for call in client.command.call_args_list:
        assert call[0][0] == expected_sql

@pytest.mark.clickhouse
def test_bulk_load_empty_list_returns_completed_without_commands(
    dialect, client, cloud_storage, load_mode
):
    result = dialect.bulk_load(load_mode, "my_table", [])
    assert result.status == BulkLoadStatus.COMPLETED
    client.command.assert_not_called()
    cloud_storage.clickhouse_table_function.assert_not_called()

@pytest.mark.clickhouse
def test_bulk_load_uses_cloud_storage_table_function_not_raw_ref(
    dialect, client, cloud_storage, load_mode
):
    cloud_storage.clickhouse_table_function.return_value = (
        "azureBlobStorage('conn', 'container', 'blob.parquet', 'Parquet')"
    )
    dialect.bulk_load(load_mode, "my_table", ["staging/upload1/data.parquet"])
    sql = client.command.call_args[0][0]
    assert "azureBlobStorage" in sql
    assert "staging/upload1/data.parquet" not in sql


# ── get_type_widening_map ─────────────────────────────────────────────────────────────

@pytest.mark.clickhouse
def test_get_type_widening_map_integer_widening(dialect):
    widening = dialect.get_type_widening_map()
    assert widening["Int16"] == {"Int8"}
    assert widening["Int32"] == {"Int8", "Int16"}
    assert widening["Int64"] == {"Int8", "Int16", "Int32"}

@pytest.mark.clickhouse
def test_get_type_widening_map_unsigned_integer_widening(dialect):
    widening = dialect.get_type_widening_map()
    assert widening["UInt16"] == {"UInt8"}
    assert widening["UInt32"] == {"UInt8", "UInt16"}
    assert widening["UInt64"] == {"UInt8", "UInt16", "UInt32"}

@pytest.mark.clickhouse
def test_get_type_widening_map_float_widening(dialect):
    widening = dialect.get_type_widening_map()
    assert widening["Float64"] == {"Float32"}

@pytest.mark.clickhouse
def test_get_type_widening_map_does_not_include_string_or_date(dialect):
    widening = dialect.get_type_widening_map()
    assert "String" not in widening
    assert "Date32" not in widening
    assert "Bool" not in widening


@pytest.mark.clickhouse
@pytest.mark.s3
@pytest.mark.parametrize("secure", [False, True], ids=["insecure", "secure"])
def test_upload_with_s3_append(clickhouse_config, s3_config, clickhouse_client,
                               s3_client, secure):
    _, run_id = encode_id()
    if secure and not os.environ.get("CLICKHOUSE0__CLICKHOUSE_ENVS"):
        pytest.skip("secure=True requires a TLS-enabled ClickHouse; skipping for local Docker")
    secure_label = "secure" if secure else "insecure"
    table = f"test_upload_with_s3_append_{run_id}_{secure_label}"
    # Check if a table exists
    result = clickhouse_client.query(
        "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
        parameters={"db": clickhouse_config.database, "tbl": table},
    )
    exists = result.result_rows[0][0] > 0
    assert not exists
    bucket = s3_config["BUCKET_NAME"]
    region = s3_config["REGION"]
    base_path = f"staging/clickhouse/test_upload_with_s3_append_{run_id}_{secure_label}"
    s3_clickhouse_config = S3Config(
        bucket=bucket,
        region=region,
        base_path=base_path,
        credentials=td.S3AccessKeyCredentials(
            s3_config["ACCESS_KEY"], s3_config["SECRET_KEY"]
        ),
    )
    cloud_storage = S3ClickHouseStorage(s3_clickhouse_config)
    ch_config = ClickHouseConfig(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        database=clickhouse_config.database,
        credentials=UserPasswordCredentials(
            clickhouse_config.user, clickhouse_config.password
        ),
        secure=secure,
    )

    dialect = ClickHouseDialect(config=ch_config, cloud_storage=cloud_storage)
    loader = CloudStagedBulkLoader(
        cloud_storage=cloud_storage,
        dialect=dialect,
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id=run_id,
    )

    import pandas as pd

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "clickhouse_test_input", "mock_table.parquet"
    )
    base_df = pl.read_parquet(input_file).to_pandas()
    _schema = pq.read_schema(input_file)
    sort_cols = [
        f.name for f in _schema
        if not (
            pa.types.is_list(f.type)
            or pa.types.is_large_list(f.type)
            or pa.types.is_map(f.type)
            or pa.types.is_struct(f.type)
        )
    ]
    complex_cols = [
        f.name for f in _schema
        if (
            pa.types.is_list(f.type)
            or pa.types.is_large_list(f.type)
            or pa.types.is_map(f.type)
            or pa.types.is_struct(f.type)
        )
    ]
    map_cols = [f.name for f in _schema if pa.types.is_map(f.type)]
    order_col = "col_int32"

    for i in range(3):
        loader.load({table: [Path(input_file)]})

        result = clickhouse_client.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
            parameters={"db": clickhouse_config.database, "tbl": table},
        )
        exists = result.result_rows[0][0] > 0
        assert exists

        # After i+1 appends the table must contain exactly (i+1) copies of the input.
        # Sort both frames by all columns (NaNs first) for a deterministic row order
        # since MergeTree gives no ordering guarantee.
        # check_dtype=False tolerates Parquet int32 vs ClickHouse Int64/nullable;
        # rtol handles floating-point rounding differences.
        expected_df = (
            pd.concat([base_df] * (i + 1), ignore_index=True)[sort_cols]
            .sort_values(sort_cols, na_position="first")
            .reset_index(drop=True)
        )
        actual_df = (
            clickhouse_client.query_df(
                f"SELECT * FROM {clickhouse_config.database}.{table}"
            )[sort_cols]
            .sort_values(sort_cols, na_position="first")
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            expected_df,
            actual_df,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
        )
        expected_complex = (
            pd.concat([base_df] * (i + 1), ignore_index=True)[complex_cols + [order_col]]
            .sort_values(order_col)
            .reset_index(drop=True)
        )
        # Polars reads Arrow Map as [{key: k, value: v}]; ClickHouse returns native {k: v}.
        # Normalize expected map columns to match.
        for _col in map_cols:
            expected_complex[_col] = expected_complex[_col].apply(
                lambda v: {item["key"]: item["value"] for item in v}
            )
        actual_complex = (
            clickhouse_client.query_df(
                f"SELECT {', '.join(complex_cols + [order_col])} FROM {clickhouse_config.database}.{table}"
            )
            .sort_values(order_col)
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            expected_complex, actual_complex, check_dtype=False, check_exact=False
        )

        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=base_path)
        assert response.get("KeyCount", 0) == 0, (
            f"Expected staging path '{base_path}' to be empty, "
            f"but found {response.get('KeyCount')} object(s)"
        )


@pytest.mark.clickhouse
@pytest.mark.s3
@pytest.mark.parametrize("secure", [False, True], ids=["insecure", "secure"])
def test_upload_with_s3_replace(clickhouse_config, s3_config, clickhouse_client,
                                s3_client, secure):
    _, run_id = encode_id()
    if secure and not os.environ.get("CLICKHOUSE0__CLICKHOUSE_ENVS"):
        pytest.skip("secure=True requires a TLS-enabled ClickHouse; skipping for local Docker")
    secure_label = "secure" if secure else "insecure"
    table = f"test_upload_with_s3_replace_{run_id}_{secure_label}"
    # Check if a table exists
    result = clickhouse_client.query(
        "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
        parameters={"db": clickhouse_config.database, "tbl": table},
    )
    exists = result.result_rows[0][0] > 0
    assert not exists
    bucket = s3_config["BUCKET_NAME"]
    region = s3_config["REGION"]
    base_path = f"staging/clickhouse/test_upload_with_s3_replace_{run_id}_{secure_label}"
    s3_clickhouse_config = S3Config(
        bucket=bucket,
        region=region,
        base_path=base_path,
        credentials=td.S3AccessKeyCredentials(
            s3_config["ACCESS_KEY"], s3_config["SECRET_KEY"]
        ),
    )
    cloud_storage = S3ClickHouseStorage(s3_clickhouse_config)
    ch_config = ClickHouseConfig(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        database=clickhouse_config.database,
        credentials=UserPasswordCredentials(
            clickhouse_config.user, clickhouse_config.password
        ),
        secure=secure,
    )

    dialect = ClickHouseDialect(config=ch_config, cloud_storage=cloud_storage)
    loader = CloudStagedBulkLoader(
        cloud_storage=cloud_storage,
        dialect=dialect,
        write_mode=WriteMode.REPLACE,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id=run_id,
    )

    import pandas as pd

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "clickhouse_test_input", "mock_table.parquet"
    )
    base_df = pl.read_parquet(input_file).to_pandas()
    _schema = pq.read_schema(input_file)
    sort_cols = [
        f.name for f in _schema
        if not (
            pa.types.is_list(f.type)
            or pa.types.is_large_list(f.type)
            or pa.types.is_map(f.type)
            or pa.types.is_struct(f.type)
        )
    ]
    complex_cols = [
        f.name for f in _schema
        if (
            pa.types.is_list(f.type)
            or pa.types.is_large_list(f.type)
            or pa.types.is_map(f.type)
            or pa.types.is_struct(f.type)
        )
    ]
    map_cols = [f.name for f in _schema if pa.types.is_map(f.type)]
    order_col = "col_int32"

    for _ in range(3):
        loader.load({table: [Path(input_file)]})

        result = clickhouse_client.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
            parameters={"db": clickhouse_config.database, "tbl": table},
        )
        exists = result.result_rows[0][0] > 0
        assert exists

        # After i+1 appends the table must contain exactly (i+1) copies of the input.
        # Sort both frames by all columns (NaNs first) for a deterministic row order
        # since MergeTree gives no ordering guarantee.
        # check_dtype=False tolerates Parquet int32 vs ClickHouse Int64/nullable;
        # rtol handles floating-point rounding differences.
        expected_df = (
            base_df[sort_cols]
            .sort_values(sort_cols, na_position="first")
            .reset_index(drop=True)
        )
        actual_df = (
            clickhouse_client.query_df(
                f"SELECT * FROM {clickhouse_config.database}.{table}"
            )[sort_cols]
            .sort_values(sort_cols, na_position="first")
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            expected_df,
            actual_df,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
        )
        expected_complex = (
            base_df[complex_cols + [order_col]]
            .sort_values(order_col)
            .reset_index(drop=True)
        )
        # Polars reads Arrow Map as [{key: k, value: v}]; ClickHouse returns native {k: v}.
        # Normalize expected map columns to match.
        for _col in map_cols:
            expected_complex[_col] = expected_complex[_col].apply(
                lambda v: {item["key"]: item["value"] for item in v}
            )
        actual_complex = (
            clickhouse_client.query_df(
                f"SELECT {', '.join(complex_cols + [order_col])} FROM {clickhouse_config.database}.{table}"
            )
            .sort_values(order_col)
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            expected_complex, actual_complex, check_dtype=False, check_exact=False
        )

        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=base_path)
        assert response.get("KeyCount", 0) == 0, (
            f"Expected staging path '{base_path}' to be empty, "
            f"but found {response.get('KeyCount')} object(s)"
        )


# ── Schema evolution integration tests ────────────────────────────────────────────────


def _make_s3_dialect(clickhouse_config, s3_config, run_id):
    """Build a ClickHouseDialect backed by S3 cloud storage."""
    bucket = s3_config["BUCKET_NAME"]
    region = s3_config["REGION"]
    base_path = f"staging/clickhouse/schema_evo_{run_id}"
    s3_clickhouse_config = S3Config(
        bucket=bucket,
        region=region,
        base_path=base_path,
        credentials=td.S3AccessKeyCredentials(
            s3_config["ACCESS_KEY"], s3_config["SECRET_KEY"]
        ),
    )
    cloud_storage = S3ClickHouseStorage(s3_clickhouse_config)
    ch_config = ClickHouseConfig(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        database=clickhouse_config.database,
        credentials=UserPasswordCredentials(
            clickhouse_config.user, clickhouse_config.password
        ),
    )
    return ClickHouseDialect(config=ch_config, cloud_storage=cloud_storage)


def _create_table_direct(clickhouse_client, db, table, col_defs_sql):
    """Create a ClickHouse table using raw DDL."""
    clickhouse_client.command(
        f"CREATE TABLE `{db}`.`{table}` ({col_defs_sql}) "
        f"ENGINE = MergeTree() ORDER BY tuple()"
    )


def _get_column_info(clickhouse_client, db, table):
    """Return [(name, type, default_kind, default_expression)] from system.columns."""
    result = clickhouse_client.query(
        "SELECT name, type, default_kind, default_expression "
        "FROM system.columns "
        "WHERE database = {db:String} AND table = {tbl:String} "
        "ORDER BY position",
        parameters={"db": db, "tbl": table},
    )
    return result.result_rows


@pytest.mark.clickhouse
@pytest.mark.s3
def test_schema_evolution_add_nullable_column(
    clickhouse_config, clickhouse_client, s3_config
):
    """Add a nullable column to an existing table."""
    from tabsdata._utils.id import encode_id as _encode_id
    from tabsdata.connector.schema.evolution.core import compute_delta
    from tabsdata.connector.schema.evolution.models import ColumnDef, TableSchema
    from tabsdata.connector.schema.evolution.renderers.clickhouse import (
        CLICKHOUSE_TYPE_WIDENING_MAP,
    )

    _, run_id = _encode_id()
    table = f"test_evo_add_col_{run_id}"
    db = clickhouse_config.database

    _create_table_direct(
        clickhouse_client, db, table,
        "`id` Int64, `name` String",
    )
    clickhouse_client.command(
        f"INSERT INTO `{db}`.`{table}` VALUES (1, 'alice'), (2, 'bob')"
    )

    dialect = _make_s3_dialect(clickhouse_config, s3_config, run_id)
    current = dialect.get_evolution_table_schema(table)
    desired = TableSchema(
        table_name=current.table_name,
        columns=[
            ColumnDef(name="id", type="Int64", nullable=False),
            ColumnDef(name="name", type="String", nullable=False),
            ColumnDef(name="email", type="String", nullable=True),
        ],
    )
    delta = compute_delta(current, desired, CLICKHOUSE_TYPE_WIDENING_MAP)
    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )
    dialect.apply_schema_delta(load_mode, table, delta)

    cols = _get_column_info(clickhouse_client, db, table)
    col_names = [c[0] for c in cols]
    assert "email" in col_names
    email_type = next(c[1] for c in cols if c[0] == "email")
    assert "Nullable" in email_type

    # Old data still accessible, new column is NULL
    result = clickhouse_client.query(f"SELECT id, name, email FROM `{db}`.`{table}` ORDER BY id")
    rows = result.result_rows
    assert len(rows) == 2
    assert rows[0] == (1, "alice", None)
    assert rows[1] == (2, "bob", None)


@pytest.mark.clickhouse
@pytest.mark.s3
def test_schema_evolution_add_column_with_default(
    clickhouse_config, clickhouse_client, s3_config
):
    """Add a non-nullable column with a default value."""
    from tabsdata._utils.id import encode_id as _encode_id
    from tabsdata.connector.schema.evolution.core import compute_delta
    from tabsdata.connector.schema.evolution.models import ColumnDef, TableSchema
    from tabsdata.connector.schema.evolution.renderers.clickhouse import (
        CLICKHOUSE_TYPE_WIDENING_MAP,
    )

    _, run_id = _encode_id()
    table = f"test_evo_add_default_{run_id}"
    db = clickhouse_config.database

    _create_table_direct(
        clickhouse_client, db, table,
        "`id` Int64, `name` String",
    )
    clickhouse_client.command(
        f"INSERT INTO `{db}`.`{table}` VALUES (1, 'alice')"
    )

    dialect = _make_s3_dialect(clickhouse_config, s3_config, run_id)
    current = dialect.get_evolution_table_schema(table)
    desired = TableSchema(
        table_name=current.table_name,
        columns=[
            ColumnDef(name="id", type="Int64", nullable=False),
            ColumnDef(name="name", type="String", nullable=False),
            ColumnDef(
                name="status", type="String", nullable=False, default="'active'"
            ),
        ],
    )
    delta = compute_delta(current, desired, CLICKHOUSE_TYPE_WIDENING_MAP)
    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )
    dialect.apply_schema_delta(load_mode, table, delta)

    result = clickhouse_client.query(
        f"SELECT id, name, status FROM `{db}`.`{table}` ORDER BY id"
    )
    rows = result.result_rows
    assert len(rows) == 1
    assert rows[0] == (1, "alice", "active")


@pytest.mark.clickhouse
@pytest.mark.s3
def test_schema_evolution_drop_column(
    clickhouse_config, clickhouse_client, s3_config
):
    """Drop a column from an existing table."""
    from tabsdata._utils.id import encode_id as _encode_id
    from tabsdata.connector.schema.evolution.core import compute_delta
    from tabsdata.connector.schema.evolution.models import ColumnDef, TableSchema
    from tabsdata.connector.schema.evolution.renderers.clickhouse import (
        CLICKHOUSE_TYPE_WIDENING_MAP,
    )

    _, run_id = _encode_id()
    table = f"test_evo_drop_col_{run_id}"
    db = clickhouse_config.database

    _create_table_direct(
        clickhouse_client, db, table,
        "`id` Int64, `name` String, `old_col` Nullable(String)",
    )
    clickhouse_client.command(
        f"INSERT INTO `{db}`.`{table}` VALUES (1, 'alice', 'legacy')"
    )

    dialect = _make_s3_dialect(clickhouse_config, s3_config, run_id)
    current = dialect.get_evolution_table_schema(table)
    desired = TableSchema(
        table_name=current.table_name,
        columns=[
            ColumnDef(name="id", type="Int64", nullable=False),
            ColumnDef(name="name", type="String", nullable=False),
        ],
    )
    delta = compute_delta(current, desired, CLICKHOUSE_TYPE_WIDENING_MAP)
    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )
    dialect.apply_schema_delta(load_mode, table, delta)

    cols = _get_column_info(clickhouse_client, db, table)
    col_names = [c[0] for c in cols]
    assert "old_col" not in col_names

    result = clickhouse_client.query(f"SELECT * FROM `{db}`.`{table}`")
    rows = result.result_rows
    assert rows == [(1, "alice")]


@pytest.mark.clickhouse
@pytest.mark.s3
def test_schema_evolution_widen_type(
    clickhouse_config, clickhouse_client, s3_config
):
    """Widen Int32 → Int64 and Float32 → Float64."""
    from tabsdata._utils.id import encode_id as _encode_id
    from tabsdata.connector.schema.evolution.core import compute_delta, validate_delta
    from tabsdata.connector.schema.evolution.models import ColumnDef, TableSchema
    from tabsdata.connector.schema.evolution.renderers.clickhouse import (
        CLICKHOUSE_TYPE_WIDENING_MAP,
    )

    _, run_id = _encode_id()
    table = f"test_evo_widen_{run_id}"
    db = clickhouse_config.database

    _create_table_direct(
        clickhouse_client, db, table,
        "`id` Int32, `value` Float32",
    )
    clickhouse_client.command(
        f"INSERT INTO `{db}`.`{table}` VALUES (42, 3.14)"
    )

    dialect = _make_s3_dialect(clickhouse_config, s3_config, run_id)
    current = dialect.get_evolution_table_schema(table)
    desired = TableSchema(
        table_name=current.table_name,
        columns=[
            ColumnDef(name="id", type="Int64", nullable=False),
            ColumnDef(name="value", type="Float64", nullable=False),
        ],
    )
    delta = compute_delta(current, desired, CLICKHOUSE_TYPE_WIDENING_MAP)
    assert validate_delta(delta, strict=True) is True

    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )
    dialect.apply_schema_delta(load_mode, table, delta)

    cols = _get_column_info(clickhouse_client, db, table)
    id_type = next(c[1] for c in cols if c[0] == "id")
    value_type = next(c[1] for c in cols if c[0] == "value")
    assert id_type == "Int64"
    assert value_type == "Float64"

    result = clickhouse_client.query(f"SELECT id, value FROM `{db}`.`{table}` ORDER BY id")
    rows = result.result_rows
    assert len(rows) == 1
    assert rows[0][0] == 42
    assert abs(rows[0][1] - 3.14) < 0.01


@pytest.mark.clickhouse
@pytest.mark.s3
def test_schema_evolution_set_nullable(
    clickhouse_config, clickhouse_client, s3_config
):
    """Make a NOT NULL column nullable."""
    from tabsdata._utils.id import encode_id as _encode_id
    from tabsdata.connector.schema.evolution.core import compute_delta
    from tabsdata.connector.schema.evolution.models import ColumnDef, TableSchema
    from tabsdata.connector.schema.evolution.renderers.clickhouse import (
        CLICKHOUSE_TYPE_WIDENING_MAP,
    )

    _, run_id = _encode_id()
    table = f"test_evo_set_nullable_{run_id}"
    db = clickhouse_config.database

    _create_table_direct(
        clickhouse_client, db, table,
        "`id` Int64, `name` String",
    )
    clickhouse_client.command(
        f"INSERT INTO `{db}`.`{table}` VALUES (1, 'alice')"
    )

    dialect = _make_s3_dialect(clickhouse_config, s3_config, run_id)
    current = dialect.get_evolution_table_schema(table)
    desired = TableSchema(
        table_name=current.table_name,
        columns=[
            ColumnDef(name="id", type="Int64", nullable=False),
            ColumnDef(name="name", type="String", nullable=True),
        ],
    )
    delta = compute_delta(current, desired, CLICKHOUSE_TYPE_WIDENING_MAP)
    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )
    dialect.apply_schema_delta(load_mode, table, delta)

    cols = _get_column_info(clickhouse_client, db, table)
    name_type = next(c[1] for c in cols if c[0] == "name")
    assert name_type == "Nullable(String)"

    # Old data preserved, can now insert NULLs
    clickhouse_client.command(
        f"INSERT INTO `{db}`.`{table}` VALUES (2, NULL)"
    )
    result = clickhouse_client.query(
        f"SELECT id, name FROM `{db}`.`{table}` ORDER BY id"
    )
    rows = result.result_rows
    assert rows == [(1, "alice"), (2, None)]


@pytest.mark.clickhouse
@pytest.mark.s3
def test_schema_evolution_set_and_drop_default(
    clickhouse_config, clickhouse_client, s3_config
):
    """Set a default on a column, then drop it."""
    from tabsdata._utils.id import encode_id as _encode_id
    from tabsdata.connector.schema.evolution.core import compute_delta
    from tabsdata.connector.schema.evolution.models import ColumnDef, TableSchema
    from tabsdata.connector.schema.evolution.renderers.clickhouse import (
        CLICKHOUSE_TYPE_WIDENING_MAP,
    )

    _, run_id = _encode_id()
    table = f"test_evo_default_{run_id}"
    db = clickhouse_config.database

    _create_table_direct(
        clickhouse_client, db, table,
        "`id` Int64, `status` String",
    )

    dialect = _make_s3_dialect(clickhouse_config, s3_config, run_id)
    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )

    # Step 1: Set a default
    current = dialect.get_evolution_table_schema(table)
    with_default = TableSchema(
        table_name=current.table_name,
        columns=[
            ColumnDef(name="id", type="Int64", nullable=False),
            ColumnDef(
                name="status", type="String", nullable=False, default="'pending'"
            ),
        ],
    )
    delta = compute_delta(current, with_default, CLICKHOUSE_TYPE_WIDENING_MAP)
    dialect.apply_schema_delta(load_mode, table, delta)

    # Insert without specifying status — should get the default
    clickhouse_client.command(
        f"INSERT INTO `{db}`.`{table}` (`id`) VALUES (1)"
    )
    result = clickhouse_client.query(
        f"SELECT id, status FROM `{db}`.`{table}` ORDER BY id"
    )
    assert result.result_rows == [(1, "pending")]

    # Step 2: Drop the default
    current2 = dialect.get_evolution_table_schema(table)
    without_default = TableSchema(
        table_name=current2.table_name,
        columns=[
            ColumnDef(name="id", type="Int64", nullable=False),
            ColumnDef(name="status", type="String", nullable=False),
        ],
    )
    delta2 = compute_delta(current2, without_default, CLICKHOUSE_TYPE_WIDENING_MAP)
    dialect.apply_schema_delta(load_mode, table, delta2)

    cols = _get_column_info(clickhouse_client, db, table)
    status_default = next(c[2] for c in cols if c[0] == "status")
    assert status_default == ""  # no default


@pytest.mark.clickhouse
@pytest.mark.s3
def test_schema_evolution_combined(
    clickhouse_config, clickhouse_client, s3_config
):
    """Combined evolution: add column, drop column, widen type, set nullable."""
    from tabsdata._utils.id import encode_id as _encode_id
    from tabsdata.connector.schema.evolution.core import compute_delta, validate_delta
    from tabsdata.connector.schema.evolution.models import ColumnDef, TableSchema
    from tabsdata.connector.schema.evolution.renderers.clickhouse import (
        CLICKHOUSE_TYPE_WIDENING_MAP,
    )

    _, run_id = _encode_id()
    table = f"test_evo_combined_{run_id}"
    db = clickhouse_config.database

    _create_table_direct(
        clickhouse_client, db, table,
        "`id` Int32, `name` String, `value` Float32, `old_col` Nullable(String)",
    )
    clickhouse_client.command(
        f"INSERT INTO `{db}`.`{table}` VALUES (1, 'alice', 1.5, 'legacy')"
    )

    dialect = _make_s3_dialect(clickhouse_config, s3_config, run_id)
    current = dialect.get_evolution_table_schema(table)
    desired = TableSchema(
        table_name=current.table_name,
        columns=[
            ColumnDef(name="id", type="Int64", nullable=False),
            ColumnDef(name="name", type="String", nullable=True),
            ColumnDef(name="value", type="Float64", nullable=False),
            ColumnDef(
                name="category", type="String", nullable=True, default="'general'"
            ),
        ],
    )
    delta = compute_delta(current, desired, CLICKHOUSE_TYPE_WIDENING_MAP)
    assert validate_delta(delta, strict=True) is True

    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )
    dialect.apply_schema_delta(load_mode, table, delta)

    # Verify schema
    cols = _get_column_info(clickhouse_client, db, table)
    col_names = [c[0] for c in cols]
    assert col_names == ["id", "name", "value", "category"]
    assert "old_col" not in col_names

    id_type = next(c[1] for c in cols if c[0] == "id")
    name_type = next(c[1] for c in cols if c[0] == "name")
    value_type = next(c[1] for c in cols if c[0] == "value")
    category_type = next(c[1] for c in cols if c[0] == "category")

    assert id_type == "Int64"
    assert name_type == "Nullable(String)"
    assert value_type == "Float64"
    assert "Nullable" in category_type

    # Verify data preserved with new schema
    result = clickhouse_client.query(
        f"SELECT id, name, value, category FROM `{db}`.`{table}` ORDER BY id"
    )
    rows = result.result_rows
    assert len(rows) == 1
    assert rows[0][0] == 1
    assert rows[0][1] == "alice"
    assert abs(rows[0][2] - 1.5) < 0.01
    assert rows[0][3] == "general"  # default applied to existing row

    # Insert a row with the new schema
    clickhouse_client.command(
        f"INSERT INTO `{db}`.`{table}` VALUES (2, NULL, 2.5, 'sports')"
    )
    result = clickhouse_client.query(
        f"SELECT id, name, value, category FROM `{db}`.`{table}` ORDER BY id"
    )
    rows = result.result_rows
    assert len(rows) == 2
    assert rows[1] == (2, None, 2.5, "sports")


@pytest.mark.clickhouse
@pytest.mark.s3
def test_schema_evolution_no_changes(
    clickhouse_config, clickhouse_client, s3_config
):
    """When schemas match, no ALTER statements should be executed."""
    from tabsdata._utils.id import encode_id as _encode_id
    from tabsdata.connector.schema.evolution.core import compute_delta
    from tabsdata.connector.schema.evolution.renderers.clickhouse import (
        CLICKHOUSE_TYPE_WIDENING_MAP,
    )

    _, run_id = _encode_id()
    table = f"test_evo_noop_{run_id}"
    db = clickhouse_config.database

    _create_table_direct(
        clickhouse_client, db, table,
        "`id` Int64, `name` Nullable(String)",
    )
    clickhouse_client.command(
        f"INSERT INTO `{db}`.`{table}` VALUES (1, 'alice')"
    )

    dialect = _make_s3_dialect(clickhouse_config, s3_config, run_id)
    current = dialect.get_evolution_table_schema(table)
    delta = compute_delta(current, current, CLICKHOUSE_TYPE_WIDENING_MAP)
    assert delta.changes == []

    load_mode = LoadMode(
        write_mode=WriteMode.APPEND,
        schema_evolution=SchemaEvolutionMode.ICEBERG,
        upload_id=run_id,
    )
    dialect.apply_schema_delta(load_mode, table, delta)

    # Data untouched
    result = clickhouse_client.query(f"SELECT id, name FROM `{db}`.`{table}`")
    assert result.result_rows == [(1, "alice")]


# ── _detect_nullability tests ────────────────────────────────────────────────────────


@pytest.mark.clickhouse
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


@pytest.mark.clickhouse
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


@pytest.mark.clickhouse
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


@pytest.mark.clickhouse
def test_detect_nullability_multiple_files_no_nulls(tmp_path):
    """Multiple files, all with zero nulls."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    t1 = pa.table({"val": pa.array([1, 2], type=pa.int64())})
    p1 = tmp_path / "f1.parquet"
    pq.write_table(t1, p1)

    t2 = pa.table({"val": pa.array([3, 4], type=pa.int64())})
    p2 = tmp_path / "f2.parquet"
    pq.write_table(t2, p2)

    result = CloudStagedBulkLoader._detect_nullability([p1, p2])
    assert result["val"] is False


# ── GCS integration tests ─────────────────────────────────────────────────────────────


def _delete_gcs_base_path_folder(bucket: str, base_path: str) -> None:
    """Delete the base_path folder from a GCS HNS bucket using storage_control_v2.

    After all upload_id subfolders have been removed by the loader's cleanup,
    the base_path directory object itself remains in HNS buckets and must be
    deleted explicitly. On standard (non-HNS) buckets this is a silent no-op.
    """
    from google.cloud import storage_control_v2

    sc_client = storage_control_v2.StorageControlClient()
    folder_resource = sc_client.folder_path(
        project="_",
        bucket=bucket,
        folder=base_path.rstrip("/"),  # no trailing slash
    )
    try:
        sc_client.delete_folder(
            request=storage_control_v2.DeleteFolderRequest(name=folder_resource)
        )
    except Exception:
        pass  # Not an HNS bucket, or folder was already removed


def _make_gcs_loader(
    clickhouse_config, bucket, base_path, run_id, write_mode, gcs_config, secure=False
):
    gcs_clickhouse_config = GCSConfig(
        bucket=bucket,
        base_path=base_path,
        upload_credentials=td.GCPServiceAccountKeyCredentials(
            td.EnvironmentSecret(gcs_config["ENV"])
        ),
        hmac_credentials=td.HMACCredentials(
            gcs_config["ACCESS_KEY"], gcs_config["SECRET_KEY"]
        ),
    )
    cloud_storage = GCSClickHouseStorage(gcs_clickhouse_config)
    ch_config = ClickHouseConfig(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        database=clickhouse_config.database,
        credentials=UserPasswordCredentials(
            clickhouse_config.user, clickhouse_config.password
        ),
        secure=secure,
    )
    dialect = ClickHouseDialect(config=ch_config, cloud_storage=cloud_storage)
    loader = CloudStagedBulkLoader(
        cloud_storage=cloud_storage,
        dialect=dialect,
        write_mode=write_mode,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id=run_id,
    )
    return loader


@pytest.mark.clickhouse
@pytest.mark.gcs
@pytest.mark.parametrize("secure", [False, True], ids=["insecure", "secure"])
def test_upload_with_gcs_append(
    clickhouse_config, gcs_config, gcs_client, clickhouse_client, secure
):
    _, run_id = encode_id()
    if secure and not os.environ.get("CLICKHOUSE0__CLICKHOUSE_ENVS"):
        pytest.skip("secure=True requires a TLS-enabled ClickHouse; skipping for local Docker")
    secure_label = "secure" if secure else "insecure"
    table = f"test_upload_with_gcs_append_{run_id}_{secure_label}"
    result = clickhouse_client.query(
        "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
        parameters={"db": clickhouse_config.database, "tbl": table},
    )
    assert result.result_rows[0][0] == 0

    bucket = gcs_config["BUCKET"]
    base_path = f"staging/clickhouse/test_upload_with_gcs_append_{run_id}_{secure_label}"
    loader = _make_gcs_loader(
        clickhouse_config, bucket, base_path, run_id, WriteMode.APPEND, gcs_config,
        secure=secure,
    )

    import pandas as pd

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "clickhouse_test_input", "mock_table.parquet"
    )
    base_df = pl.read_parquet(input_file).to_pandas()
    _schema = pq.read_schema(input_file)
    sort_cols = [
        f.name for f in _schema
        if not (
            pa.types.is_list(f.type)
            or pa.types.is_large_list(f.type)
            or pa.types.is_map(f.type)
            or pa.types.is_struct(f.type)
        )
    ]
    complex_cols = [
        f.name for f in _schema
        if (
            pa.types.is_list(f.type)
            or pa.types.is_large_list(f.type)
            or pa.types.is_map(f.type)
            or pa.types.is_struct(f.type)
        )
    ]
    map_cols = [f.name for f in _schema if pa.types.is_map(f.type)]
    order_col = "col_int32"
    gcs_bucket = gcs_client.bucket(bucket)

    for i in range(3):
        loader.load({table: [Path(input_file)]})

        result = clickhouse_client.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
            parameters={"db": clickhouse_config.database, "tbl": table},
        )
        assert result.result_rows[0][0] > 0

        expected_df = (
            pd.concat([base_df] * (i + 1), ignore_index=True)[sort_cols]
            .sort_values(sort_cols, na_position="first")
            .reset_index(drop=True)
        )
        actual_df = (
            clickhouse_client.query_df(
                f"SELECT * FROM {clickhouse_config.database}.{table}"
            )[sort_cols]
            .sort_values(sort_cols, na_position="first")
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            expected_df,
            actual_df,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
        )
        expected_complex = (
            pd.concat([base_df] * (i + 1), ignore_index=True)[complex_cols + [order_col]]
            .sort_values(order_col)
            .reset_index(drop=True)
        )
        # Polars reads Arrow Map as [{key: k, value: v}]; ClickHouse returns native {k: v}.
        # Normalize expected map columns to match.
        for _col in map_cols:
            expected_complex[_col] = expected_complex[_col].apply(
                lambda v: {item["key"]: item["value"] for item in v}
            )
        actual_complex = (
            clickhouse_client.query_df(
                f"SELECT {', '.join(complex_cols + [order_col])} FROM {clickhouse_config.database}.{table}"
            )
            .sort_values(order_col)
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            expected_complex, actual_complex, check_dtype=False, check_exact=False
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


@pytest.mark.clickhouse
@pytest.mark.gcs
@pytest.mark.parametrize("secure", [False, True], ids=["insecure", "secure"])
def test_upload_with_gcs_replace(
    clickhouse_config, gcs_config, gcs_client, clickhouse_client, secure
):
    _, run_id = encode_id()
    if secure and not os.environ.get("CLICKHOUSE0__CLICKHOUSE_ENVS"):
        pytest.skip("secure=True requires a TLS-enabled ClickHouse; skipping for local Docker")
    secure_label = "secure" if secure else "insecure"
    table = f"test_upload_with_gcs_replace_{run_id}_{secure_label}"
    result = clickhouse_client.query(
        "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
        parameters={"db": clickhouse_config.database, "tbl": table},
    )
    assert result.result_rows[0][0] == 0

    bucket = gcs_config["BUCKET"]
    base_path = f"staging/clickhouse/test_upload_with_gcs_replace_{run_id}_{secure_label}"
    loader = _make_gcs_loader(
        clickhouse_config, bucket, base_path, run_id, WriteMode.REPLACE, gcs_config,
        secure=secure,
    )

    import pandas as pd

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "clickhouse_test_input", "mock_table.parquet"
    )
    base_df = pl.read_parquet(input_file).to_pandas()
    _schema = pq.read_schema(input_file)
    sort_cols = [
        f.name for f in _schema
        if not (
            pa.types.is_list(f.type)
            or pa.types.is_large_list(f.type)
            or pa.types.is_map(f.type)
            or pa.types.is_struct(f.type)
        )
    ]
    complex_cols = [
        f.name for f in _schema
        if (
            pa.types.is_list(f.type)
            or pa.types.is_large_list(f.type)
            or pa.types.is_map(f.type)
            or pa.types.is_struct(f.type)
        )
    ]
    map_cols = [f.name for f in _schema if pa.types.is_map(f.type)]
    order_col = "col_int32"
    gcs_bucket = gcs_client.bucket(bucket)

    for _ in range(3):
        loader.load({table: [Path(input_file)]})

        result = clickhouse_client.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
            parameters={"db": clickhouse_config.database, "tbl": table},
        )
        assert result.result_rows[0][0] > 0

        expected_df = (
            base_df[sort_cols]
            .sort_values(sort_cols, na_position="first")
            .reset_index(drop=True)
        )
        actual_df = (
            clickhouse_client.query_df(
                f"SELECT * FROM {clickhouse_config.database}.{table}"
            )[sort_cols]
            .sort_values(sort_cols, na_position="first")
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            expected_df,
            actual_df,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
        )
        expected_complex = (
            base_df[complex_cols + [order_col]]
            .sort_values(order_col)
            .reset_index(drop=True)
        )
        # Polars reads Arrow Map as [{key: k, value: v}]; ClickHouse returns native {k: v}.
        # Normalize expected map columns to match.
        for _col in map_cols:
            expected_complex[_col] = expected_complex[_col].apply(
                lambda v: {item["key"]: item["value"] for item in v}
            )
        actual_complex = (
            clickhouse_client.query_df(
                f"SELECT {', '.join(complex_cols + [order_col])} FROM {clickhouse_config.database}.{table}"
            )
            .sort_values(order_col)
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            expected_complex, actual_complex, check_dtype=False, check_exact=False
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


# ── Azure integration tests ───────────────────────────────────────────────────────────


def _make_azure_loader(
    clickhouse_config, container, base_path, run_id, write_mode, azure_credentials,
    secure=False,
):
    azure_clickhouse_config = AzureConfig(
        container=container,
        base_path=base_path,
        credentials=azure_credentials,
    )
    cloud_storage = AzureClickHouseStorage(azure_clickhouse_config)
    ch_config = ClickHouseConfig(
        host=clickhouse_config.host,
        port=clickhouse_config.port,
        database=clickhouse_config.database,
        credentials=UserPasswordCredentials(
            clickhouse_config.user, clickhouse_config.password
        ),
        secure=secure,
    )
    dialect = ClickHouseDialect(config=ch_config, cloud_storage=cloud_storage)
    loader = CloudStagedBulkLoader(
        cloud_storage=cloud_storage,
        dialect=dialect,
        write_mode=write_mode,
        schema_evolution=SchemaEvolutionMode.NONE,
        upload_id=run_id,
    )
    return loader


@pytest.mark.clickhouse
@pytest.mark.azure
@pytest.mark.parametrize("secure", [False, True], ids=["insecure", "secure"])
def test_upload_with_azure_append(
    clickhouse_config, azure_config, azure_client, clickhouse_client, secure
):
    _, run_id = encode_id()
    if secure and not os.environ.get("CLICKHOUSE0__CLICKHOUSE_ENVS"):
        pytest.skip("secure=True requires a TLS-enabled ClickHouse; skipping for local Docker")
    secure_label = "secure" if secure else "insecure"
    table = f"test_upload_with_azure_append_{run_id}_{secure_label}"
    result = clickhouse_client.query(
        "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
        parameters={"db": clickhouse_config.database, "tbl": table},
    )
    assert result.result_rows[0][0] == 0

    container = azure_config["CONTAINER_NAME"]
    base_path = f"staging/clickhouse/test_upload_with_azure_append_{run_id}_{secure_label}"
    loader = _make_azure_loader(
        clickhouse_config,
        container,
        base_path,
        run_id,
        WriteMode.APPEND,
        azure_config["CREDENTIALS"],
        secure=secure,
    )

    import pandas as pd

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "clickhouse_test_input", "mock_table.parquet"
    )
    base_df = pl.read_parquet(input_file).to_pandas()
    _schema = pq.read_schema(input_file)
    sort_cols = [
        f.name for f in _schema
        if not (
            pa.types.is_list(f.type)
            or pa.types.is_large_list(f.type)
            or pa.types.is_map(f.type)
            or pa.types.is_struct(f.type)
        )
    ]
    complex_cols = [
        f.name for f in _schema
        if (
            pa.types.is_list(f.type)
            or pa.types.is_large_list(f.type)
            or pa.types.is_map(f.type)
            or pa.types.is_struct(f.type)
        )
    ]
    map_cols = [f.name for f in _schema if pa.types.is_map(f.type)]
    order_col = "col_int32"
    az_container = azure_client.get_container_client(container)

    for i in range(3):
        loader.load({table: [Path(input_file)]})

        result = clickhouse_client.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
            parameters={"db": clickhouse_config.database, "tbl": table},
        )
        assert result.result_rows[0][0] > 0

        expected_df = (
            pd.concat([base_df] * (i + 1), ignore_index=True)[sort_cols]
            .sort_values(sort_cols, na_position="first")
            .reset_index(drop=True)
        )
        actual_df = (
            clickhouse_client.query_df(
                f"SELECT * FROM {clickhouse_config.database}.{table}"
            )[sort_cols]
            .sort_values(sort_cols, na_position="first")
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            expected_df,
            actual_df,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
        )
        expected_complex = (
            pd.concat([base_df] * (i + 1), ignore_index=True)[complex_cols + [order_col]]
            .sort_values(order_col)
            .reset_index(drop=True)
        )
        # Polars reads Arrow Map as [{key: k, value: v}]; ClickHouse returns native {k: v}.
        # Normalize expected map columns to match.
        for _col in map_cols:
            expected_complex[_col] = expected_complex[_col].apply(
                lambda v: {item["key"]: item["value"] for item in v}
            )
        actual_complex = (
            clickhouse_client.query_df(
                f"SELECT {', '.join(complex_cols + [order_col])} FROM {clickhouse_config.database}.{table}"
            )
            .sort_values(order_col)
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            expected_complex, actual_complex, check_dtype=False, check_exact=False
        )

        remaining = list(az_container.list_blobs(name_starts_with=base_path + "/"))
        assert len(remaining) == 0, (
            f"Expected staging path '{base_path}' to be empty, "
            f"but found {len(remaining)} object(s)"
        )


@pytest.mark.clickhouse
@pytest.mark.azure
@pytest.mark.parametrize("secure", [False, True], ids=["insecure", "secure"])
def test_upload_with_azure_replace(
    clickhouse_config, azure_config, azure_client, clickhouse_client, secure
):
    _, run_id = encode_id()
    if secure and not os.environ.get("CLICKHOUSE0__CLICKHOUSE_ENVS"):
        pytest.skip("secure=True requires a TLS-enabled ClickHouse; skipping for local Docker")
    secure_label = "secure" if secure else "insecure"
    table = f"test_upload_with_azure_replace_{run_id}_{secure_label}"
    result = clickhouse_client.query(
        "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
        parameters={"db": clickhouse_config.database, "tbl": table},
    )
    assert result.result_rows[0][0] == 0

    container = azure_config["CONTAINER_NAME"]
    base_path = f"staging/clickhouse/test_upload_with_azure_replace_{run_id}_{secure_label}"
    loader = _make_azure_loader(
        clickhouse_config,
        container,
        base_path,
        run_id,
        WriteMode.REPLACE,
        azure_config["CREDENTIALS"],
        secure=secure,
    )

    import pandas as pd

    input_file = os.path.join(
        TESTING_RESOURCES_FOLDER, "clickhouse_test_input", "mock_table.parquet"
    )
    base_df = pl.read_parquet(input_file).to_pandas()
    _schema = pq.read_schema(input_file)
    sort_cols = [
        f.name for f in _schema
        if not (
            pa.types.is_list(f.type)
            or pa.types.is_large_list(f.type)
            or pa.types.is_map(f.type)
            or pa.types.is_struct(f.type)
        )
    ]
    complex_cols = [
        f.name for f in _schema
        if (
            pa.types.is_list(f.type)
            or pa.types.is_large_list(f.type)
            or pa.types.is_map(f.type)
            or pa.types.is_struct(f.type)
        )
    ]
    map_cols = [f.name for f in _schema if pa.types.is_map(f.type)]
    order_col = "col_int32"
    az_container = azure_client.get_container_client(container)

    for _ in range(3):
        loader.load({table: [Path(input_file)]})

        result = clickhouse_client.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = {tbl:String}",
            parameters={"db": clickhouse_config.database, "tbl": table},
        )
        assert result.result_rows[0][0] > 0

        expected_df = (
            base_df[sort_cols]
            .sort_values(sort_cols, na_position="first")
            .reset_index(drop=True)
        )
        actual_df = (
            clickhouse_client.query_df(
                f"SELECT * FROM {clickhouse_config.database}.{table}"
            )[sort_cols]
            .sort_values(sort_cols, na_position="first")
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            expected_df,
            actual_df,
            check_dtype=False,
            check_exact=False,
            rtol=1e-5,
        )
        expected_complex = (
            base_df[complex_cols + [order_col]]
            .sort_values(order_col)
            .reset_index(drop=True)
        )
        # Polars reads Arrow Map as [{key: k, value: v}]; ClickHouse returns native {k: v}.
        # Normalize expected map columns to match.
        for _col in map_cols:
            expected_complex[_col] = expected_complex[_col].apply(
                lambda v: {item["key"]: item["value"] for item in v}
            )
        actual_complex = (
            clickhouse_client.query_df(
                f"SELECT {', '.join(complex_cols + [order_col])} FROM {clickhouse_config.database}.{table}"
            )
            .sort_values(order_col)
            .reset_index(drop=True)
        )
        pd.testing.assert_frame_equal(
            expected_complex, actual_complex, check_dtype=False, check_exact=False
        )

        remaining = list(az_container.list_blobs(name_starts_with=base_path + "/"))
        assert len(remaining) == 0, (
            f"Expected staging path '{base_path}' to be empty, "
            f"but found {len(remaining)} object(s)"
        )

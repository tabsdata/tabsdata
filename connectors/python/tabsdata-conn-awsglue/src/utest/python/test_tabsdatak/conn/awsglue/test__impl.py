#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import datetime as dt
import sys
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

import polars as pl
import pytest
from test_tabsdatak.conn.awsglue.support import conn

from tabsdatak.api import Secret

# noinspection PyProtectedMember
from tabsdatak.conn.awsglue import _impl as impl
from tabsdatak.conn.awsglue.error import AwsGlueErrorCode, AwsGlueRuntimeException
from tabsdatak.conn.common.file._store import FileStore
from tabsdatak.conn.common.sql.error import (
    SqlCommonErrorCode,
    SqlCommonRuntimeException,
)
from tabsdatak.conn.common.types import AwsAccessSecretKey

pytestmark = pytest.mark.conn_awsglue

# pyiceberg's local file IO reads a location as a POSIX path, so it cannot
# open the drive-letter paths of a Windows temporary directory. The tests that
# write through a local catalog run on Linux and macOS; on Windows the write
# path is covered by the integration tests.
local_catalog = pytest.mark.skipif(
    sys.platform == "win32",
    reason="pyiceberg's local file IO does not take Windows drive-letter paths",
)


# --- Catalog properties -------------------------------------------------


def test_catalog_properties_carry_the_region_and_credentials_to_both_clients():
    assert impl.catalog_properties(conn()) == {
        "glue.region": "us-east-1",
        "glue.access-key-id": "AKIA",
        "glue.secret-access-key": "shh",
        "s3.region": "us-east-1",
        "s3.access-key-id": "AKIA",
        "s3.secret-access-key": "shh",
    }


def test_catalog_properties_name_the_catalog_of_another_account():
    properties = impl.catalog_properties(conn(catalog_id="123456789012"))
    assert properties["glue.id"] == "123456789012"


def test_catalog_properties_forward_the_pyiceberg_keys_without_their_prefix():
    properties = impl.catalog_properties(
        conn(
            conn_cfg={
                "pyiceberg.glue.max-retries": 3,
                "pyiceberg.glue.skip-archive": False,
                "pyiceberg.s3.endpoint": "http://127.0.0.1:9000",
                "obstore.endpoint": "http://127.0.0.1:9000",
            }
        )
    )
    assert properties["glue.max-retries"] == 3
    assert properties["glue.skip-archive"] == "false"
    assert properties["s3.endpoint"] == "http://127.0.0.1:9000"
    assert not any("obstore" in key or "endpoint" == key for key in properties)


def test_a_forwarded_pyiceberg_key_overrides_the_one_the_connection_sets():
    properties = impl.catalog_properties(
        conn(conn_cfg={"pyiceberg.s3.region": "eu-west-1"})
    )
    assert properties["s3.region"] == "eu-west-1"
    assert properties["glue.region"] == "us-east-1"


def test_credentials_that_cannot_be_resolved_are_reported_as_such():
    unresolved = conn(
        credentials=AwsAccessSecretKey(
            access_key_id=Secret(), secret_access_key=Secret()
        )
    )
    with pytest.raises(AwsGlueRuntimeException) as raised:
        impl.catalog_properties(unresolved)
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_7


# --- Data files ---------------------------------------------------------


@pytest.mark.parametrize("scheme", ["s3", "s3a", "s3n"])
def test_data_target_writes_under_the_location_of_the_table(scheme):
    store, prefix = impl.data_target(
        conn(), "sales.orders", f"{scheme}://other-lake/tables/orders"
    )
    assert store.root == "s3://other-lake"
    assert prefix == "tables/orders"
    assert store.options["aws_access_key_id"] == "AKIA"
    assert store.options["aws_secret_access_key"] == "shh"
    assert store.options["aws_region"] == "us-east-1"


def test_data_target_forwards_the_obstore_keys_without_their_prefix():
    store, _ = impl.data_target(
        conn(
            conn_cfg={
                "obstore.endpoint": "http://127.0.0.1:9000",
                "pyiceberg.glue.max-retries": 3,
            }
        ),
        "sales.orders",
        "s3://my-lake/warehouse/sales/orders",
    )
    assert "http://127.0.0.1:9000" in store.options.values()
    assert not {3, "3"} & set(store.options.values())


def test_data_target_refuses_a_table_stored_outside_s3():
    with pytest.raises(AwsGlueRuntimeException) as raised:
        impl.data_target(conn(), "sales.orders", "hdfs://namenode/tables/orders")
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_14


# --- Names --------------------------------------------------------------


def test_identifier_takes_the_database_the_table_names():
    assert impl.identifier(conn(), "returns.orders") == ("returns", "orders")


def test_identifier_completes_a_bare_table_from_the_connection():
    assert impl.identifier(conn(), "orders") == ("sales", "orders")


def test_identifier_refuses_a_bare_table_without_a_default_database():
    with pytest.raises(AwsGlueRuntimeException) as raised:
        impl.identifier(conn(database=None), "orders")
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_8


def test_a_new_table_is_located_under_the_base_path_of_the_connection():
    assert impl.new_table_location(conn(), "sales", "orders") == (
        "s3://my-lake/warehouse/sales/orders"
    )


def test_a_new_table_is_located_at_the_bucket_root_without_a_base_path():
    assert impl.new_table_location(conn(base_path="/"), "sales", "orders") == (
        "s3://my-lake/sales/orders"
    )


# --- Types --------------------------------------------------------------


def _typed(dtype: pl.DataType, value=None) -> pl.LazyFrame:
    return pl.LazyFrame({"x": pl.Series([value], dtype=dtype)})


@pytest.mark.parametrize(
    "written,held",
    [
        (pl.Int8, pl.Int32),
        (pl.Int16, pl.Int32),
        (pl.UInt8, pl.Int32),
        (pl.UInt16, pl.Int32),
        (pl.UInt32, pl.Int64),
        (pl.UInt64, pl.Int64),
        (pl.Categorical, pl.String),
        (pl.Enum(["a", "b"]), pl.String),
        (pl.Datetime("ns"), pl.Datetime("us")),
        (pl.Datetime("ms"), pl.Datetime("us")),
        (pl.Datetime("ns", "Europe/Madrid"), pl.Datetime("us", "UTC")),
        (pl.List(pl.UInt32), pl.List(pl.Int64)),
        (pl.Array(pl.Int8, 2), pl.List(pl.Int32)),
        (pl.Struct({"a": pl.Int16}), pl.Struct({"a": pl.Int32})),
        (pl.List(pl.Struct({"a": pl.UInt8})), pl.List(pl.Struct({"a": pl.Int32}))),
    ],
)
def test_a_type_iceberg_lacks_is_written_as_the_one_that_holds_it(written, held):
    assert impl.iceberg_frame(_typed(written)).collect_schema()["x"] == held


@pytest.mark.parametrize(
    "dtype",
    [
        pl.Int32,
        pl.Int64,
        pl.Float32,
        pl.Float64,
        pl.Boolean,
        pl.String,
        pl.Binary,
        pl.Date,
        pl.Decimal(18, 4),
        pl.Datetime("us"),
        pl.Datetime("us", "UTC"),
    ],
)
def test_a_type_iceberg_holds_is_left_as_it_is(dtype):
    frame = _typed(dtype)
    assert impl.iceberg_frame(frame) is frame


@pytest.mark.parametrize(
    "dtype",
    [pl.Time, pl.Duration, pl.Null, pl.List(pl.Time), pl.Struct({"a": pl.Duration})],
)
def test_a_type_iceberg_cannot_hold_is_refused_by_column(dtype):
    with pytest.raises(AwsGlueRuntimeException) as raised:
        impl.iceberg_frame(_typed(dtype))
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_12
    assert "'x'" in str(raised.value)


def test_a_zoned_timestamp_keeps_its_instant_in_utc():
    madrid = dt.datetime(2026, 7, 1, 12, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    frame = pl.LazyFrame({"x": [madrid]}).with_columns(
        pl.col("x").dt.convert_time_zone("Europe/Madrid")
    )
    (value,) = impl.iceberg_frame(frame).collect()["x"].to_list()
    assert value == dt.datetime(2026, 7, 1, 10, tzinfo=dt.timezone.utc)


def test_an_unsigned_value_too_large_for_a_long_fails_the_write():
    frame = impl.iceberg_frame(_typed(pl.UInt64, 2**63))
    with pytest.raises(pl.exceptions.InvalidOperationError):
        frame.collect()


# --- Writes through a local catalog -------------------------------------


class Lake:
    def __init__(self, root: Path) -> None:
        from pyiceberg.catalog.sql import SqlCatalog

        self.root = root
        self.catalog = SqlCatalog(
            "local",
            uri=f"sqlite:///{root.as_posix()}/catalog.db",
            warehouse=root.as_uri(),
        )
        self.catalog.create_namespace("sales")
        self.store = FileStore(root.as_uri())

    def location(self, database: str, name: str) -> str:
        return (self.root / "warehouse" / database / name).as_uri()

    def target(self, location: str) -> tuple[FileStore, str]:
        path = Path(url2pathname(urlparse(location).path))
        return self.store, path.relative_to(self.root).as_posix()

    def rows(self, table: str = "sales.orders") -> list[dict]:
        rows = self.catalog.load_table(table).scan().to_arrow().to_pylist()
        return sorted(rows, key=lambda row: row["id"])

    def table(self, table: str = "sales.orders"):
        return self.catalog.load_table(table)

    def data_files(self) -> list[str]:
        return sorted(
            path.relative_to(self.root).as_posix()
            for path in self.root.rglob("*.parquet")
            if path.parent.name == impl.DATA_FOLDER
        )


@pytest.fixture
def lake(tmp_path, monkeypatch) -> Lake:
    found = Lake(tmp_path.resolve())
    monkeypatch.setattr(impl, "glue_catalog", lambda conn: found.catalog)
    monkeypatch.setattr(
        impl,
        "new_table_location",
        lambda conn, database, name: found.location(database, name),
    )
    monkeypatch.setattr(
        impl, "data_target", lambda conn, table, location: found.target(location)
    )
    return found


def _write(
    frame: pl.DataFrame,
    *,
    trx: str,
    tables: list[str] | None = None,
    if_table_exists: str = "append",
    schema_evolution: str = "strict",
    connection=None,
) -> None:
    cfg = impl.WriteCfg(
        conn=connection or conn(),
        tables=tables or ["orders"],
        if_table_exists=if_table_exists,
        schema_evolution=schema_evolution,
        transaction_id=trx,
    )
    impl.write_all_tables(cfg, [frame.lazy()] * len(cfg.tables))


def _orders(*ids: int) -> pl.DataFrame:
    return pl.DataFrame(
        {"id": list(ids), "name": [f"n{i}" for i in ids]},
        schema={"id": pl.Int64, "name": pl.String},
    )


@local_catalog
def test_a_write_creates_the_table_it_does_not_find(lake):
    _write(_orders(1, 2), trx="t1")
    assert lake.rows() == [{"id": 1, "name": "n1"}, {"id": 2, "name": "n2"}]
    assert lake.table().location() == lake.location("sales", "orders")


@local_catalog
def test_a_write_puts_its_file_in_the_data_folder_of_the_table(lake):
    _write(_orders(1), trx="t1")
    assert lake.data_files() == ["warehouse/sales/orders/data/tabsdata-t1.parquet"]


@local_catalog
def test_an_append_adds_rows(lake):
    _write(_orders(1), trx="t1")
    _write(_orders(2), trx="t2")
    assert [row["id"] for row in lake.rows()] == [1, 2]


@local_catalog
def test_a_replace_leaves_the_batch_alone_in_the_table(lake):
    _write(_orders(1, 2), trx="t1")
    _write(_orders(3), trx="t2", if_table_exists="replace")
    assert [row["id"] for row in lake.rows()] == [3]


@local_catalog
def test_a_replace_creates_the_table_it_does_not_find(lake):
    _write(_orders(1), trx="t1", if_table_exists="replace")
    assert [row["id"] for row in lake.rows()] == [1]


@local_catalog
def test_a_replace_with_an_empty_batch_empties_the_table(lake):
    _write(_orders(1, 2), trx="t1")
    _write(_orders(), trx="t2", if_table_exists="replace")
    assert lake.rows() == []


@local_catalog
def test_a_write_stamps_its_snapshots_with_the_trx(lake):
    _write(_orders(1), trx="t1")
    _write(_orders(2), trx="t2", if_table_exists="replace")
    stamps = [
        snapshot.summary.additional_properties.get(impl.TRX_PROPERTY)
        for snapshot in lake.table().metadata.snapshots
    ]
    assert stamps == ["t1", "t2", "t2"]


@local_catalog
@pytest.mark.parametrize("if_table_exists", ["append", "replace"])
def test_a_rerun_of_a_written_trx_writes_nothing(lake, if_table_exists):
    _write(_orders(1), trx="t1")
    _write(_orders(2), trx="t2", if_table_exists=if_table_exists)
    before = (lake.rows(), len(lake.table().metadata.snapshots))
    _write(_orders(2), trx="t2", if_table_exists=if_table_exists)
    assert (lake.rows(), len(lake.table().metadata.snapshots)) == before


@local_catalog
def test_a_rerun_is_skipped_once_the_stamp_is_gone_while_the_file_is_held(
    lake, monkeypatch
):
    _write(_orders(1), trx="t1")
    monkeypatch.setattr(impl, "stamped", lambda table, transaction_id: False)
    _write(_orders(1), trx="t1")
    assert [row["id"] for row in lake.rows()] == [1]
    assert len(lake.table().metadata.snapshots) == 1


@local_catalog
def test_a_write_reaches_each_table_of_the_destination(lake):
    lake.catalog.create_namespace("returns")
    _write(_orders(1), trx="t1", tables=["orders", "returns.orders"])
    assert [row["id"] for row in lake.rows("sales.orders")] == [1]
    assert [row["id"] for row in lake.rows("returns.orders")] == [1]


@local_catalog
def test_a_write_to_a_database_that_does_not_exist_is_refused(lake):
    with pytest.raises(AwsGlueRuntimeException) as raised:
        _write(_orders(1), trx="t1", tables=["nowhere.orders"])
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_9
    assert lake.data_files() == []


@local_catalog
def test_a_bare_table_without_a_default_database_is_refused_before_any_write(lake):
    with pytest.raises(AwsGlueRuntimeException) as raised:
        _write(
            _orders(1),
            trx="t1",
            tables=["sales.orders", "customers"],
            connection=conn(database=None),
        )
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_8
    assert not lake.catalog.table_exists("sales.orders")


# --- Schema evolution ---------------------------------------------------


@local_catalog
def test_a_batch_lacking_a_column_is_written_with_it_null(lake):
    _write(_orders(1), trx="t1")
    _write(pl.DataFrame({"id": [2]}), trx="t2")
    assert lake.rows() == [{"id": 1, "name": "n1"}, {"id": 2, "name": None}]


@local_catalog
def test_a_narrower_type_fits_the_wider_column(lake):
    _write(_orders(1), trx="t1")
    narrow = _orders(2).with_columns(pl.col("id").cast(pl.Int32))
    _write(narrow, trx="t2")
    assert [row["id"] for row in lake.rows()] == [1, 2]


@local_catalog
@pytest.mark.parametrize("if_table_exists", ["append", "replace"])
def test_strict_refuses_a_new_column_and_leaves_the_table_alone(lake, if_table_exists):
    _write(_orders(1), trx="t1")
    wider = _orders(2).with_columns(pl.lit(1.5).alias("amount"))
    with pytest.raises(SqlCommonRuntimeException) as raised:
        _write(wider, trx="t2", if_table_exists=if_table_exists)
    assert raised.value.api_error is SqlCommonErrorCode.SQL_COMMON_10
    assert "new column 'amount'" in str(raised.value)
    assert lake.rows() == [{"id": 1, "name": "n1"}]
    assert lake.data_files() == ["warehouse/sales/orders/data/tabsdata-t1.parquet"]


@local_catalog
def test_strict_takes_a_wider_type_whose_values_fit(lake):
    _write(_orders(1).with_columns(pl.col("id").cast(pl.Int32)), trx="t1")
    _write(_orders(2), trx="t2")
    assert [row["id"] for row in lake.rows()] == [1, 2]
    assert str(lake.table().schema().find_type("id")) == "int"


@local_catalog
def test_strict_takes_a_wider_decimal_whose_values_fit(lake):
    narrow = pl.DataFrame(
        {"id": [1], "amount": [Decimal("10.25")]},
        schema={"id": pl.Int64, "amount": pl.Decimal(10, 2)},
    )
    wide = pl.DataFrame(
        {"id": [2], "amount": [Decimal("99999999.99")]},
        schema={"id": pl.Int64, "amount": pl.Decimal(38, 2)},
    )
    _write(narrow, trx="t1")
    _write(wide, trx="t2")
    assert [row["amount"] for row in lake.rows()] == [
        Decimal("10.25"),
        Decimal("99999999.99"),
    ]
    assert str(lake.table().schema().find_type("amount")) == "decimal(10, 2)"


@local_catalog
@pytest.mark.parametrize("if_table_exists", ["append", "replace"])
def test_strict_fails_a_wider_value_that_does_not_fit_and_keeps_the_rows(
    lake, if_table_exists
):
    _write(_orders(1).with_columns(pl.col("id").cast(pl.Int32)), trx="t1")
    with pytest.raises(pl.exceptions.InvalidOperationError):
        _write(_orders(2**40), trx="t2", if_table_exists=if_table_exists)
    assert [row["id"] for row in lake.rows()] == [1]
    assert lake.data_files() == ["warehouse/sales/orders/data/tabsdata-t1.parquet"]


@local_catalog
def test_strict_refuses_a_wider_type_it_cannot_narrow(lake):
    nested = pl.DataFrame({"id": [1], "tags": [[1]]}).with_columns(
        pl.col("tags").cast(pl.List(pl.Int32))
    )
    _write(nested, trx="t1")
    wider = pl.DataFrame({"id": [2], "tags": [[2]]})
    with pytest.raises(SqlCommonRuntimeException) as raised:
        _write(wider, trx="t2")
    assert raised.value.api_error is SqlCommonErrorCode.SQL_COMMON_10


@local_catalog
def test_update_adds_the_new_column(lake):
    _write(_orders(1), trx="t1")
    wider = _orders(2).with_columns(pl.lit(1.5).alias("amount"))
    _write(wider, trx="t2", schema_evolution="update")
    assert lake.rows() == [
        {"id": 1, "name": "n1", "amount": None},
        {"id": 2, "name": "n2", "amount": 1.5},
    ]


@local_catalog
def test_update_widens_the_type(lake):
    _write(_orders(1).with_columns(pl.col("id").cast(pl.Int32)), trx="t1")
    _write(_orders(2**40), trx="t2", schema_evolution="update")
    assert [row["id"] for row in lake.rows()] == [1, 2**40]


@local_catalog
def test_update_evolves_a_replace_too(lake):
    _write(_orders(1), trx="t1")
    wider = _orders(2).with_columns(pl.lit(1.5).alias("amount"))
    _write(wider, trx="t2", if_table_exists="replace", schema_evolution="update")
    assert lake.rows() == [{"id": 2, "name": "n2", "amount": 1.5}]


@local_catalog
@pytest.mark.parametrize("schema_evolution", ["strict", "update"])
def test_a_change_iceberg_cannot_make_is_refused_under_either_policy(
    lake, schema_evolution
):
    _write(_orders(1), trx="t1")
    retyped = pl.DataFrame({"id": ["two"], "name": ["n2"]})
    with pytest.raises(SqlCommonRuntimeException) as raised:
        _write(retyped, trx="t2", schema_evolution=schema_evolution)
    assert raised.value.api_error is SqlCommonErrorCode.SQL_COMMON_10
    assert f"schema_evolution={schema_evolution!r}" in str(raised.value)
    assert lake.rows() == [{"id": 1, "name": "n1"}]
    assert lake.data_files() == ["warehouse/sales/orders/data/tabsdata-t1.parquet"]


@local_catalog
def test_a_batch_lacking_a_required_column_is_refused(lake):
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField, StringType

    lake.catalog.create_table(
        "sales.orders",
        schema=Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "name", StringType(), required=False),
        ),
        location=lake.location("sales", "orders"),
    )
    with pytest.raises(AwsGlueRuntimeException) as raised:
        _write(pl.DataFrame({"name": ["n1"]}), trx="t1")
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_13
    assert "id" in str(raised.value)


@local_catalog
def test_a_partitioned_table_is_refused(lake):
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import LongType, NestedField, StringType

    lake.catalog.create_table(
        "sales.orders",
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        ),
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=2, field_id=1000, transform=IdentityTransform(), name="name"
            )
        ),
        location=lake.location("sales", "orders"),
    )
    with pytest.raises(AwsGlueRuntimeException) as raised:
        _write(_orders(1), trx="t1")
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_11
    assert lake.data_files() == []


# --- Failures -----------------------------------------------------------


@local_catalog
def test_a_failed_commit_removes_the_file_it_uploaded(lake, monkeypatch):
    _write(_orders(1), trx="t1")

    def commit(table, uri, cfg, schema):
        raise RuntimeError("commit failed")

    monkeypatch.setattr(impl, "commit", commit)
    with pytest.raises(RuntimeError):
        _write(_orders(2), trx="t2")
    assert lake.data_files() == ["warehouse/sales/orders/data/tabsdata-t1.parquet"]
    assert [row["id"] for row in lake.rows()] == [1]


@local_catalog
def test_a_commit_of_unknown_outcome_keeps_the_file(lake, monkeypatch):
    from pyiceberg.exceptions import CommitStateUnknownException

    def commit(table, uri, cfg, schema):
        raise CommitStateUnknownException("timed out")

    monkeypatch.setattr(impl, "commit", commit)
    with pytest.raises(CommitStateUnknownException):
        _write(_orders(1), trx="t1")
    assert lake.data_files() == ["warehouse/sales/orders/data/tabsdata-t1.parquet"]


@local_catalog
def test_a_failed_upload_commits_nothing(lake):
    _write(_orders(1), trx="t1")
    overflowing = pl.DataFrame({
        "id": pl.Series([2**63], dtype=pl.UInt64),
        "name": ["big"],
    })
    with pytest.raises(pl.exceptions.InvalidOperationError):
        _write(overflowing, trx="t2")
    assert [row["id"] for row in lake.rows()] == [1]
    assert lake.data_files() == ["warehouse/sales/orders/data/tabsdata-t1.parquet"]


@local_catalog
def test_a_rerun_after_a_failed_commit_writes_the_batch_once(lake, monkeypatch):
    commit = impl.commit
    calls = []

    def flaky(table, uri, cfg, schema):
        calls.append(uri)
        if len(calls) == 1:
            raise RuntimeError("commit failed")
        commit(table, uri, cfg, schema)

    monkeypatch.setattr(impl, "commit", flaky)
    with pytest.raises(RuntimeError):
        _write(_orders(1), trx="t1")
    _write(_orders(1), trx="t1")
    assert [row["id"] for row in lake.rows()] == [1]
    assert lake.data_files() == ["warehouse/sales/orders/data/tabsdata-t1.parquet"]

#
# Copyright 2026 Tabsdata Inc.
#

"""AWS Glue destination-plugin integration tests: the connector alone against
the Glue Data Catalog and S3."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.awsglue import AwsGlueDomain, awsglue_domain
from tabsdatax.itest.fixture.functions.harness import assert_frames_match
from testi_tabsdatak.conn.awsglue.backend import TrxCtx, dest_conn, store
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir

# noinspection PyProtectedMember
from tabsdatak.conn.awsglue import AwsGlueDest, _plugin as plugin
from tabsdatak.conn.awsglue.error import AwsGlueErrorCode, AwsGlueRuntimeException

pytestmark = pytest.mark.conn_awsglue


def _orders(*ids: int) -> pl.DataFrame:
    return pl.DataFrame(
        {"id": list(ids), "name": [f"n{i}" for i in ids]},
        schema={"id": pl.Int64, "name": pl.String},
    )


def _write(
    domain: AwsGlueDomain,
    work: Path,
    tables: list[str],
    frames: list[pl.DataFrame],
    *,
    trx: str | None = None,
    conn=None,
    **dest,
) -> None:
    plugin.AwsGlueDestPlugin().write_out(
        TrxCtx(work, trx),
        conn or dest_conn(domain),
        AwsGlueDest(tables=tables, **dest),
        [frame.lazy() for frame in frames],
    )


@awsglue_domain("awsglue")
def test_dest_plugin_write_out_creates_the_table(
    domain_awsglue: AwsGlueDomain, tmp_path: Path
):
    catalog = store(domain_awsglue)
    table = "td_dest_create"
    catalog.drop_table(table)
    try:
        _write(domain_awsglue, work_dir(tmp_path), [table], [_orders(1, 2, 3)])
        assert_frames_match(catalog.read_table(table), _orders(1, 2, 3))
        assert catalog.column_types(table) == {"id": "long", "name": "string"}
    finally:
        catalog.drop_table(table)


@awsglue_domain("awsglue")
def test_dest_plugin_write_out_registers_an_iceberg_table_under_the_base_path(
    domain_awsglue: AwsGlueDomain, tmp_path: Path
):
    # What Athena needs to read the table: the Glue entry marked as Iceberg and
    # pointing at the metadata the write committed.
    catalog = store(domain_awsglue)
    table = "td_dest_registered"
    catalog.drop_table(table)
    conn = dest_conn(domain_awsglue)
    try:
        _write(domain_awsglue, work_dir(tmp_path), [table], [_orders(1)], trx="t1")
        parameters = catalog.glue_parameters(table)
        assert parameters["table_type"].upper() == "ICEBERG"
        assert parameters["metadata_location"].endswith(".metadata.json")
        location = (
            f"s3://{domain_awsglue.bucket}{conn._base_path()}"
            f"/{domain_awsglue.glue_database}/{table}"
        )
        assert catalog.table(table).location() == location
        assert catalog.data_files(table) == [f"{location}/data/tabsdata-t1.parquet"]
    finally:
        catalog.drop_table(table)


@awsglue_domain("awsglue")
def test_dest_plugin_write_out_append_adds_rows(
    domain_awsglue: AwsGlueDomain, tmp_path: Path
):
    catalog = store(domain_awsglue)
    table = "td_dest_append"
    work = work_dir(tmp_path)
    catalog.drop_table(table)
    try:
        _write(domain_awsglue, work, [table], [_orders(1, 2)])
        _write(domain_awsglue, work, [table], [_orders(3)])
        assert_frames_match(catalog.read_table(table), _orders(1, 2, 3))
    finally:
        catalog.drop_table(table)


@awsglue_domain("awsglue")
def test_dest_plugin_write_out_replace_clears_existing(
    domain_awsglue: AwsGlueDomain, tmp_path: Path
):
    catalog = store(domain_awsglue)
    table = "td_dest_replace"
    work = work_dir(tmp_path)
    catalog.drop_table(table)
    try:
        for mode in ("replace", "append"):
            _write(domain_awsglue, work, [table], [_orders(1, 2)], if_table_exists=mode)
        assert catalog.count(table) == 4
        _write(domain_awsglue, work, [table], [_orders(3)], if_table_exists="replace")
        assert_frames_match(catalog.read_table(table), _orders(3))
        assert catalog.column_types(table) == {"id": "long", "name": "string"}
    finally:
        catalog.drop_table(table)


@awsglue_domain("awsglue")
def test_dest_plugin_write_out_replace_with_an_empty_batch_empties_the_table(
    domain_awsglue: AwsGlueDomain, tmp_path: Path
):
    catalog = store(domain_awsglue)
    table = "td_dest_replace_empty"
    work = work_dir(tmp_path)
    catalog.drop_table(table)
    try:
        _write(domain_awsglue, work, [table], [_orders(1, 2)])
        _write(domain_awsglue, work, [table], [_orders()], if_table_exists="replace")
        assert catalog.count(table) == 0
    finally:
        catalog.drop_table(table)


@pytest.mark.parametrize("mode", ["append", "replace"])
@awsglue_domain("awsglue")
def test_dest_plugin_write_out_rerun_of_a_trx_writes_nothing(
    domain_awsglue: AwsGlueDomain, tmp_path: Path, mode: str
):
    catalog = store(domain_awsglue)
    table = f"td_dest_rerun_{mode}"
    work = work_dir(tmp_path)
    catalog.drop_table(table)
    try:
        _write(domain_awsglue, work, [table], [_orders(1)], trx="t1")
        _write(
            domain_awsglue, work, [table], [_orders(2)], trx="t2", if_table_exists=mode
        )
        before = (catalog.read_table(table).sort("id"), catalog.snapshots(table))
        _write(
            domain_awsglue, work, [table], [_orders(2)], trx="t2", if_table_exists=mode
        )
        assert catalog.read_table(table).sort("id").equals(before[0])
        assert catalog.snapshots(table) == before[1]
        assert {trx for _, trx in before[1]} == {"t1", "t2"}
    finally:
        catalog.drop_table(table)


@awsglue_domain("awsglue")
def test_dest_plugin_write_out_writes_each_table_of_the_destination(
    domain_awsglue: AwsGlueDomain, tmp_path: Path
):
    # One bare name, completed from the connection, and one that names the
    # database itself.
    catalog = store(domain_awsglue)
    bare, qualified = "td_dest_multi_a", "td_dest_multi_b"
    for table in (bare, qualified):
        catalog.drop_table(table)
    try:
        _write(
            domain_awsglue,
            work_dir(tmp_path),
            [bare, f"{domain_awsglue.glue_database}.{qualified}"],
            [_orders(1), _orders(2, 3)],
        )
        assert_frames_match(catalog.read_table(bare), _orders(1))
        assert_frames_match(catalog.read_table(qualified), _orders(2, 3))
    finally:
        for table in (bare, qualified):
            catalog.drop_table(table)


@awsglue_domain("awsglue")
def test_dest_plugin_write_out_appends_to_a_table_another_writer_created(
    domain_awsglue: AwsGlueDomain, tmp_path: Path
):
    # The table is located where the catalog puts it, not under the base path
    # of the connection, and already holds files written with field ids.
    catalog = store(domain_awsglue)
    table = "td_dest_foreign"
    try:
        catalog.create_table(table, _orders(1).to_arrow())
        _write(domain_awsglue, work_dir(tmp_path), [table], [_orders(2)], trx="t1")
        assert_frames_match(catalog.read_table(table), _orders(1, 2))
        location = catalog.table(table).location()
        assert f"{location}/data/tabsdata-t1.parquet" in catalog.data_files(table)
    finally:
        catalog.drop_table(table)


@awsglue_domain("awsglue")
def test_dest_plugin_write_out_refuses_a_partitioned_table(
    domain_awsglue: AwsGlueDomain, tmp_path: Path
):
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import LongType, NestedField, StringType

    catalog = store(domain_awsglue)
    table = "td_dest_partitioned"
    catalog.drop_table(table)
    catalog._catalog.create_table(
        (domain_awsglue.glue_database, table),
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "name", StringType(), required=False),
        ),
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=2, field_id=1000, transform=IdentityTransform(), name="name"
            )
        ),
    )
    try:
        with pytest.raises(AwsGlueRuntimeException) as raised:
            _write(domain_awsglue, work_dir(tmp_path), [table], [_orders(1)])
        assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_11
        assert catalog.count(table) == 0
    finally:
        catalog.drop_table(table)


@awsglue_domain("awsglue")
def test_dest_plugin_write_out_refuses_a_database_that_does_not_exist(
    domain_awsglue: AwsGlueDomain, tmp_path: Path
):
    with pytest.raises(AwsGlueRuntimeException) as raised:
        _write(
            domain_awsglue,
            work_dir(tmp_path),
            ["td_no_such_database.td_dest_orphan"],
            [_orders(1)],
        )
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_9


@awsglue_domain("awsglue")
def test_dest_plugin_write_out_refuses_a_bare_table_without_a_default_database(
    domain_awsglue: AwsGlueDomain, tmp_path: Path
):
    catalog = store(domain_awsglue)
    table = "td_dest_no_default"
    with pytest.raises(AwsGlueRuntimeException) as raised:
        _write(
            domain_awsglue,
            work_dir(tmp_path),
            [table],
            [_orders(1)],
            conn=dest_conn(domain_awsglue, database=None),
        )
    assert raised.value.api_error is AwsGlueErrorCode.AWSGLUE_8
    assert not catalog.exists(table)


@awsglue_domain("awsglue")
def test_dest_plugin_write_out_stores_the_types_iceberg_lacks_in_the_ones_it_has(
    domain_awsglue: AwsGlueDomain, tmp_path: Path
):
    catalog = store(domain_awsglue)
    table = "td_dest_types"
    catalog.drop_table(table)
    madrid = dt.datetime(2026, 7, 1, 12, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    frame = pl.DataFrame({
        "tiny": pl.Series([1], dtype=pl.Int8),
        "unsigned": pl.Series([4_000_000_000], dtype=pl.UInt32),
        "category": pl.Series(["a"], dtype=pl.Categorical),
        "nanos": pl.Series([dt.datetime(2026, 7, 1, 12)], dtype=pl.Datetime("ns")),
        "zoned": pl.Series([madrid]).dt.convert_time_zone("Europe/Madrid"),
        "tags": pl.Series([[1, 2]], dtype=pl.List(pl.UInt16)),
    })
    try:
        _write(domain_awsglue, work_dir(tmp_path), [table], [frame])
        assert catalog.column_types(table) == {
            "tiny": "int",
            "unsigned": "long",
            "category": "string",
            "nanos": "timestamp",
            "zoned": "timestamptz",
            "tags": "list<int>",
        }
        (row,) = catalog.read_table(table).rows(named=True)
        assert row["unsigned"] == 4_000_000_000
        assert row["category"] == "a"
        assert row["zoned"] == dt.datetime(2026, 7, 1, 10, tzinfo=dt.timezone.utc)
        assert row["tags"] == [1, 2]
    finally:
        catalog.drop_table(table)


@awsglue_domain("awsglue")
def test_dest_plugin_write_out_forwards_the_client_settings_of_the_connection(
    domain_awsglue: AwsGlueDomain, tmp_path: Path
):
    # A setting each client accepts reaches it; one it rejects would fail the
    # write.
    catalog = store(domain_awsglue)
    table = "td_dest_client_cfg"
    catalog.drop_table(table)
    conn = dest_conn(
        domain_awsglue,
        conn_cfg={
            "pyiceberg.glue.max-retries": 5,
            "pyiceberg.glue.retry-mode": "standard",
            "obstore.virtual_hosted_style_request": False,
        },
    )
    try:
        _write(domain_awsglue, work_dir(tmp_path), [table], [_orders(1)], conn=conn)
        assert_frames_match(catalog.read_table(table), _orders(1))
    finally:
        catalog.drop_table(table)

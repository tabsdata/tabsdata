#
# Copyright 2026 Tabsdata Inc.
#

"""ClickHouse iceberg schema-evolution coverage on write"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.clickhouse import (
    ClickhouseDomain,
    clickhouse_domain,
)
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.clickhouse.backend import dest_conn, store
from testi_tabsdatak.conn.common.sql.employees_model import DEPARTMENTS
from testi_tabsdatak.conn.common.sql.plugin_io import provisioned_parquet, work_dir

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse import ClickHouseDest, _plugin as plugin
from tabsdatak.conn.clickhouse.error import (
    ClickHouseErrorCode,
    ClickHouseRuntimeException,
)

pytestmark = pytest.mark.clickhouse

# S3 staging only: the delta is computed and rendered dialect-side, before any
# cloud call, so running the same cases against the other providers would only
# re-check cloud IO.


def _write(conn, work: Path, parquet: Path, table: str, **dest_kwargs) -> None:
    plugin._ClickHouseDestPlugin().write_out(
        PluginDestCtx(work),
        conn,
        ClickHouseDest(tables=[table], **dest_kwargs),
        [pl.scan_parquet(parquet)],
    )


def _parquet(path: Path, frame: pl.DataFrame) -> Path:
    frame.write_parquet(path, use_pyarrow=True)
    return path


@fs_domain("fs_employees_xs")
@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_iceberg_adds_nullable_column(
    domain_fs_employees_xs: FsDomain,
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
):
    db = store(domain_clickhouse)
    table = "td_iceberg_add_column"
    work = work_dir(tmp_path)
    seed = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(seed)
    evolved = _parquet(
        tmp_path / "evolved.parquet", rows.with_columns(pl.lit("emea").alias("region"))
    )
    conn = dest_conn(domain_clickhouse, domain_s3)
    try:
        _write(conn, work, seed, table, if_table_exists="replace")
        _write(conn, work, evolved, table, schema_evolution="iceberg")

        columns = db.column_types(table)
        assert columns["region"].startswith("Nullable(")
        assert db.count(table) == 2 * rows.height
    finally:
        db.drop_table(table)


@fs_domain("fs_employees_xs")
@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_iceberg_drops_column(
    domain_fs_employees_xs: FsDomain,
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
):
    db = store(domain_clickhouse)
    table = "td_iceberg_drop_column"
    work = work_dir(tmp_path)
    seed = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(seed)
    evolved = _parquet(tmp_path / "evolved.parquet", rows.drop("dept_name"))
    conn = dest_conn(domain_clickhouse, domain_s3)
    try:
        _write(conn, work, seed, table, if_table_exists="replace")
        _write(conn, work, evolved, table, schema_evolution="iceberg")

        columns = db.column_types(table)
        assert "dept_name" not in columns
        assert "dept_no" in columns
        assert db.count(table) == 2 * rows.height
    finally:
        db.drop_table(table)


@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_iceberg_widens_int_type(
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
):
    # A widening case pins the column's dtype on both sides, so its input is
    # built here rather than taken from the provisioned parquet.
    db = store(domain_clickhouse)
    table = "td_iceberg_widen_int"
    work = work_dir(tmp_path)
    seed = _parquet(
        tmp_path / "seed.parquet", pl.DataFrame({"id": [1]}, schema={"id": pl.Int32})
    )
    evolved = _parquet(
        tmp_path / "evolved.parquet", pl.DataFrame({"id": [2]}, schema={"id": pl.Int64})
    )
    conn = dest_conn(domain_clickhouse, domain_s3)
    try:
        _write(conn, work, seed, table, if_table_exists="replace")
        _write(conn, work, evolved, table, schema_evolution="iceberg")

        # ClickHouse may wrap the column in Nullable(), so match the inner token.
        assert "Int64" in db.column_types(table)["id"]
        assert db.count(table) == 2
    finally:
        db.drop_table(table)


@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_iceberg_rejects_unsafe_narrowing(
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
):
    db = store(domain_clickhouse)
    table = "td_iceberg_reject_narrowing"
    work = work_dir(tmp_path)
    seed = _parquet(
        tmp_path / "seed.parquet", pl.DataFrame({"id": [1]}, schema={"id": pl.Int64})
    )
    narrowed = _parquet(
        tmp_path / "narrowed.parquet",
        pl.DataFrame({"id": [2]}, schema={"id": pl.Int32}),
    )
    conn = dest_conn(domain_clickhouse, domain_s3)
    try:
        _write(conn, work, seed, table, if_table_exists="replace")
        with pytest.raises(ClickHouseRuntimeException) as raised:
            _write(conn, work, narrowed, table, schema_evolution="iceberg")
        assert raised.value.api_error is ClickHouseErrorCode.CLICKHOUSE_6

        assert "Int64" in db.column_types(table)["id"]
        assert db.count(table) == 1
    finally:
        db.drop_table(table)


@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_iceberg_applies_combined_delta(
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
):
    db = store(domain_clickhouse)
    table = "td_iceberg_combined"
    work = work_dir(tmp_path)
    seed = _parquet(
        tmp_path / "seed.parquet",
        pl.DataFrame(
            {"id": [1], "name": ["alice"], "old_col": ["value"]},
            schema={"id": pl.Int32, "name": pl.String, "old_col": pl.String},
        ),
    )
    evolved = _parquet(
        tmp_path / "evolved.parquet",
        pl.DataFrame(
            {"id": [2], "name": ["bob"], "category": ["staff"]},
            schema={"id": pl.Int64, "name": pl.String, "category": pl.String},
        ),
    )
    conn = dest_conn(domain_clickhouse, domain_s3)
    try:
        _write(conn, work, seed, table, if_table_exists="replace")
        _write(conn, work, evolved, table, schema_evolution="iceberg")

        columns = db.column_types(table)
        assert "old_col" not in columns
        assert "Int64" in columns["id"]
        assert columns["category"].startswith("Nullable(")
        assert db.count(table) == 2
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("from_insertion_table", [2, 0])
@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_columns_land_by_name_whatever_their_order(
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
    monkeypatch,
    from_insertion_table: int,
):
    # With the setting at 0 the table function no longer takes the target's
    # structure, so only naming the columns keeps each value in its own column.
    import clickhouse_connect

    original = clickhouse_connect.get_client

    def client(**kwargs):
        setting = "use_structure_from_insertion_table_in_table_functions"
        return original(**kwargs, settings={setting: from_insertion_table})

    monkeypatch.setattr(clickhouse_connect, "get_client", client)
    db = store(domain_clickhouse)
    table = f"td_iceberg_column_order_{from_insertion_table}"
    work = work_dir(tmp_path)
    seed = _parquet(
        tmp_path / "seed.parquet",
        pl.DataFrame({"id": [1], "name": ["one"], "score": [10]}),
    )
    reordered = _parquet(
        tmp_path / "reordered.parquet",
        pl.DataFrame({"score": [20], "name": ["two"], "id": [2]}),
    )
    conn = dest_conn(domain_clickhouse, domain_s3)
    try:
        _write(conn, work, seed, table, if_table_exists="replace")
        _write(conn, work, reordered, table, schema_evolution="iceberg")

        got = db.read_table(table).sort("id")
        assert got.select("id", "name", "score").rows() == [
            (1, "one", 10),
            (2, "two", 20),
        ]
    finally:
        db.drop_table(table)


@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_a_backtick_in_a_column_name_creates_and_evolves(
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
):
    db = store(domain_clickhouse)
    table = "td_iceberg_backtick_column"
    work = work_dir(tmp_path)
    seed = _parquet(tmp_path / "seed.parquet", pl.DataFrame({"we`ird": [1]}))
    evolved = _parquet(
        tmp_path / "evolved.parquet", pl.DataFrame({"we`ird": [2], "ne`w": ["x"]})
    )
    conn = dest_conn(domain_clickhouse, domain_s3)
    try:
        _write(conn, work, seed, table, if_table_exists="replace")
        _write(conn, work, evolved, table, schema_evolution="iceberg")
        got = db.read_table(table).sort("we`ird")
        assert got.columns == ["we`ird", "ne`w"]
        assert got.rows() == [(1, None), (2, "x")]
    finally:
        db.drop_table(table)

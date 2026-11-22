#
# Copyright 2026 Tabsdata Inc.
#

"""``if_table_exists="replace"`` against a live ClickHouse: the batch loads
into a copy of each table, swapped in only once every table loaded."""

from __future__ import annotations

import datetime as dt
import uuid
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.clickhouse import (
    ClickhouseDomain,
    clickhouse_domain,
)
from tabsdatax.itest.fixture.backends.s3 import S3Domain, s3_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from testi_tabsdatak.conn.clickhouse.backend import ClickHouseStore, dest_conn, store
from testi_tabsdatak.conn.common.sql.plugin_io import work_dir

# noinspection PyProtectedMember
from tabsdatak.conn.clickhouse import ClickHouseDest, _plugin as plugin
from tabsdatak.conn.clickhouse.error import (
    ClickHouseErrorCode,
    ClickHouseRuntimeException,
)

pytestmark = pytest.mark.clickhouse

_SWAP = "__td_swap"


def _replace(
    domain: ClickhouseDomain,
    staging: S3Domain,
    tmp_path: Path,
    frames: dict[str, pl.LazyFrame],
) -> None:
    plugin._ClickHouseDestPlugin().write_out(
        PluginDestCtx(work_dir(tmp_path)),
        dest_conn(domain, staging),
        ClickHouseDest(tables=list(frames), if_table_exists="replace"),
        list(frames.values()),
    )


def _prefix() -> str:
    # Workers share the domain's database, so every test names its own tables.
    return f"td_dest_swap_{uuid.uuid4().hex[:8]}"


def _drop(db: ClickHouseStore, prefix: str) -> None:
    for table in db.table_names(prefix):
        db.drop_table(table)


@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_replace_keeps_the_table_definition_and_only_the_batch(
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
):
    db = store(domain_clickhouse)
    table = _prefix()
    db.create_table(
        table,
        "(id Int64, day Date32, v String) ENGINE = ReplacingMergeTree"
        " PARTITION BY toYYYYMM(day) ORDER BY id"
        " SETTINGS index_granularity = 1024",
    )
    db.insert(table, [{"id": 999, "day": dt.date(2000, 1, 1), "v": "old"}])
    definition = db.show_create(table)
    # 150 months: more partitions than ClickHouse's default of 100 per insert.
    months = [dt.date(2010 + i // 12, i % 12 + 1, 1) for i in range(150)]
    batch = pl.LazyFrame(
        {"id": list(range(150)), "day": months, "v": ["new"] * 150},
        schema={"id": pl.Int64, "day": pl.Date, "v": pl.String},
    )
    try:
        _replace(domain_clickhouse, domain_s3, tmp_path, {table: batch})
        written = db.read_table(table)
        assert written.height == 150
        assert set(written["v"]) == {"new"}
        assert db.show_create(table) == definition
        assert db.table_names(table) == [table]
    finally:
        _drop(db, table)


@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_a_failed_replace_leaves_every_table_as_it_was(
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
):
    db = store(domain_clickhouse)
    prefix = _prefix()
    loads, refuses = f"{prefix}_loads", f"{prefix}_refuses"
    db.create_table(loads, "(id Int64, v String) ENGINE = MergeTree ORDER BY id")
    # The constraint is copied with the table, so the batch is refused only
    # when it is inserted, after the first table loaded.
    db.create_table(
        refuses,
        "(id Int64, v String, CONSTRAINT small CHECK id < 100)"
        " ENGINE = MergeTree ORDER BY id",
    )
    for table in (loads, refuses):
        db.insert(table, [{"id": 1, "v": "old"}])
    try:
        with pytest.raises(ClickHouseRuntimeException) as ei:
            _replace(
                domain_clickhouse,
                domain_s3,
                tmp_path,
                {
                    loads: pl.LazyFrame({"id": [2, 3], "v": ["new", "new"]}),
                    refuses: pl.LazyFrame({"id": [500], "v": ["new"]}),
                },
            )
        assert ei.value.api_error is ClickHouseErrorCode.CLICKHOUSE_6
        for table in (loads, refuses):
            assert db.read_table(table).to_dicts() == [{"id": 1, "v": "old"}]
        assert db.table_names(prefix) == [loads, refuses]
    finally:
        _drop(db, prefix)


@clickhouse_domain("clickhouse")
@s3_domain("s3")
def test_replace_drops_a_swap_table_a_failed_run_left(
    domain_clickhouse: ClickhouseDomain,
    domain_s3: S3Domain,
    tmp_path: Path,
):
    db = store(domain_clickhouse)
    table = _prefix()
    db.create_table(table, "(id Int64) ENGINE = MergeTree ORDER BY id")
    db.create_table(f"{table}{_SWAP}", "(other String) ENGINE = Memory")
    try:
        _replace(
            domain_clickhouse,
            domain_s3,
            tmp_path,
            {table: pl.LazyFrame({"id": [7]})},
        )
        assert db.read_table(table).to_dicts() == [{"id": 7}]
        assert db.table_names(table) == [table]
    finally:
        _drop(db, table)

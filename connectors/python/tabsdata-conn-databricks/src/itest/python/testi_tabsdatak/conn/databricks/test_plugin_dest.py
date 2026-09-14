#
# Copyright 2026 Tabsdata Inc.
#

"""Databricks destination-plugin integration tests using fake context"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.databricks import (
    DatabricksDomain,
    databricks_domain,
)
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from tabsdatax.itest.fixture.functions.harness import assert_frames_match
from testi_tabsdatak.conn.common.sql.employees_model import DEPARTMENTS, DEPT_MANAGER
from testi_tabsdatak.conn.common.sql.plugin_io import provisioned_parquet, work_dir
from testi_tabsdatak.conn.databricks.backend import RESCUED_DATA, dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.databricks import DatabricksDest, MergeCfg, _plugin as plugin

pytestmark = pytest.mark.databricks


@fs_domain("fs_employees_xs")
@databricks_domain("databricks")
def test_dest_plugin_write_out_appends_rows(
    domain_fs_employees_xs: FsDomain,
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
):
    db = store(domain_databricks)
    table = "td_dest_append"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(domain_databricks)
    try:
        plugin._DatabricksDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            DatabricksDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        assert db.read_table(table).height == rows

        plugin._DatabricksDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            DatabricksDest(tables=[table], if_table_exists="append"),
            [parquet],
        )
        assert db.read_table(table).height == 2 * rows
    finally:
        db.drop_table(table)


@fs_domain("fs_employees_xs")
@databricks_domain("databricks")
def test_dest_plugin_write_out_replace_clears_existing(
    domain_fs_employees_xs: FsDomain,
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
):
    # Replacing with a different entity: the second write must leave only its own
    # rows and columns behind, so neither the first load's rows nor its schema
    # survive.
    db = store(domain_databricks)
    table = "td_dest_replace"
    work = work_dir(tmp_path)
    first = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    second = provisioned_parquet(domain_fs_employees_xs, DEPT_MANAGER)
    expected = pl.read_parquet(second)
    conn = dest_conn(domain_databricks)
    try:
        plugin._DatabricksDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            DatabricksDest(tables=[table], if_table_exists="replace"),
            [first],
        )
        assert db.read_table(table).height == pl.read_parquet(first).height

        plugin._DatabricksDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            DatabricksDest(tables=[table], if_table_exists="replace"),
            [second],
        )
        got = db.read_table(table)
        assert got.height == expected.height
        assert set(got.columns) - {RESCUED_DATA} == set(expected.columns)
    finally:
        db.drop_table(table)


# ---------------------------------------------------------------------------
# if_table_exists="merge"
#
# Merge needs rows that update, delete and insert against a known prior state,
# so these tests build their own batches instead of taking a provisioned file --
# no fs domain, only the Databricks one they write into. Every batch is loaded
# through the plugin, so the first run also covers the create-then-merge path
# where the target does not exist yet and every row is NOT MATCHED BY TARGET.
#
# The batch is coalesced before it is uploaded, so a key carrying several changes
# in one file reaches the MERGE once -- Databricks rejects a non-deterministic
# match outright.
# ---------------------------------------------------------------------------


def _batch(path: Path, columns: dict[str, list | pl.Series]) -> Path:
    """A batch parquet. A column given as a `Series` pins its dtype, which the
    type-widening test needs."""
    pl.DataFrame(columns).write_parquet(path)
    return path


def _types(db, table: str) -> dict[str, str]:
    """The target's column types, less the schema-rescue column ``read_files``
    contributes to every table the write path creates."""
    return {
        name: type_
        for name, type_ in db.column_types(table).items()
        if name != RESCUED_DATA
    }


def _merge_out(
    conn,
    work: Path,
    table: str,
    parquet: Path,
    merge_cfg: MergeCfg,
    schema_evolution: str = "strict",
    trx_id: str | None = None,
) -> None:
    plugin._DatabricksDestPlugin().write_out(
        PluginDestCtx(work),
        conn,
        DatabricksDest(
            tables=[table],
            if_table_exists="merge",
            merge_cfg=merge_cfg,
            schema_evolution=schema_evolution,
        ),
        [parquet],
    )


@databricks_domain("databricks")
def test_dest_plugin_write_out_merge_inserts_updates_and_deletes(
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
):
    db = store(domain_databricks)
    table = "td_dest_merge_cdc"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_databricks)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {
            "order_id": [1, 2, 3],
            "status": ["new", "new", "new"],
            "amount": [10, 20, 30],
            "op": ["i", "i", "i"],
        },
    )
    # Order 2 advances, order 3 is tombstoned, order 4 has never been seen.
    delta = _batch(
        tmp_path / "delta.parquet",
        {
            "order_id": [2, 3, 4],
            "status": ["shipped", "new", "new"],
            "amount": [25, 30, 40],
            "op": ["u", "d", "i"],
        },
    )
    try:
        _merge_out(conn, work, table, seed, merge_cfg)
        assert db.read_table(table).height == 3

        _merge_out(conn, work, table, delta, merge_cfg)
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1, 2, 4],
                "status": ["new", "shipped", "new"],
                "amount": [10, 25, 40],
            }),
        )
    finally:
        db.drop_table(table)


@databricks_domain("databricks")
def test_dest_plugin_write_out_merge_coalesces_several_changes_for_one_key(
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
):
    # One batch, several changes per key. MERGE matches each incoming row at most
    # once, so without the coalesce Databricks rejects the whole statement for a
    # non-deterministic match. Each key resolves to its net effect: order 1 is
    # born and advances, order 2 pre-existed and ends deleted, order 3 is born and
    # gone inside the batch so it never reaches the target at all.
    db = store(domain_databricks)
    table = "td_dest_merge_coalesce"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_databricks)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [2], "status": ["new"], "op": ["i"]},
    )
    churn = _batch(
        tmp_path / "churn.parquet",
        {
            "order_id": [1, 1, 1, 2, 2, 3, 3],
            "status": [
                "new",
                "packed",
                "shipped",
                "packed",
                "cancelled",
                "new",
                "cancelled",
            ],
            "op": ["i", "u", "u", "u", "d", "i", "d"],
        },
    )
    try:
        _merge_out(conn, work, table, seed, merge_cfg)
        _merge_out(conn, work, table, churn, merge_cfg)
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({"order_id": [1], "status": ["shipped"]}),
        )
        assert db.read_table(table).height == 1
    finally:
        db.drop_table(table)


@databricks_domain("databricks")
def test_dest_plugin_write_out_merge_reads_a_marker_in_either_case(
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
):
    # The marker is lower-cased before it is compared, so the upper case a CDC
    # feed usually emits is claimed by the same clauses. A marker that is neither
    # leaves the batch during the coalesce, so the third batch stages no rows and
    # the target is untouched.
    db = store(domain_databricks)
    table = "td_dest_merge_marker_case"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_databricks)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    lower = _batch(
        tmp_path / "lower.parquet",
        {"order_id": [1], "status": ["new"], "op": ["i"]},
    )
    upper = _batch(
        tmp_path / "upper.parquet",
        {"order_id": [1, 2], "status": ["shipped", "new"], "op": ["U", "I"]},
    )
    unknown = _batch(
        tmp_path / "unknown.parquet",
        {"order_id": [3], "status": ["new"], "op": ["x"]},
    )
    try:
        _merge_out(conn, work, table, lower, merge_cfg)
        _merge_out(conn, work, table, upper, merge_cfg)
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({"order_id": [1, 2], "status": ["shipped", "new"]}),
        )

        _merge_out(conn, work, table, unknown, merge_cfg)
        assert db.read_table(table).height == 2
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("schema_evolution", ["update", "strict"])
@databricks_domain("databricks")
def test_dest_plugin_write_out_merge_keeps_the_marker_out_of_the_target(
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
    schema_evolution: str,
):
    # The marker says what to do with a row and is never part of the row. The
    # target is created without it and the write clauses never assign it, so it
    # cannot arrive later either -- not even while the schema is evolving.
    db = store(domain_databricks)
    table = f"td_dest_merge_no_marker_{schema_evolution}"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_databricks)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1], "status": ["new"], "op": ["i"]},
    )
    delta = _batch(
        tmp_path / "delta.parquet",
        {"order_id": [1, 2], "status": ["shipped", "new"], "op": ["u", "i"]},
    )
    try:
        _merge_out(conn, work, table, seed, merge_cfg, schema_evolution)
        assert "op" not in db.read_table(table).columns

        _merge_out(conn, work, table, delta, merge_cfg, schema_evolution)
        got = db.read_table(table)
        assert "op" not in got.columns
        assert set(got.columns) - {RESCUED_DATA} == {"order_id", "status"}
    finally:
        db.drop_table(table)


# ---------------------------------------------------------------------------
# if_table_exists="merge" + schema_evolution
#
# "update" puts WITH SCHEMA EVOLUTION on the MERGE and marks the target for type
# widening; "strict" does neither, so the target's schema is what the first write
# made it.
# ---------------------------------------------------------------------------


@databricks_domain("databricks")
def test_dest_plugin_write_out_merge_takes_a_new_column_on_update(
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
):
    db = store(domain_databricks)
    table = "td_dest_merge_evolve_column"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_databricks)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1], "status": ["new"], "op": ["i"]},
    )
    # A second batch carrying a column the target does not have yet.
    widened = _batch(
        tmp_path / "widened.parquet",
        {"order_id": [1], "status": ["shipped"], "carrier": ["ups"], "op": ["u"]},
    )
    try:
        _merge_out(conn, work, table, seed, merge_cfg, "update")
        assert "carrier" not in db.read_table(table).columns

        _merge_out(conn, work, table, widened, merge_cfg, "update")
        got = db.read_table(table)
        assert "carrier" in got.columns
        assert_frames_match(
            got,
            pl.DataFrame({
                "order_id": [1],
                "status": ["shipped"],
                "carrier": ["ups"],
            }),
        )
    finally:
        db.drop_table(table)


@databricks_domain("databricks")
def test_dest_plugin_write_out_merge_keeps_the_target_schema_on_strict(
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
):
    # The mirror of the evolving case: with no WITH SCHEMA EVOLUTION the wildcard
    # covers the target's own columns, so a batch carrying a new one merges its
    # shared columns and leaves the target's schema as the first write made it.
    db = store(domain_databricks)
    table = "td_dest_merge_strict_column"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_databricks)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1], "status": ["new"], "op": ["i"]},
    )
    widened = _batch(
        tmp_path / "widened.parquet",
        {"order_id": [1], "status": ["shipped"], "carrier": ["ups"], "op": ["u"]},
    )
    try:
        _merge_out(conn, work, table, seed, merge_cfg, "strict")
        before = set(db.read_table(table).columns)

        _merge_out(conn, work, table, widened, merge_cfg, "strict")
        got = db.read_table(table)
        assert "carrier" not in got.columns
        assert set(got.columns) == before
        # The columns both sides share still merge.
        assert_frames_match(got, pl.DataFrame({"order_id": [1], "status": ["shipped"]}))
    finally:
        db.drop_table(table)


@databricks_domain("databricks")
def test_dest_plugin_write_out_merge_stops_evolving_when_switched_back_to_strict(
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
):
    # The evolution mark is set on every merge, not only when evolving, so a
    # target an earlier "update" run marked stops taking new columns as soon as a
    # run asks for "strict".
    db = store(domain_databricks)
    table = "td_dest_merge_evolution_off"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_databricks)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1], "status": ["new"], "op": ["i"]},
    )
    first = _batch(
        tmp_path / "first.parquet",
        {"order_id": [1], "status": ["packed"], "carrier": ["ups"], "op": ["u"]},
    )
    second = _batch(
        tmp_path / "second.parquet",
        {
            "order_id": [1],
            "status": ["shipped"],
            "carrier": ["ups"],
            "eta": ["monday"],
            "op": ["u"],
        },
    )
    try:
        _merge_out(conn, work, table, seed, merge_cfg, "update")
        _merge_out(conn, work, table, first, merge_cfg, "update")
        assert "carrier" in db.read_table(table).columns

        _merge_out(conn, work, table, second, merge_cfg, "strict")
        got = db.read_table(table)
        assert "eta" not in got.columns
        assert_frames_match(got, pl.DataFrame({"order_id": [1], "status": ["shipped"]}))
    finally:
        db.drop_table(table)


@databricks_domain("databricks")
def test_dest_plugin_write_out_merge_widens_a_column_type_on_update(
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
):
    # ``delta.enableTypeWidening`` is set from schema_evolution, so an "update"
    # run can land a value the target's original column type could not hold:
    # Databricks widens the column itself, and only that column.
    db = store(domain_databricks)
    table = "td_dest_merge_widen_type"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_databricks)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    narrow = _batch(
        tmp_path / "narrow.parquet",
        {
            "order_id": [1],
            "amount": pl.Series([10], dtype=pl.Int32),
            "op": ["i"],
        },
    )
    # Beyond Int32: it only lands if the target's column widened.
    wide = _batch(
        tmp_path / "wide.parquet",
        {
            "order_id": [1],
            "amount": pl.Series([3_000_000_000], dtype=pl.Int64),
            "op": ["u"],
        },
    )
    try:
        _merge_out(conn, work, table, narrow, merge_cfg, "update")
        assert _types(db, table) == {"order_id": "bigint", "amount": "int"}

        _merge_out(conn, work, table, wide, merge_cfg, "update")
        assert _types(db, table) == {"order_id": "bigint", "amount": "bigint"}
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({"order_id": [1], "amount": [3_000_000_000]}),
        )
    finally:
        db.drop_table(table)


@databricks_domain("databricks")
def test_dest_plugin_write_out_merge_widens_a_float_to_a_double(
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
):
    # Widening is not only about integer width: a float column takes a double
    # batch the same way. Both values are exact in binary, so the read-back
    # compares equal whichever width the column ends up holding them at, and the
    # test turns on the type rather than on float formatting.
    db = store(domain_databricks)
    table = "td_dest_merge_widen_float"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_databricks)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    narrow = _batch(
        tmp_path / "narrow_float.parquet",
        {
            "order_id": [1],
            "rate": pl.Series([1.5], dtype=pl.Float32),
            "op": ["i"],
        },
    )
    wide = _batch(
        tmp_path / "wide_float.parquet",
        {
            "order_id": [1],
            "rate": pl.Series([2.25], dtype=pl.Float64),
            "op": ["u"],
        },
    )
    try:
        _merge_out(conn, work, table, narrow, merge_cfg, "update")
        assert _types(db, table)["rate"] == "float"

        _merge_out(conn, work, table, wide, merge_cfg, "update")
        assert _types(db, table)["rate"] == "double"
        assert_frames_match(
            db.read_table(table), pl.DataFrame({"order_id": [1], "rate": [2.25]})
        )
    finally:
        db.drop_table(table)


@databricks_domain("databricks")
def test_dest_plugin_write_out_merge_does_not_narrow_a_widened_column_back(
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
):
    # Evolution only ever widens. Once a batch has taken the column to bigint, a
    # later batch that is narrow again merges into it as it stands rather than
    # dragging the target back to int and losing the value already stored there.
    db = store(domain_databricks)
    table = "td_dest_merge_widen_no_narrow"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_databricks)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    narrow = _batch(
        tmp_path / "first_narrow.parquet",
        {
            "order_id": [1],
            "amount": pl.Series([10], dtype=pl.Int32),
            "op": ["i"],
        },
    )
    wide = _batch(
        tmp_path / "then_wide.parquet",
        {
            "order_id": [2],
            "amount": pl.Series([3_000_000_000], dtype=pl.Int64),
            "op": ["i"],
        },
    )
    again = _batch(
        tmp_path / "narrow_again.parquet",
        {
            "order_id": [1],
            "amount": pl.Series([11], dtype=pl.Int32),
            "op": ["u"],
        },
    )
    try:
        _merge_out(conn, work, table, narrow, merge_cfg, "update")
        _merge_out(conn, work, table, wide, merge_cfg, "update")
        assert _types(db, table)["amount"] == "bigint"

        _merge_out(conn, work, table, again, merge_cfg, "update")
        assert _types(db, table)["amount"] == "bigint"
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({"order_id": [1, 2], "amount": [11, 3_000_000_000]}),
        )
    finally:
        db.drop_table(table)


@databricks_domain("databricks")
def test_dest_plugin_write_out_merge_keeps_the_narrow_type_on_strict(
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
):
    # The mirror of the widening case: "strict" sets
    # ``delta.enableTypeWidening=false``, so a wider batch is written into the
    # column as it stands instead of moving it. The value is small on purpose --
    # it fits an int, so what is under test is the column's type and not what
    # Databricks does with a value that overflows it.
    db = store(domain_databricks)
    table = "td_dest_merge_strict_type"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_databricks)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    narrow = _batch(
        tmp_path / "strict_narrow.parquet",
        {
            "order_id": [1],
            "amount": pl.Series([10], dtype=pl.Int32),
            "op": ["i"],
        },
    )
    wide = _batch(
        tmp_path / "strict_wide.parquet",
        {
            "order_id": [1],
            "amount": pl.Series([42], dtype=pl.Int64),
            "op": ["u"],
        },
    )
    try:
        _merge_out(conn, work, table, narrow, merge_cfg, "strict")
        assert _types(db, table)["amount"] == "int"

        _merge_out(conn, work, table, wide, merge_cfg, "strict")
        assert _types(db, table)["amount"] == "int"
        assert_frames_match(
            db.read_table(table), pl.DataFrame({"order_id": [1], "amount": [42]})
        )
    finally:
        db.drop_table(table)


@databricks_domain("databricks")
def test_dest_plugin_write_out_merge_applies_one_cfg_per_table(
    domain_databricks: DatabricksDomain,
    tmp_path: Path,
):
    # Two slots merged in one call, each by its own key -- the list of configs has
    # to stay paired with `tables`, or a table is matched on a column that belongs
    # to the other one.
    db = store(domain_databricks)
    orders, items = "td_dest_merge_orders", "td_dest_merge_items"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_databricks)
    dest = DatabricksDest(
        tables=[orders, items],
        if_table_exists="merge",
        merge_cfg=[
            MergeCfg(on=["order_id"], op_column="op"),
            MergeCfg(on=["item_id"], op_column="op"),
        ],
    )
    seeds = [
        _batch(
            tmp_path / "orders.parquet",
            {"order_id": [1], "status": ["new"], "op": ["i"]},
        ),
        _batch(
            tmp_path / "items.parquet",
            {"item_id": [7], "sku": ["abc"], "op": ["i"]},
        ),
    ]
    deltas = [
        _batch(
            tmp_path / "orders_delta.parquet",
            {"order_id": [1], "status": ["shipped"], "op": ["u"]},
        ),
        _batch(
            tmp_path / "items_delta.parquet",
            {"item_id": [7], "sku": ["xyz"], "op": ["u"]},
        ),
    ]
    try:
        plugin._DatabricksDestPlugin().write_out(PluginDestCtx(work), conn, dest, seeds)
        plugin._DatabricksDestPlugin().write_out(
            PluginDestCtx(work), conn, dest, deltas
        )
        assert_frames_match(
            db.read_table(orders),
            pl.DataFrame({"order_id": [1], "status": ["shipped"]}),
        )
        assert_frames_match(
            db.read_table(items), pl.DataFrame({"item_id": [7], "sku": ["xyz"]})
        )
    finally:
        db.drop_table(orders)
        db.drop_table(items)

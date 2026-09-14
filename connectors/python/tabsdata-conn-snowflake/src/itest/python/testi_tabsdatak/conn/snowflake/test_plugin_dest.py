#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake destination-plugin integration tests using fake context"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.backends.snowflake import SnowflakeDomain, snowflake_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from tabsdatax.itest.fixture.functions.harness import assert_frames_match
from testi_tabsdatak.conn.common.sql.employees_model import DEPARTMENTS
from testi_tabsdatak.conn.common.sql.plugin_io import provisioned_parquet, work_dir
from testi_tabsdatak.conn.snowflake.backend import dest_conn, store

# noinspection PyProtectedMember
from tabsdatak.conn.snowflake import MergeCfg, SnowflakeDest, _plugin as plugin
from tabsdatak.conn.snowflake.error import SnowflakeRuntimeException

pytestmark = pytest.mark.snowflake

# The connector double-quotes every part of a target name, so a name only lands
# on the objects tdx provisions when it is already spelled the way Snowflake
# stores an unquoted identifier: upper case. Hence the upper-case table names
# here and the upper-cased database / schema in the multi-part spellings.
_SPELLINGS = {
    "bare-table": "{table}",
    "schema.table": "{schema}.{table}",
    "database.schema.table": "{database}.{schema}.{table}",
    "quoted-parts": '"{database}"."{schema}"."{table}"',
}


def _spelling_params(prefix: str):
    for spelling, template in _SPELLINGS.items():
        table = prefix + spelling.replace(".", "_").replace("-", "_").upper()
        yield pytest.param(template, table, id=spelling)


@fs_domain("fs_employees_xs")
@snowflake_domain("snowflake")
def test_dest_plugin_write_out_appends_rows(
    domain_fs_employees_xs: FsDomain,
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    db = store(domain_snowflake)
    table = "TD_DEST_APPEND"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(domain_snowflake)
    try:
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        assert db.count(table) == rows

        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(tables=[table], if_table_exists="append"),
            [parquet],
        )
        assert db.count(table) == 2 * rows
    finally:
        db.drop_table(table)


@fs_domain("fs_employees_xs")
@snowflake_domain("snowflake")
def test_dest_plugin_write_out_replace_clears_existing(
    domain_fs_employees_xs: FsDomain,
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # Snowflake's replace is a TRUNCATE, so it clears the rows and keeps the
    # column set: the reload is the same entity and the assertion is on the row
    # count returning to a single load's worth.
    db = store(domain_snowflake)
    table = "TD_DEST_REPLACE"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    expected = pl.read_parquet(parquet)
    conn = dest_conn(domain_snowflake)
    try:
        for mode in ("replace", "append"):
            plugin.SnowflakeDestPlugin().write_out(
                PluginDestCtx(work),
                conn,
                SnowflakeDest(tables=[table], if_table_exists=mode),
                [parquet],
            )
        assert db.count(table) == 2 * expected.height

        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        got = db.read_table(table)
        assert got.height == expected.height
        assert set(got.columns) == set(expected.columns)
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("template, table", list(_spelling_params("TD_DEST_FQN_")))
@fs_domain("fs_employees_xs")
@snowflake_domain("snowflake")
def test_dest_plugin_write_out_resolves_table_name_spelling(
    domain_fs_employees_xs: FsDomain,
    domain_snowflake: SnowflakeDomain,
    template: str,
    table: str,
    tmp_path: Path,
):
    # The table is created under its bare name and then appended to through the
    # spelling under test, which passes only if both resolve to the same
    # physical table.
    db = store(domain_snowflake)
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(domain_snowflake)
    spelled = template.format(
        database=domain_snowflake.database.upper(),
        schema=domain_snowflake.schema.upper(),
        table=table,
    )
    try:
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(tables=[table], if_table_exists="replace"),
            [parquet],
        )
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(tables=[spelled], if_table_exists="append"),
            [parquet],
        )
        assert db.count(table) == 2 * rows
    finally:
        db.drop_table(table)


# ---------------------------------------------------------------------------
# if_table_exists="merge"
#
# Merge needs rows that update, delete and insert against a known prior state,
# so these tests build their own batches instead of taking a provisioned file --
# no fs domain, only the Snowflake one they write into. Every batch is loaded
# through the plugin, so the first run also covers the create-then-merge path
# where the target does not exist yet and every row is NOT MATCHED.
# ---------------------------------------------------------------------------


# A MERGE does not read ENABLE_SCHEMA_EVOLUTION, so the connector evolves its
# target with ALTER statements or not at all -- and the write has to say so.
_ICEBERG = {"snowflake.schema_evolution_engine": "iceberg"}


def _batch(path: Path, columns: dict[str, list | pl.Series]) -> Path:
    """A batch parquet. A column given as a `Series` pins its dtype, which the
    type-widening tests need -- a parquet decimal carries its precision and
    scale, and a timestamp its unit."""
    pl.DataFrame(columns).write_parquet(path)
    return path


class _TrxCtx(PluginDestCtx):
    """A plugin context whose trx id the test picks.

    The watermark turns on which trx a write belongs to, and the shared stub
    hard-codes one id for every context it fabricates -- so a test needs this to
    say "the same trx again" apart from "a later one".
    """

    def __init__(self, work: Path, trx_id: str) -> None:
        super().__init__(work)
        self._trx_id = trx_id

    @property
    def trx_id(self) -> str:
        return self._trx_id


def _merge_out(
    conn,
    work: Path,
    table: str,
    parquet: Path,
    merge_cfg: MergeCfg,
    schema_evolution: str = "strict",
    trx_id: str | None = None,
    watermark_column: str | None = None,
) -> None:
    ctx = PluginDestCtx(work) if trx_id is None else _TrxCtx(work, trx_id)
    plugin.SnowflakeDestPlugin().write_out(
        ctx,
        conn,
        SnowflakeDest(
            tables=[table],
            if_table_exists="merge",
            merge_cfg=[merge_cfg],
            schema_evolution=schema_evolution,
            watermark_column=watermark_column,
            dest_cfg=_ICEBERG,
        ),
        [parquet],
    )


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_inserts_updates_and_deletes(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_CDC"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {
            "order_id": [1, 2, 3],
            "status": ["new", "new", "new"],
            "amount": [10, 20, 30],
            "seq": [1, 1, 1],
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
            "seq": [2, 2, 2],
            "op": ["u", "d", "i"],
        },
    )
    try:
        _merge_out(conn, work, table, seed, merge_cfg)
        assert db.count(table) == 3

        _merge_out(conn, work, table, delta, merge_cfg)
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1, 2, 4],
                "status": ["new", "shipped", "new"],
                "amount": [10, 25, 40],
                "seq": [1, 2, 2],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_coalesces_several_changes_for_one_key(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # One batch, several changes per key. MERGE matches each incoming row at
    # most once, so without the coalesce Snowflake rejects the whole statement
    # for a non-deterministic update. Each key resolves to its net effect:
    # order 1 is born and advances (insert, latest payload), order 2 pre-existed
    # and ends deleted, order 3 is born and gone inside the batch so it never
    # reaches the target at all.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_COALESCE"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
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
        assert db.count(table) == 1
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_passes_over_a_row_no_clause_claims(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # Re-sending an insert for a key the target already holds is a no-op, not an
    # upsert: the row matches, so WHEN NOT MATCHED cannot fire, and the update
    # clause wants 'u'. Worth pinning because "insert" reads like it should win.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_UNCLAIMED"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    current = _batch(
        tmp_path / "current.parquet",
        {"order_id": [1], "status": ["shipped"], "op": ["i"]},
    )
    resent = _batch(
        tmp_path / "resent.parquet",
        {"order_id": [1], "status": ["new"], "op": ["i"]},
    )
    try:
        _merge_out(conn, work, table, current, merge_cfg)
        _merge_out(conn, work, table, resent, merge_cfg)
        assert db.count(table) == 1
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({"order_id": [1], "status": ["shipped"]}),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_reads_a_marker_in_either_case(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The marker is lower-cased before it is compared, so the upper-case letters
    # a CDC feed usually emits are claimed by the same clauses as lower case. A
    # marker that is neither leaves the batch during the coalesce, so the third
    # batch here stages no rows at all and the target is untouched.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_MARKER_CASE"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
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
            pl.DataFrame({
                "order_id": [1, 2],
                "status": ["shipped", "new"],
            }),
        )

        _merge_out(conn, work, table, unknown, merge_cfg)
        assert db.count(table) == 2
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_takes_a_new_column_on_update(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # A widened batch has to reach a target that predates the new column.
    # ENABLE_SCHEMA_EVOLUTION does not reach a MERGE, so with
    # schema_evolution="update" the connector ALTERs the column in itself before
    # the statement runs, which is what puts it among the columns written. Rows
    # the batch does not touch keep a NULL for it.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_WIDENED"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1, 2], "status": ["new", "new"], "op": ["i", "i"]},
    )
    widened = _batch(
        tmp_path / "widened.parquet",
        {
            "order_id": [2, 3],
            "status": ["shipped", "new"],
            "priority": ["high", "low"],
            "op": ["u", "i"],
        },
    )
    try:
        _merge_out(conn, work, table, seed, merge_cfg, schema_evolution="update")
        assert "priority" not in db.read_table(table).columns

        _merge_out(conn, work, table, widened, merge_cfg, schema_evolution="update")
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1, 2, 3],
                "status": ["new", "shipped", "new"],
                "priority": [None, "high", "low"],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_applies_one_cfg_per_table(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # Two slots merged in one call, each by its own key -- the list of configs
    # has to stay paired with `tables`, or a table is matched on a column that
    # belongs to the other one.
    db = store(domain_snowflake)
    orders, items = "TD_DEST_MERGE_ORDERS", "TD_DEST_MERGE_ITEMS"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    dest = SnowflakeDest(
        tables=[orders, items],
        if_table_exists="merge",
        merge_cfg=[
            MergeCfg(on=["order_id"], op_column="op"),
            MergeCfg(on=["item_id"], op_column="op"),
        ],
        dest_cfg=_ICEBERG,
    )
    seeds = [
        _batch(
            tmp_path / "orders_seed.parquet",
            {"order_id": [1, 2], "status": ["new", "new"], "op": ["i", "i"]},
        ),
        _batch(
            tmp_path / "items_seed.parquet",
            {"item_id": [7, 8], "label": ["bolt", "nut"], "op": ["i", "i"]},
        ),
    ]
    deltas = [
        _batch(
            tmp_path / "orders_delta.parquet",
            {"order_id": [2, 3], "status": ["shipped", "new"], "op": ["u", "i"]},
        ),
        _batch(
            tmp_path / "items_delta.parquet",
            {"item_id": [8, 9], "label": ["washer", "screw"], "op": ["u", "i"]},
        ),
    ]
    try:
        for batch in (seeds, deltas):
            plugin.SnowflakeDestPlugin().write_out(
                PluginDestCtx(work), conn, dest, batch
            )
        assert_frames_match(
            db.read_table(orders),
            pl.DataFrame({
                "order_id": [1, 2, 3],
                "status": ["new", "shipped", "new"],
            }),
        )
        assert_frames_match(
            db.read_table(items),
            pl.DataFrame({
                "item_id": [7, 8, 9],
                "label": ["bolt", "washer", "screw"],
            }),
        )
    finally:
        db.drop_table(orders)
        db.drop_table(items)


@pytest.mark.parametrize(
    "template, table", list(_spelling_params("TD_DEST_MERGE_FQN_"))
)
@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_resolves_table_name_spelling(
    domain_snowflake: SnowflakeDomain,
    template: str,
    table: str,
    tmp_path: Path,
):
    db = store(domain_snowflake)
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    spelled = template.format(
        database=domain_snowflake.database.upper(),
        schema=domain_snowflake.schema.upper(),
        table=table,
    )
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1, 2], "status": ["new", "new"], "op": ["i", "i"]},
    )
    delta = _batch(
        tmp_path / "delta.parquet",
        {"order_id": [2, 3], "status": ["shipped", "new"], "op": ["u", "i"]},
    )
    try:
        _merge_out(conn, work, table, seed, merge_cfg)
        _merge_out(conn, work, spelled, delta, merge_cfg)
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1, 2, 3],
                "status": ["new", "shipped", "new"],
            }),
        )
    finally:
        db.drop_table(table)


# ---------------------------------------------------------------------------
# schema_evolution
#
# ``"strict"`` is the default, so a target's columns only ever change when the
# flow asked for it. The two dispositions get there differently: a COPY evolves
# the target itself once the table carries ENABLE_SCHEMA_EVOLUTION, whereas a
# MERGE ignores that mark and the connector ALTERs the columns in first. Both
# are covered here because only the first is Snowflake's own machinery.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("if_table_exists", ["append", "replace"])
@snowflake_domain("snowflake")
def test_dest_plugin_write_out_takes_a_new_column_on_update(
    domain_snowflake: SnowflakeDomain,
    if_table_exists: str,
    tmp_path: Path,
):
    db = store(domain_snowflake)
    table = f"TD_DEST_EVOLVE_{if_table_exists.upper()}"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    narrow = _batch(
        tmp_path / "narrow.parquet",
        {"order_id": [1, 2], "status": ["new", "new"], "op": ["i", "i"]},
    )
    widened = _batch(
        tmp_path / "widened.parquet",
        {"order_id": [3], "status": ["new"], "priority": ["high"]},
    )

    def write(parquet: Path) -> None:
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(
                tables=[table],
                if_table_exists=if_table_exists,
                schema_evolution="update",
            ),
            [parquet],
        )

    try:
        write(narrow)
        assert "priority" not in db.read_table(table).columns

        write(widened)
        got = db.read_table(table)
        assert "priority" in got.columns
        # replace truncates first, so only the widened row survives there.
        assert got.filter(got["order_id"] == 3)["priority"].to_list() == ["high"]
    finally:
        db.drop_table(table)


# ---------------------------------------------------------------------------
# schema_evolution_engine="iceberg" on a bulk load
#
# Snowflake's own evolution only ever adds a column, so a bulk load cannot widen
# one however the batch has grown. Diffing the target against the staged file
# reaches what the mark cannot -- and refuses what Snowflake could not do at all.
#
# A target of a particular width has to be built with DDL: one the connector
# creates is templated from the staged file itself, so both sides would agree by
# construction and there would be nothing to widen.
# ---------------------------------------------------------------------------


def _load_out(conn, work: Path, table: str, parquet: Path, **dest_kwargs) -> None:
    plugin.SnowflakeDestPlugin().write_out(
        PluginDestCtx(work),
        conn,
        SnowflakeDest(
            tables=[table],
            if_table_exists=dest_kwargs.pop("if_table_exists", "append"),
            schema_evolution=dest_kwargs.pop("schema_evolution", "update"),
            **dest_kwargs,
        ),
        [parquet],
    )


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_iceberg_engine_widens_a_bounded_text_column(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # A parquet string carries no length, so infer_schema reports the widest
    # text type and the bounded target column is widened to it. Recorded rather
    # than endorsed -- it is the one widening no file can justify, and Snowflake
    # cannot narrow the column back -- but it is what asking for the Iceberg
    # engine buys, and native evolution cannot do it at all.
    db = store(domain_snowflake)
    table = "TD_DEST_ICEBERG_WIDEN_TEXT"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    db.create_typed_table(table, "ORDER_ID NUMBER(38,0), STATUS VARCHAR(10)")
    assert db.column_types(table)["status"] == ("TEXT", 10, None)
    batch = _batch(tmp_path / "short.parquet", {"order_id": [1], "status": ["new"]})
    try:
        _load_out(conn, work, table, batch, dest_cfg=_ICEBERG)
        assert db.column_types(table)["status"] == ("TEXT", 16777216, None)
        assert db.count(table) == 1
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_native_engine_leaves_the_width_alone(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The counterpart, and the reason the option exists: ENABLE_SCHEMA_EVOLUTION
    # adds columns and nothing else, so the bounded column keeps its width.
    db = store(domain_snowflake)
    table = "TD_DEST_NATIVE_WIDEN_TEXT"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    db.create_typed_table(table, "ORDER_ID NUMBER(38,0), STATUS VARCHAR(10)")
    batch = _batch(tmp_path / "short.parquet", {"order_id": [1], "status": ["new"]})
    try:
        _load_out(conn, work, table, batch)
        assert db.column_types(table)["status"] == ("TEXT", 10, None)
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_iceberg_engine_adds_a_new_column(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The half both engines can do, so the Iceberg pass has to do it too --
    # with the mark off, it is the only thing that would.
    db = store(domain_snowflake)
    table = "TD_DEST_ICEBERG_ADD_COLUMN"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    narrow = _batch(tmp_path / "narrow.parquet", {"order_id": [1], "status": ["new"]})
    widened = _batch(
        tmp_path / "widened.parquet",
        {"order_id": [2], "status": ["new"], "priority": ["high"]},
    )
    try:
        _load_out(conn, work, table, narrow, dest_cfg=_ICEBERG)
        assert "priority" not in db.read_table(table).columns

        _load_out(conn, work, table, widened, dest_cfg=_ICEBERG)
        got = db.read_table(table)
        assert "priority" in got.columns
        assert got.filter(got["order_id"] == 2)["priority"].to_list() == ["high"]
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_iceberg_engine_refuses_an_impossible_retype(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # No ALTER reconciles two bases, so the write stops before anything is
    # loaded rather than after COPY INTO has half-applied it.
    db = store(domain_snowflake)
    table = "TD_DEST_ICEBERG_UNSAFE"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    db.create_typed_table(table, "ORDER_ID NUMBER(38,0), AMOUNT NUMBER(10,2)")
    before = db.column_types(table)
    batch = _batch(
        tmp_path / "lettered.parquet",
        {"order_id": [1], "amount": ["not a number"]},
    )
    try:
        with pytest.raises(SnowflakeRuntimeException):
            _load_out(conn, work, table, batch, dest_cfg=_ICEBERG)
        assert db.column_types(table) == before
        assert db.count(table) == 0
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_iceberg_engine_does_nothing_under_strict(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # Strict pins the target's schema; the engine only says *how* a schema that
    # is allowed to move is moved. The rows still land.
    db = store(domain_snowflake)
    table = "TD_DEST_ICEBERG_STRICT"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    db.create_typed_table(table, "ORDER_ID NUMBER(38,0), STATUS VARCHAR(10)")
    batch = _batch(tmp_path / "short.parquet", {"order_id": [1], "status": ["new"]})
    try:
        _load_out(
            conn, work, table, batch, schema_evolution="strict", dest_cfg=_ICEBERG
        )
        assert db.column_types(table)["status"] == ("TEXT", 10, None)
        assert db.count(table) == 1
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("if_table_exists", ["append", "replace"])
@snowflake_domain("snowflake")
def test_dest_plugin_write_out_ignores_a_new_column_on_strict(
    domain_snowflake: SnowflakeDomain,
    if_table_exists: str,
    tmp_path: Path,
):
    # The counterpart of ..._takes_a_new_column_on_update, and the proof that
    # strict is more than "no ALTER": the write clears ENABLE_SCHEMA_EVOLUTION,
    # so a target an earlier update-mode run had marked stops evolving rather
    # than widening behind the flow's back. Unmarked, MATCH_BY_COLUMN_NAME simply
    # drops the column the table has no home for -- the rows still land, which is
    # the half that would break if "ignored" ever became "rejected".
    db = store(domain_snowflake)
    table = f"TD_DEST_STRICT_WIDENED_{if_table_exists.upper()}"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    narrow = _batch(
        tmp_path / "narrow.parquet",
        {"order_id": [1, 2], "status": ["new", "new"]},
    )
    widened = _batch(
        tmp_path / "widened.parquet",
        {"order_id": [3], "status": ["new"], "priority": ["high"]},
    )

    def write(parquet: Path) -> None:
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(
                tables=[table],
                if_table_exists=if_table_exists,
                schema_evolution="strict",
            ),
            [parquet],
        )

    try:
        write(narrow)
        write(widened)
        got = db.read_table(table)
        assert set(got.columns) == {"order_id", "status"}
        # replace truncates before the COPY, so only the widened row survives.
        order_ids = [3] if if_table_exists == "replace" else [1, 2, 3]
        assert_frames_match(
            got,
            pl.DataFrame({
                "order_id": order_ids,
                "status": ["new"] * len(order_ids),
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_keeps_the_target_schema_on_strict(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The merge path reaches ``_evolve_target_columns`` only on update, so a
    # same-shape batch has to merge cleanly under strict without it.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_STRICT"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1, 2], "status": ["new", "new"], "op": ["i", "i"]},
    )
    delta = _batch(
        tmp_path / "delta.parquet",
        {"order_id": [2, 3], "status": ["shipped", "new"], "op": ["u", "i"]},
    )
    try:
        _merge_out(conn, work, table, seed, merge_cfg, schema_evolution="strict")
        _merge_out(conn, work, table, delta, merge_cfg, schema_evolution="strict")
        got = db.read_table(table)
        assert set(got.columns) == {"order_id", "status"}
        assert_frames_match(
            got,
            pl.DataFrame({
                "order_id": [1, 2, 3],
                "status": ["new", "shipped", "new"],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_ignores_a_new_column_on_strict(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The counterpart of ..._merge_takes_a_new_column_on_update: strict adds
    # nothing, so the column the batch carries has nowhere to land in the target
    # so it is not among the columns the merge writes. What must still happen is
    # the merge itself: the rows land, only the schema is held still, and this
    # fails either if the column appeared or if the rows did not move.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_STRICT_WIDENED"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1, 2], "status": ["new", "new"], "op": ["i", "i"]},
    )
    widened = _batch(
        tmp_path / "widened.parquet",
        {
            "order_id": [2, 3],
            "status": ["shipped", "new"],
            "priority": ["high", "low"],
            "op": ["u", "i"],
        },
    )
    try:
        _merge_out(conn, work, table, seed, merge_cfg, schema_evolution="strict")
        _merge_out(conn, work, table, widened, merge_cfg, schema_evolution="strict")
        got = db.read_table(table)
        assert set(got.columns) == {"order_id", "status"}
        assert_frames_match(
            got,
            pl.DataFrame({
                "order_id": [1, 2, 3],
                "status": ["new", "shipped", "new"],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_takes_a_batch_narrower_than_the_target(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The mirror of the widened case, and the direction schema_evolution cannot
    # help with: the target has a column the batch does not. The write lists name
    # only what both sides have, so the absent column simply goes unwritten --
    # keeping its value on an updated row and taking a NULL on an inserted one.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_NARROWED"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    wide = _batch(
        tmp_path / "wide.parquet",
        {
            "order_id": [1, 2],
            "status": ["new", "new"],
            "priority": ["high", "high"],
            "op": ["i", "i"],
        },
    )
    narrow = _batch(
        tmp_path / "narrow.parquet",
        {"order_id": [2, 3], "status": ["shipped", "new"], "op": ["u", "i"]},
    )
    try:
        _merge_out(conn, work, table, wide, merge_cfg)
        _merge_out(conn, work, table, narrow, merge_cfg)
        got = db.read_table(table)
        assert set(got.columns) == {"order_id", "status", "priority"}
        assert_frames_match(
            got,
            pl.DataFrame({
                "order_id": [1, 2, 3],
                "status": ["new", "shipped", "new"],
                "priority": ["high", "high", None],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_stops_evolving_when_switched_back_to_strict(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # ENABLE_SCHEMA_EVOLUTION persists on the table, so strict has to clear what
    # an earlier update-mode run set -- otherwise a flow that once evolved keeps
    # evolving no matter what it asks for afterwards. The same widened file is
    # written twice, once per setting, and only the first may reach the schema.
    db = store(domain_snowflake)
    table = "TD_DEST_EVOLVE_THEN_STRICT"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    narrow = _batch(
        tmp_path / "narrow.parquet",
        {"order_id": [1], "status": ["new"]},
    )
    widened = _batch(
        tmp_path / "widened.parquet",
        {"order_id": [2], "status": ["new"], "priority": ["high"]},
    )
    wider = _batch(
        tmp_path / "wider.parquet",
        {"order_id": [3], "status": ["new"], "priority": ["low"], "note": ["hi"]},
    )

    def write(parquet: Path, schema_evolution: str) -> None:
        plugin.SnowflakeDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            SnowflakeDest(
                tables=[table],
                if_table_exists="append",
                schema_evolution=schema_evolution,
            ),
            [parquet],
        )

    try:
        write(narrow, "strict")
        write(widened, "update")
        assert "priority" in db.read_table(table).columns

        write(wider, "strict")
        got = db.read_table(table)
        assert set(got.columns) == {"order_id", "status", "priority"}
        assert got.height == 3
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("schema_evolution", ["strict", "update"])
@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_keeps_the_marker_out_of_the_target(
    domain_snowflake: SnowflakeDomain,
    schema_evolution: str,
    tmp_path: Path,
):
    # The marker says what to do with a row; it is not part of the row. The
    # clauses read it off the staged copy and no write list names it, so the
    # target never gains the column -- under either evolution setting, since
    # update mode excludes it from what it ALTERs in.
    db = store(domain_snowflake)
    table = f"TD_DEST_MERGE_NO_MARKER_{schema_evolution.upper()}"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1, 2], "status": ["new", "new"], "op": ["i", "i"]},
    )
    delta = _batch(
        tmp_path / "delta.parquet",
        {"order_id": [2, 3], "status": ["shipped", "new"], "op": ["u", "i"]},
    )
    try:
        _merge_out(conn, work, table, seed, merge_cfg, schema_evolution)
        _merge_out(conn, work, table, delta, merge_cfg, schema_evolution)
        got = db.read_table(table)
        assert set(got.columns) == {"order_id", "status"}
        assert_frames_match(
            got,
            pl.DataFrame({
                "order_id": [1, 2, 3],
                "status": ["new", "shipped", "new"],
            }),
        )
    finally:
        db.drop_table(table)


# ---------------------------------------------------------------------------
# if_table_exists="merge" + a watermark column
#
# A merge is not naturally idempotent: rerunning one applies the same batch a
# second time. Naming a watermark column makes it so -- every row the merge
# writes is stamped with the id of the trx that wrote it, and a rerun that finds
# the target already carrying that id does nothing at all.
#
# Which trx a write belongs to is the whole variable here, so these drive the
# plugin through :class:`_TrxCtx` rather than the shared context stub.
# ---------------------------------------------------------------------------

_WATERMARK = "wm"


def _watermark_cfg(on: str = "order_id") -> MergeCfg:
    """The merge config a watermarked test uses. The watermark itself is named
    on the destination -- see the ``watermark_column=`` each call passes."""
    return MergeCfg(on=[on], op_column="op")


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_stamps_the_rows_it_writes(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The target the merge creates is templated from the staged relation, and the
    # watermark column is added to that relation before the target is built, so
    # even a first merge under strict -- which never reaches the ALTER pass --
    # brings the column into being and stamps every row it inserts.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_WATERMARK_STAMP"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1, 2], "status": ["new", "new"], "op": ["i", "i"]},
    )
    try:
        _merge_out(
            conn,
            work,
            table,
            seed,
            _watermark_cfg(),
            trx_id="TRX_A",
            watermark_column=_WATERMARK,
        )
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1, 2],
                "status": ["new", "new"],
                _WATERMARK: ["TRX_A", "TRX_A"],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_skips_a_batch_the_trx_already_wrote(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The point of the whole thing. The second write carries a different payload
    # for the same key and a new one besides, so applying it would be plainly
    # visible -- and it must not be, because the target already holds the trx's
    # stamp.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_WATERMARK_RERUN"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1], "status": ["new"], "op": ["i"]},
    )
    rerun = _batch(
        tmp_path / "rerun.parquet",
        {"order_id": [1, 2], "status": ["shipped", "new"], "op": ["u", "i"]},
    )
    try:
        _merge_out(
            conn,
            work,
            table,
            seed,
            _watermark_cfg(),
            trx_id="TRX_A",
            watermark_column=_WATERMARK,
        )
        _merge_out(
            conn,
            work,
            table,
            rerun,
            _watermark_cfg(),
            trx_id="TRX_A",
            watermark_column=_WATERMARK,
        )
        assert db.count(table) == 1
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1],
                "status": ["new"],
                _WATERMARK: ["TRX_A"],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_applies_a_batch_from_a_later_trx(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The other half: the guard is on this trx's id, not on the column being
    # populated at all, so a later trx merges normally and restamps the rows it
    # touches. Order 1 keeps the older stamp because no clause claims it.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_WATERMARK_NEXT_TRX"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1, 2], "status": ["new", "new"], "op": ["i", "i"]},
    )
    delta = _batch(
        tmp_path / "delta.parquet",
        {"order_id": [2, 3], "status": ["shipped", "new"], "op": ["u", "i"]},
    )
    try:
        _merge_out(
            conn,
            work,
            table,
            seed,
            _watermark_cfg(),
            trx_id="TRX_A",
            watermark_column=_WATERMARK,
        )
        _merge_out(
            conn,
            work,
            table,
            delta,
            _watermark_cfg(),
            trx_id="TRX_B",
            watermark_column=_WATERMARK,
        )
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1, 2, 3],
                "status": ["new", "shipped", "new"],
                _WATERMARK: ["TRX_A", "TRX_B", "TRX_B"],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_adds_the_watermark_to_an_older_target(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # A target that predates the watermark has no column to be stamped in, so the
    # first stamped merge has to evolve one in -- which only update mode does. It
    # is checked before it is written, so this first run also covers the probe
    # against a target that has not got the column: no such column means never
    # stamped, not an error.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_WATERMARK_EVOLVED"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    db.create_typed_table(table, "ORDER_ID NUMBER(38,0), STATUS VARCHAR(16777216)")
    assert _WATERMARK not in db.column_types(table)
    batch = _batch(
        tmp_path / "batch.parquet",
        {"order_id": [1], "status": ["new"], "op": ["i"]},
    )
    rerun = _batch(
        tmp_path / "rerun.parquet",
        {"order_id": [1], "status": ["shipped"], "op": ["u"]},
    )
    try:
        _merge_out(
            conn,
            work,
            table,
            batch,
            _watermark_cfg(),
            schema_evolution="update",
            trx_id="TRX_A",
            watermark_column=_WATERMARK,
        )
        assert db.column_types(table)[_WATERMARK] == ("TEXT", 16777216, None)

        _merge_out(
            conn,
            work,
            table,
            rerun,
            _watermark_cfg(),
            schema_evolution="update",
            trx_id="TRX_A",
            watermark_column=_WATERMARK,
        )
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1],
                "status": ["new"],
                _WATERMARK: ["TRX_A"],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_stamps_an_older_target_on_strict_too(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # Strict holds the target's schema still, and the watermark is the one
    # column it cannot hold back: without it nothing is ever stamped, so the
    # guard would read every run as the first and the watermark would quietly do
    # nothing. The seed merges without a watermark, the way a flow that turned
    # one on later would have been running -- so its row predates the column and
    # keeps a NULL there. Nothing backfills this trx's id onto rows it did not
    # write.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_WATERMARK_STRICT"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    db.create_typed_table(table, "ORDER_ID NUMBER(38,0), STATUS VARCHAR(16777216)")
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1], "status": ["old"], "op": ["i"]},
    )
    batch = _batch(
        tmp_path / "batch.parquet",
        {"order_id": [2], "status": ["new"], "op": ["i"]},
    )
    rerun = _batch(
        tmp_path / "rerun.parquet",
        {"order_id": [2], "status": ["shipped"], "op": ["u"]},
    )
    try:
        _merge_out(conn, work, table, seed, MergeCfg(on=["order_id"], op_column="op"))
        assert _WATERMARK not in db.column_types(table)

        _merge_out(
            conn,
            work,
            table,
            batch,
            _watermark_cfg(),
            trx_id="TRX_A",
            watermark_column=_WATERMARK,
        )
        assert db.column_types(table)[_WATERMARK] == ("TEXT", 16777216, None)

        _merge_out(
            conn,
            work,
            table,
            rerun,
            _watermark_cfg(),
            trx_id="TRX_A",
            watermark_column=_WATERMARK,
        )
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1, 2],
                "status": ["old", "new"],
                _WATERMARK: [None, "TRX_A"],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_adds_only_the_watermark_on_strict(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The watermark is an exception to strict, not a hole in it: the rest of a
    # widened batch is still held back, so `priority` has nowhere to land and
    # goes unwritten while `wm` is added.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_WATERMARK_STRICT_WIDENED"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    db.create_typed_table(table, "ORDER_ID NUMBER(38,0), STATUS VARCHAR(16777216)")
    widened = _batch(
        tmp_path / "widened.parquet",
        {
            "order_id": [1],
            "status": ["new"],
            "priority": ["high"],
            "op": ["i"],
        },
    )
    try:
        _merge_out(
            conn,
            work,
            table,
            widened,
            _watermark_cfg(),
            trx_id="TRX_A",
            watermark_column=_WATERMARK,
        )
        assert set(db.read_table(table).columns) == {"order_id", "status", _WATERMARK}
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1],
                "status": ["new"],
                _WATERMARK: ["TRX_A"],
            }),
        )
    finally:
        db.drop_table(table)


# ---------------------------------------------------------------------------
# if_table_exists="merge" + type widening
#
# ENABLE_SCHEMA_EVOLUTION does not reach a MERGE, so the merge path ALTERs the
# target itself: a column both sides have whose type has moved is widened before
# the statement runs, and one Snowflake cannot widen stops the write instead.
#
# What a parquet file can say about a type is the question underneath all of
# these. It carries a decimal's precision and scale and a timestamp's unit, and
# carries no length for a string at all -- so these tests are also what pins
# down what ``infer_schema`` reports back for each.
#
# A target of a particular width has to be built with DDL: one the connector
# creates is templated from the staged file itself, so both sides would agree by
# construction and there would be nothing to widen.
# ---------------------------------------------------------------------------


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_repeats_without_touching_the_schema(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The target the first merge creates is templated from the staged file, so
    # the second merge diffs that file against a table built from it and must
    # find nothing to do. The two sides do not spell a type the same way --
    # DESC TABLE reports VARCHAR(16777216) where infer_schema reports TEXT -- so
    # this is the case that breaks if the spellings are compared as strings.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_SCHEMA_STABLE"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1], "status": ["new"], "amount": [10], "op": ["i"]},
    )
    delta = _batch(
        tmp_path / "delta.parquet",
        {
            "order_id": [1, 2],
            "status": ["shipped", "new"],
            "amount": [11, 20],
            "op": ["u", "i"],
        },
    )
    try:
        _merge_out(conn, work, table, seed, merge_cfg, schema_evolution="update")
        before = db.column_types(table)

        _merge_out(conn, work, table, delta, merge_cfg, schema_evolution="update")
        assert db.column_types(table) == before
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1, 2],
                "status": ["shipped", "new"],
                "amount": [11, 20],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_keeps_a_timestamp_precision_it_cannot_alter(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # TIMESTAMP_NTZ defaults to precision 9 and the parquet carries microseconds,
    # so the two sides can report the column differently -- a real difference
    # with no statement behind it, since ALTER COLUMN reaches the parameters of
    # NUMBER and VARCHAR alone. The load casts into the column as it stands, so
    # the write has to go through and the precision has to stay where it was.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_TIMESTAMP"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    db.create_typed_table(table, "ORDER_ID NUMBER(38,0), SEEN_AT TIMESTAMP_NTZ")
    before = db.column_types(table)
    batch = _batch(
        tmp_path / "stamped.parquet",
        {
            "order_id": [1],
            "seen_at": pl.Series(
                [datetime(2026, 1, 2, 3, 4, 5)], dtype=pl.Datetime("us")
            ),
            "op": ["i"],
        },
    )
    try:
        _merge_out(conn, work, table, batch, merge_cfg, schema_evolution="update")
        assert db.column_types(table) == before
        assert db.count(table) == 1
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_widens_a_number_precision(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # Parquet stores a decimal's precision and scale, so this is the one widening
    # the incoming file can actually justify: same scale, more digits. If it
    # fails as an unsafe retype instead, infer_schema is reporting the column
    # without its parameters and no decimal widening is reachable from a file.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_WIDEN_NUMBER"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    db.create_typed_table(table, "ORDER_ID NUMBER(38,0), AMOUNT NUMBER(9,2)")
    batch = _batch(
        tmp_path / "priced.parquet",
        {
            "order_id": [1],
            "amount": pl.Series([Decimal("123456.78")], dtype=pl.Decimal(20, 2)),
            "op": ["i"],
        },
    )
    try:
        _merge_out(conn, work, table, batch, merge_cfg, schema_evolution="update")
        assert db.column_types(table)["amount"] == ("NUMBER", 20, 2)
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({"order_id": [1], "amount": [Decimal("123456.78")]}),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_widens_a_bounded_text_column_to_full_width(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # A parquet string carries no length, so infer_schema can only report the
    # widest text type and every bounded text column in the target is widened to
    # it -- on the first update-mode merge, whatever the values in the batch
    # measure. Recorded rather than endorsed: it is the one widening no file can
    # ever justify, and Snowflake cannot narrow the column back afterwards.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_WIDEN_TEXT"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    db.create_typed_table(table, "ORDER_ID NUMBER(38,0), STATUS VARCHAR(10)")
    assert db.column_types(table)["status"] == ("TEXT", 10, None)
    batch = _batch(
        tmp_path / "short.parquet",
        {"order_id": [1], "status": ["new"], "op": ["i"]},
    )
    try:
        _merge_out(conn, work, table, batch, merge_cfg, schema_evolution="update")
        assert db.column_types(table)["status"] == ("TEXT", 16777216, None)
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_holds_a_bounded_text_column_on_strict(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The counterpart: strict never reaches the ALTER pass at all, so the target
    # keeps the width it was declared with and the rows still land.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_STRICT_TEXT"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    db.create_typed_table(table, "ORDER_ID NUMBER(38,0), STATUS VARCHAR(10)")
    batch = _batch(
        tmp_path / "short.parquet",
        {"order_id": [1], "status": ["new"], "op": ["i"]},
    )
    try:
        _merge_out(conn, work, table, batch, merge_cfg, schema_evolution="strict")
        assert db.column_types(table)["status"] == ("TEXT", 10, None)
        assert_frames_match(
            db.read_table(table), pl.DataFrame({"order_id": [1], "status": ["new"]})
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_merge_refuses_a_change_snowflake_cannot_make(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # A batch whose column is text against a target column that is a number:
    # no ALTER reconciles two bases, so the write stops before anything is
    # staged rather than after the target has been half evolved.
    db = store(domain_snowflake)
    table = "TD_DEST_MERGE_UNSAFE_RETYPE"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    db.create_typed_table(table, "ORDER_ID NUMBER(38,0), AMOUNT NUMBER(10,2)")
    before = db.column_types(table)
    batch = _batch(
        tmp_path / "lettered.parquet",
        {"order_id": [1], "amount": ["not a number"], "op": ["i"]},
    )
    try:
        with pytest.raises(SnowflakeRuntimeException):
            _merge_out(conn, work, table, batch, merge_cfg, schema_evolution="update")
        assert db.column_types(table) == before
        assert db.count(table) == 0
    finally:
        db.drop_table(table)


# ---------------------------------------------------------------------------
# if_table_exists="append" + a watermark column
#
# An append is not idempotent either, and for a blunter reason than a merge:
# Snowflake's load history does not recognise a re-staged file, so a rerun of
# the same transaction simply loads the batch a second time (see
# ``..._append_reloads_a_file_it_has_seen``). Naming a watermark closes it --
# the column is written into the parquet before it is staged, so the COPY
# stamps the rows it loads, and a rerun that finds the target already carrying
# this trx's id does nothing at all.
# ---------------------------------------------------------------------------


def _append_out(
    conn,
    work: Path,
    table: str,
    parquet: Path,
    schema_evolution: str = "strict",
    trx_id: str | None = None,
    watermark_column: str | None = None,
) -> None:
    ctx = PluginDestCtx(work) if trx_id is None else _TrxCtx(work, trx_id)
    plugin.SnowflakeDestPlugin().write_out(
        ctx,
        conn,
        SnowflakeDest(
            tables=[table],
            if_table_exists="append",
            schema_evolution=schema_evolution,
            watermark_column=watermark_column,
        ),
        [parquet],
    )


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_append_stamps_the_rows_it_loads(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The target the append creates is templated from the staged file, and the
    # watermark is written into that file before it is staged -- so even a first
    # append under strict brings the column into being and stamps every row.
    db = store(domain_snowflake)
    table = "TD_DEST_APPEND_WATERMARK_STAMP"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    batch = _batch(
        tmp_path / "batch.parquet",
        {"order_id": [1, 2], "status": ["new", "new"]},
    )
    try:
        _append_out(
            conn, work, table, batch, trx_id="TRX_A", watermark_column=_WATERMARK
        )
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1, 2],
                "status": ["new", "new"],
                _WATERMARK: ["TRX_A", "TRX_A"],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_append_skips_a_batch_the_trx_already_loaded(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The point of the whole thing: the same trx running twice must leave the
    # target exactly as one run did. Without the watermark this is the case that
    # doubles the row count.
    db = store(domain_snowflake)
    table = "TD_DEST_APPEND_WATERMARK_RERUN"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    batch = _batch(
        tmp_path / "batch.parquet",
        {"order_id": [1, 2], "status": ["new", "new"]},
    )
    try:
        _append_out(
            conn, work, table, batch, trx_id="TRX_A", watermark_column=_WATERMARK
        )
        _append_out(
            conn, work, table, batch, trx_id="TRX_A", watermark_column=_WATERMARK
        )
        assert db.count(table) == 2
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1, 2],
                "status": ["new", "new"],
                _WATERMARK: ["TRX_A", "TRX_A"],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_append_loads_again_under_a_later_trx(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The other half: the guard is on this trx's id, not on the column being
    # populated at all, so a later trx appends normally and its rows carry their
    # own stamp beside the earlier ones.
    db = store(domain_snowflake)
    table = "TD_DEST_APPEND_WATERMARK_NEXT_TRX"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    first = _batch(tmp_path / "first.parquet", {"order_id": [1], "status": ["new"]})
    second = _batch(
        tmp_path / "second.parquet", {"order_id": [2], "status": ["shipped"]}
    )
    try:
        _append_out(
            conn, work, table, first, trx_id="TRX_A", watermark_column=_WATERMARK
        )
        _append_out(
            conn, work, table, second, trx_id="TRX_B", watermark_column=_WATERMARK
        )
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1, 2],
                "status": ["new", "shipped"],
                _WATERMARK: ["TRX_A", "TRX_B"],
            }),
        )
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("schema_evolution", ["strict", "update"])
@snowflake_domain("snowflake")
def test_dest_plugin_write_out_append_adds_the_watermark_to_an_older_target(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
    schema_evolution,
):
    # A target that predates the watermark has no column to be stamped in. Under
    # strict nothing else would add one -- which is exactly why the append adds
    # it itself, rather than leaving the COPY to drop a column it cannot match
    # and the guard to read every rerun as the first.
    #
    # The rows already there belong to an earlier transaction and must not be
    # claimed for this one, so they take a NULL.
    db = store(domain_snowflake)
    table = f"TD_DEST_APPEND_WATERMARK_OLDER_{schema_evolution.upper()}"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    seed = _batch(tmp_path / "seed.parquet", {"order_id": [1], "status": ["new"]})
    later = _batch(tmp_path / "later.parquet", {"order_id": [2], "status": ["new"]})
    try:
        # The way a flow that turned the watermark on later would have started.
        _append_out(conn, work, table, seed, schema_evolution=schema_evolution)
        _append_out(
            conn,
            work,
            table,
            later,
            schema_evolution=schema_evolution,
            trx_id="TRX_A",
            watermark_column=_WATERMARK,
        )
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1, 2],
                "status": ["new", "new"],
                _WATERMARK: [None, "TRX_A"],
            }),
        )
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_append_guards_a_rerun_of_an_older_target(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The column added on the way past has to be a working watermark, not just a
    # column: the run that introduces it is also the first run the guard can
    # protect, so rerunning it must still be a no-op.
    db = store(domain_snowflake)
    table = "TD_DEST_APPEND_WATERMARK_OLDER_RERUN"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    seed = _batch(tmp_path / "seed.parquet", {"order_id": [1], "status": ["new"]})
    later = _batch(tmp_path / "later.parquet", {"order_id": [2], "status": ["new"]})
    try:
        _append_out(conn, work, table, seed)
        _append_out(
            conn, work, table, later, trx_id="TRX_A", watermark_column=_WATERMARK
        )
        _append_out(
            conn, work, table, later, trx_id="TRX_A", watermark_column=_WATERMARK
        )
        assert db.count(table) == 2
    finally:
        db.drop_table(table)


@snowflake_domain("snowflake")
def test_dest_plugin_write_out_append_leaves_the_batch_columns_untouched(
    domain_snowflake: SnowflakeDomain,
    tmp_path: Path,
):
    # The watermark is added to the staged copy, not to the file the write was
    # handed: a batch reused by a later write -- or by a second target in the
    # same one -- must not come back carrying it.
    db = store(domain_snowflake)
    table = "TD_DEST_APPEND_WATERMARK_BATCH_INTACT"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_snowflake)
    batch = _batch(tmp_path / "batch.parquet", {"order_id": [1], "status": ["new"]})
    try:
        _append_out(
            conn, work, table, batch, trx_id="TRX_A", watermark_column=_WATERMARK
        )
        assert pl.read_parquet(batch).columns == ["order_id", "status"]
    finally:
        db.drop_table(table)

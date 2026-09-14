#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake destination-plugin integration tests using fake context"""

from __future__ import annotations

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


def _batch(path: Path, columns: dict[str, list]) -> Path:
    pl.DataFrame(columns).write_parquet(path)
    return path


def _merge_out(
    conn,
    work: Path,
    table: str,
    parquet: Path,
    merge_cfg: MergeCfg,
    schema_evolution: str = "strict",
) -> None:
    plugin.SnowflakeDestPlugin().write_out(
        PluginDestCtx(work),
        conn,
        SnowflakeDest(
            tables=[table],
            if_table_exists="merge",
            merge_cfg=merge_cfg,
            schema_evolution=schema_evolution,
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
    # The merge path reaches ``_add_missing_columns`` only on update, so a
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

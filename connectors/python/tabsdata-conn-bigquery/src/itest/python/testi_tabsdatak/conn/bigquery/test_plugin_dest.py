#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery destination-plugin integration tests using fake context"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import polars as pl
import pytest
from tabsdatax.itest.fixture.backends.bigquery import BigQueryDomain, bigquery_domain
from tabsdatax.itest.fixture.backends.fs import FsDomain, fs_domain
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from tabsdatax.itest.fixture.functions.harness import assert_frames_match
from testi_tabsdatak.conn.bigquery.backend import dest_conn, store, table_fqn
from testi_tabsdatak.conn.common.sql.employees_model import DEPARTMENTS, DEPT_MANAGER
from testi_tabsdatak.conn.common.sql.plugin_io import provisioned_parquet, work_dir

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import BigQueryDest, MergeCfg, _plugin as plugin
from tabsdatak.conn.bigquery.error import BigQueryErrorCode, BigQueryRuntimeException

pytestmark = pytest.mark.bigquery

# Every accepted spelling of a target table name, each with its own table so a
# run leaves nothing unpredictable behind. The missing leading parts are
# completed from the connection's project / dataset.
_SPELLINGS = {
    "bare-table": "{table}",
    "dataset.table": "{dataset}.{table}",
    "project.dataset.table": "{project}.{dataset}.{table}",
}


def _spelling_params(prefix: str = "td_dest_fqn"):
    for spelling, template in _SPELLINGS.items():
        table = f"{prefix}_" + spelling.replace(".", "_").replace("-", "_")
        yield pytest.param(template, table, id=spelling)


@fs_domain("fs_employees_xs")
@bigquery_domain("bigquery")
def test_dest_plugin_write_out_appends_rows(
    domain_fs_employees_xs: FsDomain,
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    db = store(domain_bigquery)
    table = "td_dest_append"
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(domain_bigquery)
    try:
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(tables=[table_fqn(table)], if_table_exists="replace"),
            [parquet],
        )
        assert db.count(table) == rows

        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(tables=[table_fqn(table)], if_table_exists="append"),
            [parquet],
        )
        assert db.count(table) == 2 * rows
    finally:
        db.drop_table(table)


@fs_domain("fs_employees_xs")
@bigquery_domain("bigquery")
def test_dest_plugin_write_out_replace_clears_existing(
    domain_fs_employees_xs: FsDomain,
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # Replacing with a different entity: the load job truncates the table and
    # replaces its schema wholesale, so neither the first load's rows nor its
    # columns survive.
    db = store(domain_bigquery)
    table = "td_dest_replace"
    work = work_dir(tmp_path)
    first = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    second = provisioned_parquet(domain_fs_employees_xs, DEPT_MANAGER)
    expected = pl.read_parquet(second)
    conn = dest_conn(domain_bigquery)
    try:
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(tables=[table_fqn(table)], if_table_exists="replace"),
            [first],
        )
        assert db.count(table) == pl.read_parquet(first).height

        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(tables=[table_fqn(table)], if_table_exists="replace"),
            [second],
        )
        got = db.read_table(table)
        assert got.height == expected.height
        assert set(got.columns) == set(expected.columns)
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("template, table", list(_spelling_params()))
@fs_domain("fs_employees_xs")
@bigquery_domain("bigquery")
def test_dest_plugin_write_out_resolves_table_name_spelling(
    domain_fs_employees_xs: FsDomain,
    domain_bigquery: BigQueryDomain,
    template: str,
    table: str,
    tmp_path: Path,
):
    # The table is created under its bare name and then appended to through the
    # spelling under test, which passes only if both resolve to the same
    # physical table.
    db = store(domain_bigquery)
    work = work_dir(tmp_path)
    parquet = provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS)
    rows = pl.read_parquet(parquet).height
    conn = dest_conn(domain_bigquery)
    spelled = template.format(
        project=domain_bigquery.project,
        dataset=domain_bigquery.dataset,
        table=table,
    )
    try:
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(tables=[table_fqn(table)], if_table_exists="replace"),
            [parquet],
        )
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(tables=[table_fqn(spelled)], if_table_exists="append"),
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
# no fs domain, only the BigQuery one they write into. A target that does not
# exist yet is created from the staging table's schema, so a first merge is a
# create-then-insert; the tests that need a prior state seed one with a plain
# "replace" write instead, from a frame without the marker column.
#
# The batch is coalesced before it is uploaded, so a key carrying several changes
# in one file reaches the MERGE once -- BigQuery rejects a source that matches a
# target row more than once.
# ---------------------------------------------------------------------------


def _batch(path: Path, columns: dict[str, list | pl.Series]) -> Path:
    """A batch parquet. A column given as a `Series` pins its dtype."""
    pl.DataFrame(columns).write_parquet(path, use_pyarrow=True)
    return path


def _seed_target(conn, work: Path, table: str, parquet: Path) -> None:
    """Create the target from ``parquet``, which carries no marker column."""
    plugin._BigQueryDestPlugin().write_out(
        PluginDestCtx(work),
        conn,
        BigQueryDest(tables=[table_fqn(table)], if_table_exists="replace"),
        [parquet],
    )


def _merge_out(
    conn,
    work: Path,
    table: str,
    parquet: Path,
    merge_cfg: MergeCfg,
    schema_evolution: str = "strict",
) -> None:
    plugin._BigQueryDestPlugin().write_out(
        PluginDestCtx(work),
        conn,
        BigQueryDest(
            tables=[table_fqn(table)],
            if_table_exists="merge",
            merge_cfg=merge_cfg,
            schema_evolution=schema_evolution,
        ),
        [parquet],
    )


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_inserts_updates_and_deletes(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    db = store(domain_bigquery)
    table = "td_dest_merge_cdc"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {
            "order_id": [1, 2, 3],
            "status": ["new", "new", "new"],
            "amount": [10, 20, 30],
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
        _seed_target(conn, work, table, seed)
        assert db.count(table) == 3

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


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_coalesces_several_changes_for_one_key(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # One batch, several changes per key. MERGE matches each incoming row at most
    # once, so without the coalesce BigQuery rejects the whole statement for
    # matching a target row more than once. Each key resolves to its net effect:
    # order 1 is born and advances, order 2 pre-existed and ends deleted, order 3
    # is born and gone inside the batch so it never reaches the target at all.
    db = store(domain_bigquery)
    table = "td_dest_merge_coalesce"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [2], "status": ["new"]},
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
        _seed_target(conn, work, table, seed)
        _merge_out(conn, work, table, churn, merge_cfg)
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({"order_id": [1], "status": ["shipped"]}),
        )
        assert db.count(table) == 1
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_reads_a_marker_in_either_case(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # The marker is lower-cased before it is compared, so the upper case a CDC
    # feed usually emits is claimed by the same clauses. A marker that is neither
    # leaves the batch during the coalesce, so the third batch stages no rows and
    # the target is untouched.
    db = store(domain_bigquery)
    table = "td_dest_merge_marker_case"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1], "status": ["new"]},
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
        _seed_target(conn, work, table, seed)
        _merge_out(conn, work, table, upper, merge_cfg)
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({"order_id": [1, 2], "status": ["shipped", "new"]}),
        )

        _merge_out(conn, work, table, unknown, merge_cfg)
        assert db.count(table) == 2
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("schema_evolution", ["update", "strict"])
@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_keeps_the_marker_out_of_the_target(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
    schema_evolution: str,
):
    # The marker says what to do with a row and is never part of the row. No
    # write list names it and the evolution pass excludes it, so it cannot arrive
    # in the target -- not even while the schema is evolving.
    db = store(domain_bigquery)
    table = f"td_dest_merge_no_marker_{schema_evolution}"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(tmp_path / "seed.parquet", {"order_id": [1], "status": ["new"]})
    delta = _batch(
        tmp_path / "delta.parquet",
        {"order_id": [1, 2], "status": ["shipped", "new"], "op": ["u", "i"]},
    )
    try:
        _seed_target(conn, work, table, seed)
        _merge_out(conn, work, table, delta, merge_cfg, schema_evolution)
        got = db.read_table(table)
        assert "op" not in got.columns
        assert set(got.columns) == {"order_id", "status"}
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("schema_evolution", ["update", "strict"])
@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_creates_a_target_that_does_not_exist(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
    schema_evolution: str,
):
    # The first batch has no prior state to merge onto, so the target is created
    # from the staging table's schema and every row lands as NOT MATCHED BY
    # TARGET. Strict has to bootstrap a target too -- the create is not part of
    # evolving one.
    db = store(domain_bigquery)
    table = f"td_dest_merge_create_{schema_evolution}"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    batch = _batch(
        tmp_path / "batch.parquet",
        {"order_id": [1, 2], "status": ["new", "new"], "op": ["i", "i"]},
    )
    db.drop_table(table)
    try:
        _merge_out(conn, work, table, batch, merge_cfg, schema_evolution)
        got = db.read_table(table)
        # Created without the marker, since it is never part of a row.
        assert set(got.columns) == {"order_id", "status"}
        assert_frames_match(
            got,
            pl.DataFrame({"order_id": [1, 2], "status": ["new", "new"]}),
        )
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_creates_on_the_first_run_and_merges_on_the_second(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # Two merges and nothing else: the first has no target to merge onto, so it
    # creates one and every row lands as an insert; the second finds that target
    # and applies a real delta to it. The create is ``IF NOT EXISTS``, so the
    # second run adds to what the first left rather than starting it over --
    # order 1, which the delta never mentions, is what proves it.
    db = store(domain_bigquery)
    table = "td_dest_merge_create_then_merge"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    first = _batch(
        tmp_path / "first.parquet",
        {
            "order_id": [1, 2, 3],
            "status": ["new", "new", "new"],
            "amount": [10, 20, 30],
            "op": ["i", "i", "i"],
        },
    )
    # Order 2 advances, order 3 is tombstoned, order 4 has never been seen.
    second = _batch(
        tmp_path / "second.parquet",
        {
            "order_id": [2, 3, 4],
            "status": ["shipped", "new", "new"],
            "amount": [25, 30, 40],
            "op": ["u", "d", "i"],
        },
    )
    db.drop_table(table)
    try:
        _merge_out(conn, work, table, first, merge_cfg)
        created = db.read_table(table)
        # The target the first run built: the batch's columns, without the
        # marker, and every row inserted.
        assert set(created.columns) == {"order_id", "status", "amount"}
        assert created.height == 3

        _merge_out(conn, work, table, second, merge_cfg)
        got = db.read_table(table)
        assert set(got.columns) == {"order_id", "status", "amount"}
        assert_frames_match(
            got,
            pl.DataFrame({
                "order_id": [1, 2, 4],
                "status": ["new", "shipped", "new"],
                "amount": [10, 25, 40],
            }),
        )
    finally:
        db.drop_table(table)


@pytest.mark.parametrize("template, table", list(_spelling_params("td_dest_merge_fqn")))
@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_resolves_table_name_spelling(
    domain_bigquery: BigQueryDomain,
    template: str,
    table: str,
    tmp_path: Path,
):
    # The target is created by a merge under its bare name and then merged into
    # through the spelling under test. Only a spelling that resolves to the same
    # physical table passes: a second one would take the delta as inserts of its
    # own and leave the first table as the seed made it.
    db = store(domain_bigquery)
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    spelled = template.format(
        project=domain_bigquery.project,
        dataset=domain_bigquery.dataset,
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
    db.drop_table(table)
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
# if_table_exists="merge" + schema_evolution
#
# "update" runs an Iceberg-style diff before the MERGE and adds the columns the
# batch has and the target does not; "strict" does not, so the target's schema is
# whatever it already was and only the shared columns are written.
# ---------------------------------------------------------------------------


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_takes_a_new_column_on_update(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    db = store(domain_bigquery)
    table = "td_dest_merge_evolve_column"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(tmp_path / "seed.parquet", {"order_id": [1], "status": ["new"]})
    # A second batch carrying a column the target does not have yet.
    widened = _batch(
        tmp_path / "widened.parquet",
        {"order_id": [1], "status": ["shipped"], "carrier": ["ups"], "op": ["u"]},
    )
    try:
        _seed_target(conn, work, table, seed)
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


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_adds_a_new_column_to_an_inserted_row(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # The added column is in the INSERT list too, not only the UPDATE one, so a
    # row that arrives as an insert carries its value rather than a NULL. Rows
    # already in the target keep NULL, since nothing ever supplied a value.
    db = store(domain_bigquery)
    table = "td_dest_merge_evolve_insert"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(tmp_path / "seed.parquet", {"order_id": [1], "status": ["new"]})
    widened = _batch(
        tmp_path / "widened.parquet",
        {"order_id": [2], "status": ["new"], "carrier": ["ups"], "op": ["i"]},
    )
    try:
        _seed_target(conn, work, table, seed)
        _merge_out(conn, work, table, widened, merge_cfg, "update")
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1, 2],
                "status": ["new", "new"],
                "carrier": [None, "ups"],
            }),
        )
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_widens_a_column_type_on_update(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # The batch's column is wider than the target's, and the base types alone
    # settle it: INT64 becomes FLOAT64. The column is retyped before the MERGE
    # runs, so the batch's own value lands rather than being refused.
    db = store(domain_bigquery)
    table = "td_dest_merge_widen_type"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {
            "order_id": pl.Series([1], dtype=pl.Int64),
            "amount": pl.Series([10], dtype=pl.Int64),
        },
    )
    widened = _batch(
        tmp_path / "widened.parquet",
        {
            "order_id": pl.Series([1], dtype=pl.Int64),
            "amount": pl.Series([10.5], dtype=pl.Float64),
            "op": ["u"],
        },
    )
    try:
        _seed_target(conn, work, table, seed)
        assert db.column_types(table)["amount"][0] == "INTEGER"

        _merge_out(conn, work, table, widened, merge_cfg, "update")
        assert db.column_types(table)["amount"][0] == "FLOAT"
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({"order_id": [1], "amount": [10.5]}),
        )
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_widens_a_numeric_by_its_parameters(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # Both sides are NUMERIC, so the base types say nothing and only the
    # precision and scale decide: the target holds 7 integer digits and 2
    # decimal ones, an unparameterized NUMERIC holds 29 and 9, so neither
    # budget shrinks and the widening is taken.
    #
    # The target is built with DDL because a parameterized type cannot be
    # loaded into being -- a parquet decimal lands as plain NUMERIC whatever
    # precision and scale it was written with. That is also why the batch's
    # side is unparameterized: a merge only ever meets parameters that were
    # already in the target.
    db = store(domain_bigquery)
    table = "td_dest_merge_widen_numeric"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    # Four decimal places, which a NUMERIC(9, 2) could not have held.
    widened = _batch(
        tmp_path / "widened.parquet",
        {
            "order_id": pl.Series([1], dtype=pl.Int64),
            "total": pl.Series([Decimal("1234.5678")], dtype=pl.Decimal(20, 4)),
            "op": ["i"],
        },
    )
    try:
        db.create_typed_table(table, "order_id INT64, total NUMERIC(9, 2)")
        assert db.column_types(table)["total"] == ("NUMERIC", 9, 2)

        _merge_out(conn, work, table, widened, merge_cfg, "update")
        assert db.column_types(table)["total"] == ("NUMERIC", None, None)
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({"order_id": [1], "total": [Decimal("1234.5678")]}),
        )
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_refuses_a_batch_that_would_narrow(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # The mirror of the widening case, and it does not go quietly: FLOAT64 down
    # to INT64 is no widening, so nothing is altered and the MERGE would write
    # the batch's column into an incompatible one. The delta is refused before
    # the statement is built, so the target is left as the seed made it.
    db = store(domain_bigquery)
    table = "td_dest_merge_narrow_type"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {
            "order_id": pl.Series([1], dtype=pl.Int64),
            "amount": pl.Series([10.5], dtype=pl.Float64),
        },
    )
    narrowed = _batch(
        tmp_path / "narrowed.parquet",
        {
            "order_id": pl.Series([1], dtype=pl.Int64),
            "amount": pl.Series([11], dtype=pl.Int64),
            "op": ["u"],
        },
    )
    try:
        _seed_target(conn, work, table, seed)
        with pytest.raises(BigQueryRuntimeException) as ei:
            _merge_out(conn, work, table, narrowed, merge_cfg, "update")
        assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_18

        assert db.column_types(table)["amount"][0] == "FLOAT"
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({"order_id": [1], "amount": [10.5]}),
        )
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_refuses_a_widened_batch_on_strict(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # Strict does not evolve, so a batch whose column widened is written into
    # the target's narrower one -- which BigQuery will not do. A type change in
    # the batch is therefore a hard failure under strict, not a no-op: the same
    # batch merges cleanly under "update", where the column is retyped first.
    db = store(domain_bigquery)
    table = "td_dest_merge_strict_type"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(
        tmp_path / "seed.parquet",
        {
            "order_id": pl.Series([1], dtype=pl.Int64),
            "amount": pl.Series([10], dtype=pl.Int64),
        },
    )
    widened = _batch(
        tmp_path / "widened.parquet",
        {
            "order_id": pl.Series([1], dtype=pl.Int64),
            "amount": pl.Series([10.5], dtype=pl.Float64),
            "op": ["u"],
        },
    )
    try:
        _seed_target(conn, work, table, seed)
        with pytest.raises(BigQueryRuntimeException) as ei:
            _merge_out(conn, work, table, widened, merge_cfg, "strict")
        assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_18
        assert db.column_types(table)["amount"][0] == "INTEGER"
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_keeps_the_target_schema_on_strict(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # The mirror of the evolving case: with no diff pass the write lists cover
    # the columns both sides share, so a batch carrying a new one merges what it
    # can and leaves the target's schema as the seed made it.
    db = store(domain_bigquery)
    table = "td_dest_merge_strict_column"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(tmp_path / "seed.parquet", {"order_id": [1], "status": ["new"]})
    widened = _batch(
        tmp_path / "widened.parquet",
        {"order_id": [1], "status": ["shipped"], "carrier": ["ups"], "op": ["u"]},
    )
    try:
        _seed_target(conn, work, table, seed)
        before = set(db.read_table(table).columns)

        _merge_out(conn, work, table, widened, merge_cfg, "strict")
        got = db.read_table(table)
        assert "carrier" not in got.columns
        assert set(got.columns) == before
        # The columns both sides share still merge.
        assert_frames_match(got, pl.DataFrame({"order_id": [1], "status": ["shipped"]}))
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_evolves_only_while_update_is_asked_for(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # The diff runs per write, so a target an earlier "update" run widened stops
    # taking new columns as soon as a run asks for "strict" -- and the column the
    # earlier run added stays.
    db = store(domain_bigquery)
    table = "td_dest_merge_evolution_off"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    merge_cfg = MergeCfg(on=["order_id"], op_column="op")
    seed = _batch(tmp_path / "seed.parquet", {"order_id": [1], "status": ["new"]})
    first = _batch(
        tmp_path / "first.parquet",
        {"order_id": [1], "status": ["packed"], "carrier": ["ups"], "op": ["u"]},
    )
    second = _batch(
        tmp_path / "second.parquet",
        {
            "order_id": [1],
            "status": ["shipped"],
            "carrier": ["dhl"],
            "eta": ["today"],
            "op": ["u"],
        },
    )
    try:
        _seed_target(conn, work, table, seed)
        _merge_out(conn, work, table, first, merge_cfg, "update")
        assert "carrier" in db.read_table(table).columns

        _merge_out(conn, work, table, second, merge_cfg, "strict")
        got = db.read_table(table)
        assert "eta" not in got.columns
        assert_frames_match(
            got,
            pl.DataFrame({
                "order_id": [1],
                "status": ["shipped"],
                "carrier": ["dhl"],
            }),
        )
    finally:
        db.drop_table(table)


@fs_domain("fs_employees_xs")
@bigquery_domain("bigquery")
def test_dest_plugin_write_out_loads_one_table_per_slot(
    domain_fs_employees_xs: FsDomain,
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    db = store(domain_bigquery)
    tables = ["td_dest_multi_a", "td_dest_multi_b"]
    work = work_dir(tmp_path)
    parquets = [
        provisioned_parquet(domain_fs_employees_xs, DEPARTMENTS),
        provisioned_parquet(domain_fs_employees_xs, DEPT_MANAGER),
    ]
    conn = dest_conn(domain_bigquery)
    try:
        plugin._BigQueryDestPlugin().write_out(
            PluginDestCtx(work),
            conn,
            BigQueryDest(
                tables=[table_fqn(t) for t in tables], if_table_exists="replace"
            ),
            parquets,
        )
        for table, parquet in zip(tables, parquets):
            expected = pl.read_parquet(parquet)
            got = db.read_table(table)
            assert got.height == expected.height
            assert set(got.columns) == set(expected.columns)
    finally:
        for table in tables:
            db.drop_table(table)

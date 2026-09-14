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


# A MERGE takes no load-job options, so the connector evolves its target with
# ALTER statements or not at all -- and the write has to say so.
_ICEBERG = {"bigquery.schema_evolution_engine": "iceberg"}


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
) -> None:
    plugin._BigQueryDestPlugin().write_out(
        PluginDestCtx(work) if trx_id is None else _TrxCtx(work, trx_id),
        conn,
        BigQueryDest(
            tables=[table_fqn(table)],
            if_table_exists="merge",
            merge_cfg=merge_cfg,
            schema_evolution=schema_evolution,
            dest_cfg=_ICEBERG,
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
# schema_evolution_engine="iceberg" on a bulk load
#
# BigQuery's schema_update_options only add a column or relax one to nullable,
# so a bulk load cannot widen one however the batch has grown. Diffing the
# target against the batch reaches what those cannot -- and refuses what
# BigQuery could not do at all.
#
# A target of a particular type has to be built with DDL: one the load job
# creates is inferred from the batch itself, so both sides would agree by
# construction and there would be nothing to widen.
# ---------------------------------------------------------------------------


def _load_out(conn, work: Path, table: str, parquet: Path, **dest_kwargs) -> None:
    plugin._BigQueryDestPlugin().write_out(
        PluginDestCtx(work),
        conn,
        BigQueryDest(
            tables=[table_fqn(table)],
            if_table_exists=dest_kwargs.pop("if_table_exists", "append"),
            schema_evolution=dest_kwargs.pop("schema_evolution", "update"),
            **dest_kwargs,
        ),
        [parquet],
    )


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_iceberg_engine_widens_a_numeric(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # An INT64 target against a NUMERIC batch: a widening BigQuery allows and
    # the load-job options cannot reach, so it is the plainest thing the engine
    # buys over native evolution.
    db = store(domain_bigquery)
    table = "td_dest_iceberg_widen_numeric"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    db.create_typed_table(table, "order_id INT64, amount INT64")
    batch = _batch(
        tmp_path / "priced.parquet",
        {
            "order_id": pl.Series([1], dtype=pl.Int64),
            "amount": pl.Series([Decimal("10.50")], dtype=pl.Decimal(20, 2)),
        },
    )
    try:
        _load_out(conn, work, table, batch, dest_cfg=_ICEBERG)
        assert db.column_types(table)["amount"][0] == "NUMERIC"
        assert db.count(table) == 1
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_native_engine_cannot_widen(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # The counterpart, and the reason the option exists: schema_update_options
    # add columns and relax nullability, so the retype has nowhere to go and
    # the load job refuses the batch.
    db = store(domain_bigquery)
    table = "td_dest_native_widen_numeric"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    db.create_typed_table(table, "order_id INT64, amount INT64")
    batch = _batch(
        tmp_path / "priced.parquet",
        {
            "order_id": pl.Series([1], dtype=pl.Int64),
            "amount": pl.Series([Decimal("10.50")], dtype=pl.Decimal(20, 2)),
        },
    )
    try:
        with pytest.raises(BigQueryRuntimeException):
            _load_out(conn, work, table, batch)
        assert db.column_types(table)["amount"][0] == "INTEGER"
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_iceberg_engine_adds_a_new_column(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # The half both engines can do, so the Iceberg pass has to do it too --
    # with the load-job options off, it is the only thing that would.
    db = store(domain_bigquery)
    table = "td_dest_iceberg_add_column"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
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


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_iceberg_engine_refuses_an_impossible_retype(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # No ALTER reconciles two kinds, so the write stops before the load rather
    # than after it has half-applied.
    db = store(domain_bigquery)
    table = "td_dest_iceberg_unsafe"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    db.create_typed_table(table, "order_id INT64, amount NUMERIC")
    before = db.column_types(table)
    batch = _batch(
        tmp_path / "lettered.parquet",
        {"order_id": [1], "amount": ["not a number"]},
    )
    try:
        with pytest.raises(BigQueryRuntimeException) as ei:
            _load_out(conn, work, table, batch, dest_cfg=_ICEBERG)
        assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_23
        assert db.column_types(table) == before
        assert db.count(table) == 0
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_iceberg_engine_still_refuses_a_widened_batch_on_strict(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # Strict pins the target's schema, and asking for the Iceberg engine does
    # not lift that -- the engine only says *how* a schema that is allowed to
    # move is moved. So the new column is never added, and the load job that
    # would have to write it fails: BigQuery does not drop a field the target
    # has no home for, it refuses the batch. That failure is the proof the
    # Iceberg pass held off; had it run, the column would be there and the load
    # would have succeeded.
    db = store(domain_bigquery)
    table = "td_dest_iceberg_strict"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    db.create_typed_table(table, "order_id INT64, status STRING")
    before = db.column_types(table)
    batch = _batch(
        tmp_path / "widened.parquet",
        {"order_id": [1], "status": ["new"], "priority": ["high"]},
    )
    try:
        with pytest.raises(BigQueryRuntimeException) as ei:
            _load_out(
                conn, work, table, batch, schema_evolution="strict", dest_cfg=_ICEBERG
            )
        assert ei.value.api_error is BigQueryErrorCode.BIGQUERY_7
        assert db.column_types(table) == before
        assert db.count(table) == 0
    finally:
        db.drop_table(table)


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_iceberg_engine_loads_a_same_shape_batch_on_strict(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # The other half: strict with nothing to evolve is an ordinary load. The
    # engine adds no statements of its own and the rows land.
    db = store(domain_bigquery)
    table = "td_dest_iceberg_strict_same_shape"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    db.create_typed_table(table, "order_id INT64, status STRING")
    before = db.column_types(table)
    batch = _batch(tmp_path / "same.parquet", {"order_id": [1], "status": ["new"]})
    try:
        _load_out(
            conn, work, table, batch, schema_evolution="strict", dest_cfg=_ICEBERG
        )
        assert db.column_types(table) == before
        assert db.count(table) == 1
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
    return MergeCfg(on=[on], op_column="op", watermark_column=_WATERMARK)


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_stamps_the_rows_it_writes(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # The target the merge creates is built from the staging table, and that is
    # stamped before the target is built from it, so even a first merge brings
    # the column into being and every inserted row carries the id.
    db = store(domain_bigquery)
    table = "td_dest_merge_watermark_stamp"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1, 2], "status": ["new", "new"], "op": ["i", "i"]},
    )
    try:
        _merge_out(conn, work, table, seed, _watermark_cfg(), trx_id="TRX_A")
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


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_skips_a_batch_the_trx_already_wrote(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # The point of the whole thing. The second write carries a different payload
    # for the same key and a new one besides, so applying it would be plainly
    # visible -- and it must not be, because the target already holds the trx's
    # stamp.
    db = store(domain_bigquery)
    table = "td_dest_merge_watermark_rerun"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1], "status": ["new"], "op": ["i"]},
    )
    rerun = _batch(
        tmp_path / "rerun.parquet",
        {"order_id": [1, 2], "status": ["shipped", "new"], "op": ["u", "i"]},
    )
    try:
        _merge_out(conn, work, table, seed, _watermark_cfg(), trx_id="TRX_A")
        _merge_out(conn, work, table, rerun, _watermark_cfg(), trx_id="TRX_A")
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


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_applies_a_batch_from_a_later_trx(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # The other half: the guard is on this trx's id, not on the column being
    # populated at all, so a later trx merges normally and restamps the rows it
    # touches. Order 1 keeps the older stamp because no clause claims it.
    db = store(domain_bigquery)
    table = "td_dest_merge_watermark_next_trx"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    seed = _batch(
        tmp_path / "seed.parquet",
        {"order_id": [1, 2], "status": ["new", "new"], "op": ["i", "i"]},
    )
    delta = _batch(
        tmp_path / "delta.parquet",
        {"order_id": [2, 3], "status": ["shipped", "new"], "op": ["u", "i"]},
    )
    try:
        _merge_out(conn, work, table, seed, _watermark_cfg(), trx_id="TRX_A")
        _merge_out(conn, work, table, delta, _watermark_cfg(), trx_id="TRX_B")
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


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_stamps_an_older_target_on_strict_too(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # Strict holds the target's schema still, and the watermark is the one column
    # it cannot hold back: without it nothing is ever stamped, so the guard would
    # read every run as the first. The seed merges without a watermark, the way a
    # flow that turned one on later would have been running, so its row predates
    # the column and keeps a NULL there -- nothing backfills this trx's id onto
    # rows it did not write.
    db = store(domain_bigquery)
    table = "td_dest_merge_watermark_strict"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
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
    plain = MergeCfg(on=["order_id"], op_column="op")
    try:
        _merge_out(conn, work, table, seed, plain, schema_evolution="strict")
        assert _WATERMARK not in db.column_types(table)

        _merge_out(
            conn,
            work,
            table,
            batch,
            _watermark_cfg(),
            schema_evolution="strict",
            trx_id="TRX_A",
        )
        assert db.column_types(table)[_WATERMARK] == ("STRING", None, None)

        _merge_out(
            conn,
            work,
            table,
            rerun,
            _watermark_cfg(),
            schema_evolution="strict",
            trx_id="TRX_A",
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


@bigquery_domain("bigquery")
def test_dest_plugin_write_out_merge_adds_only_the_watermark_on_strict(
    domain_bigquery: BigQueryDomain,
    tmp_path: Path,
):
    # The watermark is an exception to strict, not a hole in it: the rest of a
    # widened batch is still held back, so `priority` has nowhere to land and
    # goes unwritten while `wm` is added.
    db = store(domain_bigquery)
    table = "td_dest_merge_watermark_strict_widened"
    work = work_dir(tmp_path)
    conn = dest_conn(domain_bigquery)
    seed = _batch(tmp_path / "seed.parquet", {"order_id": [1], "status": ["new"]})
    widened = _batch(
        tmp_path / "widened.parquet",
        {
            "order_id": [1],
            "status": ["shipped"],
            "priority": ["high"],
            "op": ["u"],
        },
    )
    try:
        _seed_target(conn, work, table, seed)
        _merge_out(
            conn,
            work,
            table,
            widened,
            _watermark_cfg(),
            schema_evolution="strict",
            trx_id="TRX_A",
        )
        assert "priority" not in db.column_types(table)
        assert_frames_match(
            db.read_table(table),
            pl.DataFrame({
                "order_id": [1],
                "status": ["shipped"],
                _WATERMARK: ["TRX_A"],
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

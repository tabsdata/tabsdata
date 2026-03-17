#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import logging
import math
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import polars as pl

import tabsdata as td
from tabsdata.connector.cdc.common import CdcTrigger
from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.common.constants import (
    CDC_DATA_COL_NEW_PREFIX,
    CDC_DATA_COL_OLD_PREFIX,
    CDC_DATA_MAP_NEW,
    CDC_DATA_MAP_OLD,
    CDC_DATA_ROW_NEW,
    CDC_DATA_ROW_OLD,
    CDC_META_OP,
    CDC_META_SQ,
    CDC_META_TX,
    CdcOperation,
)
from tabsdata.connector.cdc.common.typing import CdcFormatSpec
from tabsdata._io.inputs.stage_input import (
    STAGE_MANIFEST_FILE_PATTERN,
    _deserialize_manifest,
    _read_manifest_file, Stage,
)
from tabsdata._tabsserver.function.global_utils import convert_uri_to_path
from tabsdata._utils.tableframe._common import drop_system_columns
from test_tabsdata.fntest import tdfnjob, tdfnrun, tdfntrigger
from test_tabsdata.test_tableframe.common import pretty_pandas, pretty_polars
from test_tabsdata_cdc_common.common.sql import CdcSampler
from test_tabsdata_cdc_db2.common.sql import Db2CdcSampler

import pytest

logger = logging.getLogger(__name__)

Q_POLL_INSERTS = 8
Q_POLL_UPDATES = 8
Q_POLL_DELETES = 8

TRIGGER_END_MINUTES = 30
TRIGGER_STOP_END_OFFSET_SECONDS = 60

TRIGGER_INIT_TIMEOUT_SECONDS = 60
TRIGGER_INIT_POLL_SECONDS = 0.5
TRIGGER_STOP_JOIN_TIMEOUT_SECONDS = 60

Q_TRANSACTIONS = 64
Q_CONTINUOUS_INSERTS = 16
Q_CONTINUOUS_UPDATES = 8
Q_CONTINUOUS_DELETES = 4
MINIMUM_TRANSACTION_TIME = 1.0

POLL_TIMEOUT_SECONDS = 120
POLL_INTERVAL_SECONDS = 2

continuous_dml_params = pytest.mark.parametrize(
    ("q_tables", "q_threads"),
    [
        pytest.param(
            q_tables,
            q_threads,
            id=f"tables={q_tables}-threads={q_threads}",)
        for q_tables in [1, 2,4]
        for q_threads in [1, 2, 4]
    ],
)

CDC_FORMAT_PARAMS = [
    pytest.param(
        CdcFormatSpec(values_format="columns", flat_new_values=True),
        id="columns-flat",
    ),
    pytest.param(
        CdcFormatSpec(values_format="columns", flat_new_values=False),
        id="columns-prefixed",
    ),
    pytest.param(
        CdcFormatSpec(values_format="struct", flat_new_values=True),
        id="struct-flat",
    ),
    pytest.param(
        CdcFormatSpec(values_format="struct", flat_new_values=False),
        id="struct-nested",
    ),
    pytest.param(
        CdcFormatSpec(values_format="map", flat_new_values=True),
        id="map-flat",
    ),
    pytest.param(
        CdcFormatSpec(values_format="map", flat_new_values=False),
        id="map-nested",
    ),
]


def fuzzy_eq(actual: list, expected: list, column: str, float_columns: list[str]) -> bool:
    if column not in float_columns:
        return actual == expected
    if len(actual) != len(expected):
        return False
    for actual_v, expected_v in zip(actual, expected):
        if actual_v is None and expected_v is None:
            continue
        if actual_v is None or expected_v is None:
            return False
        if not math.isclose(actual_v, expected_v, rel_tol=1e-6):
            return False
    return True


def _run_dml_operations(
    sampler: CdcSampler,
    connection: Any,
    table: TableObject,
) -> pl.DataFrame:
    sampler.run(
        table=table,
        inserts=Q_POLL_INSERTS,
        updates=0,
        deletes=0,)
    connection.commit()

    sampler.run(
        table=table,
        inserts=0,
        updates=Q_POLL_UPDATES,
        deletes=0,)
    connection.commit()

    sampler.run(
        table=table,
        inserts=0,
        updates=0,
        deletes=Q_POLL_DELETES,)
    connection.commit()

    expected = sampler.t_changes(table)
    pretty_polars()
    pretty_pandas()

    logger.debug("--- Expected Changes ---")
    logger.debug(f"\n{expected}")

    return expected


def _build_publisher(
    trigger: td.StageTrigger,
) -> td.TabsdataFunction | Callable[..., Any]:
    @td.publisher(
        source=td.Stage(use_existing_data=True),
        tables="output",
        name=str(uuid.uuid4()),
        trigger_by=trigger,
    )
    def process(tfs: list[td.TableFrame]) -> td.TableFrame:
        return td.concat(tfs)

    return process


def _validate_cdc_output(
    df: pl.DataFrame,
    expected: pl.DataFrame,
    sampler: CdcSampler,
) -> None:
    logger.debug("--- Actual Output ---")
    logger.debug(f"\n{df}")

    logger.debug("--- Actual Schema ---")
    logger.debug(f"\n{df.schema}")

    assert df.height == expected.height, (
        f"Expected {expected.height} rows, got {df.height}"
    )

    ops = df[CDC_META_OP].to_list()
    expected_ops = expected["op"].to_list()
    for op_type in [CdcOperation.INSERT, CdcOperation.UPDATE, CdcOperation.DELETE]:
        actual_count = ops.count(op_type)
        expected_count = expected_ops.count(op_type)
        assert actual_count == expected_count, (
            f"Expected {expected_count} {op_type}, got {actual_count}"
        )

    for op_type in [CdcOperation.INSERT, CdcOperation.UPDATE, CdcOperation.DELETE]:
        actual_pks = sorted(
            df.filter(pl.col(CDC_META_OP) == op_type)[sampler.fold("pk")].to_list()
        )
        expected_pks = sorted(
            expected.filter(pl.col("op") == op_type)[f"new.{sampler.fold('pk')}"].to_list()
        )
        assert actual_pks == expected_pks, (
            f"PK mismatch for {op_type}: {actual_pks} != {expected_pks}"
        )

    for op_type in [CdcOperation.INSERT, CdcOperation.UPDATE, CdcOperation.DELETE]:
        actual_rows = df.filter(pl.col(CDC_META_OP) == op_type).sort(sampler.fold("pk"))
        expected_rows = expected.filter(pl.col("op") == op_type).sort(f"new.{sampler.fold('pk')}")
        for column in sampler.columns:
            actual_values = actual_rows[column].to_list()
            expected_values = expected_rows[f"new.{column}"].to_list()
            assert fuzzy_eq(actual_values, expected_values, column, sampler.float_columns), (
                f"New value mismatch for {column} in {op_type}:\n"
                f"  actual:   {actual_values}\n"
                f"  expected: {expected_values}\n"
                f"  actual types:   {[type(actual_v).__name__ for actual_v in actual_values]}\n"
                f"  expected types: {[type(expected_v).__name__ for expected_v in expected_values]}"
            )

    insert_rows = df.filter(pl.col(CDC_META_OP) == CdcOperation.INSERT)
    delete_rows = df.filter(pl.col(CDC_META_OP) == CdcOperation.DELETE)
    for column in sampler.columns:
        old_col = f"{CDC_DATA_COL_OLD_PREFIX}.{column}"
        assert insert_rows[old_col].is_null().all(), (
            f"Old {column} should be null for inserts"
        )
        assert delete_rows[old_col].is_null().all(), (
            f"Old {column} should be null for deletes"
        )

    update_actual = df.filter(pl.col(CDC_META_OP) == CdcOperation.UPDATE).sort(sampler.fold("pk"))
    update_expected = expected.filter(
        pl.col("op") == CdcOperation.UPDATE
    ).sort(f"new.{sampler.fold('pk')}")
    for column in sampler.columns:
        old_col = f"{CDC_DATA_COL_OLD_PREFIX}.{column}"
        actual_values = update_actual[old_col].to_list()
        expected_values = update_expected[f"old.{column}"].to_list()
        assert fuzzy_eq(actual_values, expected_values, column, sampler.float_columns), (
            f"Old value mismatch for {column} in updates:\n"
            f"  actual:   {actual_values}\n"
            f"  expected: {expected_values}"
            f"  actual types:   {[type(actual_v).__name__ for actual_v in actual_values]}\n"
            f"  expected types: {[type(expected_v).__name__ for expected_v in expected_values]}"
        )

    insert_txs = df.filter(
        pl.col(CDC_META_OP) == CdcOperation.INSERT
    )[CDC_META_TX].unique()
    update_txs = df.filter(
        pl.col(CDC_META_OP) == CdcOperation.UPDATE
    )[CDC_META_TX].unique()
    delete_txs = df.filter(
        pl.col(CDC_META_OP) == CdcOperation.DELETE
    )[CDC_META_TX].unique()

    logger.debug("--- Transactions ---")
    logger.debug(f"Insert Transactions: {insert_txs.to_list()}")
    logger.debug(f"Update Transactions: {update_txs.to_list()}")
    logger.debug(f"Delete Transactions: {delete_txs.to_list()}")

    assert len(insert_txs) == 1, "All inserts should share one transaction"
    assert len(update_txs) == 1, "All updates should share one transaction"
    assert len(delete_txs) == 1, "All deletes should share one transaction"
    all_txs = pl.concat([insert_txs, update_txs, delete_txs]).unique()
    assert len(all_txs) == 3, (
        "Each operation type should have a different transaction id"
    )

    for op_type, op_label in [
        (CdcOperation.INSERT, "insert"),
        (CdcOperation.UPDATE, "update"),
        (CdcOperation.DELETE, "delete"),
    ]:
        seqs = df.filter(
            pl.col(CDC_META_OP) == op_type
        ).sort(CDC_META_SQ)[CDC_META_SQ].to_list()
        assert seqs == sorted(seqs), (
            f"Sequences within {op_label} transaction should be monotonically increasing"
        )
        if not isinstance(sampler, Db2CdcSampler):
            assert seqs == list(range(1, len(seqs) + 1)), (
                f"Sequences within {op_label} transaction should be 1..{len(seqs)}, got {seqs}"
            )

    insert_tx = insert_txs.to_list()[0]
    update_tx = update_txs.to_list()[0]
    delete_tx = delete_txs.to_list()[0]
    assert insert_tx < update_tx < delete_tx, (
        f"Transaction IDs should be ordered: {insert_tx} < {update_tx} < {delete_tx}"
    )


def run_poll_changes_test(
    sampler: CdcSampler,
    connection: Any,
    table: TableObject,
    trigger: CdcTrigger,
) -> None:
    expected = _run_dml_operations(sampler, connection, table)
    process = _build_publisher(trigger)

    def job():
        return tdfntrigger(trigger=trigger, function=process)

    fork = _fork_trigger(trigger, job)
    staging = Path(fork.context.staging_folder)

    _poll_until_captured(staging, trigger, expected.height)
    _stop_trigger(trigger, fork)

    fn_trigger = fork.result
    fn_trigger.check()
    fn_trigger.integrity()

    workdir = staging.parent
    parquet_files = sorted(staging.glob("*.parquet")) if staging.exists() else []
    if not parquet_files:
        parquet_files = sorted(workdir.glob("*.parquet"))

    assert len(parquet_files) > 0, (
        f"No parquet files found in staging ({staging}) or work dir ({workdir})"
    )

    df = pl.concat([pl.read_parquet(f) for f in parquet_files])
    _validate_cdc_output(df, expected, sampler)


def run_end_to_end_test(
    sampler: CdcSampler,
    connection: Any,
    table: TableObject,
    trigger: CdcTrigger,
) -> None:
    expected = _run_dml_operations(sampler, connection, table)
    process = _build_publisher(trigger)

    def job():
        return tdfnjob(trigger=trigger, function=process, function_defined_on=0)

    fork = _fork_trigger(trigger, job)
    staging = Path(fork.context.staging_folder)

    _poll_until_captured(staging, trigger, expected.height)
    _stop_trigger(trigger, fork)

    fn_trigger, fn_run = fork.result
    fn_trigger.check()

    workdir = staging.parent
    parquet_files = sorted(staging.glob("*.parquet")) if staging.exists() else []
    if not parquet_files:
        parquet_files = sorted(workdir.glob("*.parquet"))

    assert len(parquet_files) > 0, (
        f"No parquet files found in staging ({staging}) or work dir ({workdir})"
    )

    df = pl.concat([pl.read_parquet(f) for f in parquet_files])
    _validate_cdc_output(df, expected, sampler)

    output_tables = fn_run.output.as_pl_df()
    assert len(output_tables) == 1, (
        f"Expected 1 output table, got {len(output_tables)}"
    )

    output_df = drop_system_columns(output_tables[0].lazy()).collect()
    staged_df = drop_system_columns(df.lazy()).collect()
    columns = sorted(staged_df.columns)
    staged_sorted = staged_df.select(columns).sort(columns)
    output_sorted = output_df.select(columns).sort(columns)
    assert staged_sorted.equals(output_sorted), (
        "Function output is not equivalent to staged data"
    )


def _normalize_to_canonical(
    df: pl.DataFrame,
    sampler: CdcSampler,
    cdc_format: CdcFormatSpec,
) -> pl.DataFrame:
    exprs: list[Any] = [
        pl.col(CDC_META_OP),
        pl.col(CDC_META_TX),
        pl.col(CDC_META_SQ),
    ]

    for column in sampler.columns:
        if cdc_format.flat_new_values:
            exprs.append(pl.col(column))
        else:
            match cdc_format.values_format:
                case "columns":
                    exprs.append(
                        pl.col(f"{CDC_DATA_COL_NEW_PREFIX}.{column}").alias(column)
                    )
                case "struct":
                    exprs.append(
                        pl.col(CDC_DATA_ROW_NEW).struct.field(column).alias(column)
                    )

    for column in sampler.columns:
        old_col = f"{CDC_DATA_COL_OLD_PREFIX}.{column}"
        match cdc_format.values_format:
            case "columns":
                exprs.append(pl.col(old_col))
            case "struct":
                exprs.append(
                    pl.col(CDC_DATA_ROW_OLD).struct.field(column).alias(old_col)
                )

    return df.select(exprs)


def _validate_cdc_map_output(
    df: pl.DataFrame,
    expected: pl.DataFrame,
    sampler: CdcSampler,
    cdc_format: CdcFormatSpec,
) -> None:
    logger.debug("--- Actual Output ---")
    logger.debug(f"\n{df}")

    logger.debug("--- Actual Schema ---")
    logger.debug(f"\n{df.schema}")

    assert df.height == expected.height, (
        f"Expected {expected.height} rows, got {df.height}"
    )

    ops = df[CDC_META_OP].to_list()
    expected_ops = expected["op"].to_list()
    for op_type in [CdcOperation.INSERT, CdcOperation.UPDATE, CdcOperation.DELETE]:
        actual_count = ops.count(op_type)
        expected_count = expected_ops.count(op_type)
        assert actual_count == expected_count, (
            f"Expected {expected_count} {op_type}, got {actual_count}"
        )

    df_columns = set(df.columns)
    if cdc_format.flat_new_values:
        for column in sampler.columns:
            assert column in df_columns, (
                f"Expected flat new column {column!r} not in schema"
            )
    else:
        assert CDC_DATA_MAP_NEW in df_columns, (
            f"Expected map new column {CDC_DATA_MAP_NEW!r} not in schema"
        )
    assert CDC_DATA_MAP_OLD in df_columns, (
        f"Expected map old column {CDC_DATA_MAP_OLD!r} not in schema"
    )

    if not cdc_format.flat_new_values:
        for v in df[CDC_DATA_MAP_NEW].to_list():
            parsed = json.loads(v)
            assert isinstance(parsed, dict)
            for column in sampler.columns:
                assert column in parsed, (
                    f"Key {column!r} missing from new map"
                )

    for v in df[CDC_DATA_MAP_OLD].to_list():
        parsed = json.loads(v)
        assert isinstance(parsed, dict)
        for column in sampler.columns:
            assert column in parsed, (
                f"Key {column!r} missing from old map"
            )

    pk_col = sampler.fold("pk")
    for op_type in [CdcOperation.INSERT, CdcOperation.UPDATE, CdcOperation.DELETE]:
        filtered = df.filter(pl.col(CDC_META_OP) == op_type)
        if cdc_format.flat_new_values:
            actual_pks = sorted(filtered[pk_col].to_list())
        else:
            actual_pks = sorted(
                json.loads(v).get(pk_col)
                for v in filtered[CDC_DATA_MAP_NEW].to_list()
            )
        expected_pks = sorted(
            expected.filter(
                pl.col("op") == op_type
            )[f"new.{pk_col}"].to_list()
        )
        assert actual_pks == expected_pks, (
            f"PK mismatch for {op_type}: {actual_pks} != {expected_pks}"
        )

    insert_rows = df.filter(pl.col(CDC_META_OP) == CdcOperation.INSERT)
    delete_rows = df.filter(pl.col(CDC_META_OP) == CdcOperation.DELETE)
    for rows, op_label in [
        (insert_rows, "inserts"),
        (delete_rows, "deletes"),
    ]:
        for v in rows[CDC_DATA_MAP_OLD].to_list():
            parsed = json.loads(v)
            for column in sampler.columns:
                assert parsed.get(column) is None, (
                    f"Old {column} should be null for {op_label}"
                )

    update_rows = df.filter(pl.col(CDC_META_OP) == CdcOperation.UPDATE)
    for v in update_rows[CDC_DATA_MAP_OLD].to_list():
        parsed = json.loads(v)
        non_null = [
            column for column in sampler.columns
            if parsed.get(column) is not None
        ]
        assert len(non_null) > 0, "Old values should not be all null for updates"

    insert_txs = df.filter(
        pl.col(CDC_META_OP) == CdcOperation.INSERT
    )[CDC_META_TX].unique()
    update_txs = df.filter(
        pl.col(CDC_META_OP) == CdcOperation.UPDATE
    )[CDC_META_TX].unique()
    delete_txs = df.filter(
        pl.col(CDC_META_OP) == CdcOperation.DELETE
    )[CDC_META_TX].unique()

    assert len(insert_txs) == 1, "All inserts should share one transaction"
    assert len(update_txs) == 1, "All updates should share one transaction"
    assert len(delete_txs) == 1, "All deletes should share one transaction"
    all_txs = pl.concat([insert_txs, update_txs, delete_txs]).unique()
    assert len(all_txs) == 3, (
        "Each operation type should have a different transaction id"
    )

    for op_type, op_label in [
        (CdcOperation.INSERT, "insert"),
        (CdcOperation.UPDATE, "update"),
        (CdcOperation.DELETE, "delete"),
    ]:
        seqs = df.filter(
            pl.col(CDC_META_OP) == op_type
        ).sort(CDC_META_SQ)[CDC_META_SQ].to_list()
        assert seqs == sorted(seqs), (
            f"Sequences within {op_label} transaction should be monotonically increasing"
        )
        if not isinstance(sampler, Db2CdcSampler):
            assert seqs == list(range(1, len(seqs) + 1)), (
                f"Sequences within {op_label} transaction should be 1..{len(seqs)}, got {seqs}"
            )

    insert_tx = insert_txs.to_list()[0]
    update_tx = update_txs.to_list()[0]
    delete_tx = delete_txs.to_list()[0]
    assert insert_tx < update_tx < delete_tx, (
        f"Transaction IDs should be ordered: {insert_tx} < {update_tx} < {delete_tx}"
    )


def _validate_cdc_format_output(
    df: pl.DataFrame,
    expected: pl.DataFrame,
    sampler: CdcSampler,
    cdc_format: CdcFormatSpec,
) -> None:
    if cdc_format.values_format in ("columns", "struct"):
        normalized = _normalize_to_canonical(df, sampler, cdc_format)
        _validate_cdc_output(normalized, expected, sampler)
    else:
        _validate_cdc_map_output(df, expected, sampler, cdc_format)


def run_offset_resume_test(
    sampler: CdcSampler,
    connection: Any,
    table: TableObject,
    trigger_factory: Callable[[int, pl.DataFrame | None], CdcTrigger],
    iterations: int = 4,
    inserts_per_iteration: int = 32,
) -> None:
    checkpoint_df: pl.DataFrame | None = None

    for i in range(iterations):
        sampler._changes.pop(table, None)

        sampler.run(table=table, inserts=inserts_per_iteration, updates=0, deletes=0)
        connection.commit()

        expected = sampler.t_changes(table)
        pk_col = sampler.fold("pk")

        pretty_polars()
        pretty_pandas()

        logger.debug(f"--- Iteration {i + 1}/{iterations} ---")
        logger.debug(f"--- Expected Changes (iteration {i + 1}) ---")
        logger.debug(f"\n{expected}")

        trigger = trigger_factory(i, checkpoint_df)
        process = _build_publisher(trigger)

        def job():
            return tdfntrigger(trigger=trigger, function=process)

        fork = _fork_trigger(trigger, job)
        staging = Path(fork.context.staging_folder)

        _poll_until_captured(staging, trigger, inserts_per_iteration)
        _stop_trigger(trigger, fork)

        fn_trigger = fork.result
        fn_trigger.check()
        fn_trigger.integrity()

        workdir = staging.parent
        parquet_files = sorted(staging.glob("*.parquet")) if staging.exists() else []
        if not parquet_files:
            parquet_files = sorted(workdir.glob("*.parquet"))

        assert len(parquet_files) > 0, (
            f"Iteration {i + 1}: no parquet files found in staging ({staging}) "
            f"or work dir ({workdir})"
        )

        df = pl.concat([pl.read_parquet(f) for f in parquet_files])

        logger.debug(f"--- Actual Output (iteration {i + 1}) ---")
        logger.debug(f"\n{df}")

        assert df.height == expected.height, (
            f"Iteration {i + 1}: expected {expected.height} rows, got {df.height}"
        )

        ops = df[CDC_META_OP].to_list()
        assert all(op == CdcOperation.INSERT for op in ops), (
            f"Iteration {i + 1}: expected all INSERT's, "
            f"got {[op for op in ops if op != CdcOperation.INSERT]}"
        )

        actual_pks = sorted(df[pk_col].to_list())
        expected_pks = sorted(
            expected.filter(
                pl.col("op") == CdcOperation.INSERT
            )[f"new.{pk_col}"].to_list()
        )
        assert actual_pks == expected_pks, (
            f"Iteration {i + 1}: PK mismatch: {actual_pks} != {expected_pks}"
        )

        checkpoint_df = fn_trigger.context.checkpoint.read()

        logger.debug(f"--- Checkpoint (iteration {i + 1}) ---")
        logger.debug(f"\n{checkpoint_df}")
        logger.debug(f"--- Iteration {i + 1}/{iterations} PASSED ---")


def run_end_to_end_format_test(
    sampler: CdcSampler,
    connection: Any,
    table: TableObject,
    trigger: CdcTrigger,
    cdc_format: CdcFormatSpec,
) -> None:
    expected = _run_dml_operations(sampler, connection, table)
    process = _build_publisher(trigger)

    def job():
        return tdfnjob(trigger=trigger, function=process, function_defined_on=0)

    fork = _fork_trigger(trigger, job)
    staging = Path(fork.context.staging_folder)

    _poll_until_captured(staging, trigger, expected.height)
    _stop_trigger(trigger, fork)

    fn_trigger, fn_run = fork.result
    fn_trigger.check()

    workdir = staging.parent
    parquet_files = sorted(staging.glob("*.parquet")) if staging.exists() else []
    if not parquet_files:
        parquet_files = sorted(workdir.glob("*.parquet"))

    assert len(parquet_files) > 0, (
        f"No parquet files found in staging ({staging}) or work dir ({workdir})"
    )

    df = pl.concat([pl.read_parquet(f) for f in parquet_files])
    _validate_cdc_format_output(df, expected, sampler, cdc_format)

    output_tables = fn_run.output.as_pl_df()
    assert len(output_tables) == 1, (
        f"Expected 1 output table, got {len(output_tables)}"
    )

    output_df = drop_system_columns(output_tables[0].lazy()).collect()
    staged_df = drop_system_columns(df.lazy()).collect()
    columns = sorted(staged_df.columns)
    staged_sorted = staged_df.select(columns).sort(columns)
    output_sorted = output_df.select(columns).sort(columns)
    assert staged_sorted.equals(output_sorted), (
        "Function output is not equivalent to staged data"
    )


def _count_staged_rows(staging: Path) -> int:
    working = staging.parent
    total = 0
    for pf in working.glob("*.parquet"):
        # noinspection PyBroadException
        try:
            total += pl.read_parquet(pf).height
        except Exception:
            pass
    return total


@dataclass
class TriggerFork:
    context: Any = None
    thread: threading.Thread = None
    result: Any = None
    error: Exception = None


def _fork_trigger(
    trigger: td.StageTrigger,
    job: Callable[..., Any],
) -> TriggerFork:
    captured: dict[str, Any] = {}
    trigger_run = trigger.run

    def trigger_run_wrapper(context):
        captured['context'] = context
        return trigger_run(context)

    trigger.run = trigger_run_wrapper

    fork = TriggerFork()

    def worker():
        try:
            fork.result = job()
        except Exception as error:
            fork.error = error

    fork.thread = threading.Thread(target=worker, daemon=True)
    fork.thread.start()

    deadline = time.monotonic() + TRIGGER_INIT_TIMEOUT_SECONDS
    while 'context' not in captured and time.monotonic() < deadline:
        if fork.error is not None:
            raise fork.error
        time.sleep(TRIGGER_INIT_POLL_SECONDS)
    if fork.error is not None:
        raise fork.error
    assert 'context' in captured, (
        f"Trigger did not initialize within {TRIGGER_INIT_TIMEOUT_SECONDS}s"
    )

    fork.context = captured['context']
    return fork


def _poll_until_captured(
    staging: Path,
    trigger: CdcTrigger,
    expected_rows: int,
    poll_timeout_seconds: int = POLL_TIMEOUT_SECONDS,
    poll_interval_seconds: float = POLL_INTERVAL_SECONDS,
) -> int:
    logger.debug(f"--- Waiting for CDC capture (timeout {poll_timeout_seconds}s) ---")
    poll_deadline = time.monotonic() + poll_timeout_seconds
    total_actual = 0
    while time.monotonic() < poll_deadline:
        # noinspection PyBroadException
        try:
            staged = _count_staged_rows(staging)
            buffered = trigger._buffered_rows
            total_actual = staged + buffered
        except Exception:
            staged = buffered = 0
            total_actual = 0
        logger.debug(
            f"Captured rows: {total_actual}/{expected_rows} "
            f"(staged={staged}, buffered={buffered})"
        )
        if total_actual >= expected_rows:
            logger.debug("All rows captured.")
            break
        time.sleep(poll_interval_seconds)
    else:
        logger.debug(f"Poll timeout reached at {total_actual}/{expected_rows} rows.")
    return total_actual


def _stop_trigger(
    trigger: CdcTrigger,
    fork: TriggerFork,
) -> None:
    fork.context.end = datetime.now(timezone.utc) - timedelta(seconds=TRIGGER_STOP_END_OFFSET_SECONDS)
    logger.debug("--- Trigger stop signal sent ---")

    fork.thread.join(timeout=TRIGGER_STOP_JOIN_TIMEOUT_SECONDS)
    assert not fork.thread.is_alive(), f"Trigger thread did not stop within {TRIGGER_STOP_JOIN_TIMEOUT_SECONDS}s"

    staging = Path(fork.context.staging_folder)
    final_staged = _count_staged_rows(staging)
    final_buffered = trigger._buffered_rows
    logger.debug(
        f"After stop: staged={final_staged}, buffered={final_buffered}, "
        f"total={final_staged + final_buffered}"
    )

    assert final_buffered == 0, f"Buffer not fully flushed: {final_buffered} rows remaining"

    if fork.error is not None:
        raise fork.error


def _validate_continuous_output(
    staging: Path,
    tables: list[TableObject],
    sampler: CdcSampler,
) -> None:
    manifest_files = sorted(staging.glob(STAGE_MANIFEST_FILE_PATTERN))
    assert len(manifest_files) > 0, f"No manifest files found in {staging}"

    files_by_index: dict[int, list[Path]] = {}
    for mf in manifest_files:
        content = _read_manifest_file(str(mf))
        manifest = _deserialize_manifest(content)
        for idx, entry in manifest.entries.items():
            files_by_index.setdefault(idx, []).extend(
                Path(convert_uri_to_path(uri)) for uri in entry.files
            )

    pk_col = sampler.fold("pk")

    for i, table in enumerate(tables):
        parquet_files = files_by_index.get(i, [])
        assert len(parquet_files) > 0, (
            f"No parquet files for table {table} (index {i})"
        )

        df = pl.concat([pl.read_parquet(f) for f in parquet_files])
        expected = sampler.t_changes(table)

        logger.debug(f"--- Table {table} (index {i}) ---")
        logger.debug(f"Actual rows: {df.height}")
        logger.debug(f"Expected rows: {expected.height}")

        assert df.height == expected.height, (
            f"Table {table}: expected {expected.height} rows, got {df.height}"
        )

        ops = df[CDC_META_OP].to_list()
        expected_ops = expected["op"].to_list()
        for op_type in [CdcOperation.INSERT, CdcOperation.UPDATE, CdcOperation.DELETE]:
            actual_count = ops.count(op_type)
            expected_count = expected_ops.count(op_type)
            assert actual_count == expected_count, (
                f"Table {table}: expected {expected_count} {op_type}, "
                f"got {actual_count}"
            )

        for op_type in [CdcOperation.INSERT, CdcOperation.UPDATE, CdcOperation.DELETE]:
            actual_pks = sorted(
                df.filter(pl.col(CDC_META_OP) == op_type)[pk_col].to_list()
            )
            expected_pks = sorted(
                expected.filter(pl.col("op") == op_type)[f"new.{pk_col}"].to_list()
            )
            assert actual_pks == expected_pks, (
                f"Table {table}: PK mismatch for {op_type}: "
                f"{actual_pks} != {expected_pks}"
            )

        for op_type in [CdcOperation.INSERT, CdcOperation.UPDATE, CdcOperation.DELETE]:
            actual_rows = df.filter(pl.col(CDC_META_OP) == op_type).sort(sampler.columns)
            expected_rows = expected.filter(pl.col("op") == op_type).sort([f"new.{c}" for c in sampler.columns])
            for column in sampler.columns:
                actual_values = actual_rows[column].to_list()
                expected_values = expected_rows[f"new.{column}"].to_list()
                assert fuzzy_eq(actual_values, expected_values, column, sampler.float_columns), (
                    f"Table {table}: value mismatch for {column} in {op_type}:\n"
                    f"  actual:   {actual_values}\n"
                    f"  expected: {expected_values}"
                )

    logger.debug("--- Continuous validation PASSED ---")


def _run_dml_transactions(
    sampler: CdcSampler,
    connection: Any,
    tables: list[TableObject],
) -> None:
    for transaction in range(Q_TRANSACTIONS):
        transaction_start = time.monotonic()
        for table in tables:
            sampler.run(
                table=table,
                inserts=Q_CONTINUOUS_INSERTS,
                updates=Q_CONTINUOUS_UPDATES,
                deletes=Q_CONTINUOUS_DELETES,
            )
        connection.commit()
        remaining = MINIMUM_TRANSACTION_TIME - (time.monotonic() - transaction_start)
        if remaining > 0:
            time.sleep(remaining)


def run_continuous_dml_test(
    sampler: CdcSampler,
    connection: Any,
    tables: list[TableObject],
    trigger: CdcTrigger,
    dml_worker_threads: int = 1,
    dml_worker_factory: Callable[[], tuple[Any, CdcSampler]] | None = None,
) -> None:
    output_names = [f"output_{i}" for i in range(len(tables))]

    @td.publisher(
        source=Stage(use_existing_data=True),
        tables=output_names,
        name=str(uuid.uuid4()),
        trigger_by=trigger,
    )
    def process(*tfs: list[td.TableFrame],) -> tuple[td.TableFrame, ...]:
        return tuple(td.concat(tf_list) for tf_list in tfs)

    def job():
        return tdfntrigger(trigger=trigger, function=process)

    fork = _fork_trigger(trigger, job)

    pretty_polars()
    pretty_pandas()

    if dml_worker_threads <= 1:
        _run_dml_transactions(sampler, connection, tables)
    else:
        if dml_worker_factory is None:
            raise ValueError(
                "dml_worker_factory is required when dml_threads > 1"
            )

        workers: list[tuple[Any, CdcSampler]] = []
        for _ in range(dml_worker_threads):
            workers.append(dml_worker_factory())

        dml_errors: list[Exception] = []
        dml_threads: list[threading.Thread] = []

        def dml_target(worker_sampler: CdcSampler,worker_conn: Any) -> None:
            try:
                _run_dml_transactions(worker_sampler, worker_conn, tables)
            except Exception as e:
                dml_errors.append(e)

        for w_conn, w_sampler in workers:
            thread = threading.Thread(
                target=dml_target,
                args=(w_sampler, w_conn),
                daemon=True,
            )
            dml_threads.append(thread)
            thread.start()

        for thread in dml_threads:
            thread.join()

        if dml_errors:
            raise dml_errors[0]

        for _, w_sampler in workers:
            for table, changes_df in w_sampler.changes.items():
                existing = sampler.changes.get(table)
                if existing is not None and existing.height > 0:
                    sampler._changes[table] = pl.concat([existing, changes_df], how="diagonal")
                else:
                    sampler._changes[table] = changes_df

        for w_conn, _ in workers:
            # noinspection PyBroadException
            try:
                w_conn.close()
            except Exception:
                pass

    total_expected = sum(sampler.t_changes(table).height for table in tables)
    logger.debug(f"--- DML complete. Total expected: {total_expected} rows ---")
    for table in tables:
        expected = sampler.t_changes(table)
        logger.debug(f"{table}: {expected.height} rows")

    staging = Path(fork.context.staging_folder)
    _poll_until_captured(staging, trigger, total_expected)
    _stop_trigger(trigger, fork)

    _validate_continuous_output(staging, tables, sampler)

    fn_run = tdfnrun(
        function=process,
        function_defined_on=0,
        staging_folder=staging,
    )

    output_tables = fn_run.output.as_pl_df()
    n_tables = len(tables)
    assert len(output_tables) == n_tables, (
        f"Expected {n_tables} output tables, got {len(output_tables)}"
    )

    manifest_files = sorted(staging.glob(STAGE_MANIFEST_FILE_PATTERN))
    files_by_index: dict[int, list[Path]] = {}
    for mf in manifest_files:
        content = _read_manifest_file(str(mf))
        manifest = _deserialize_manifest(content)
        for idx, entry in manifest.entries.items():
            files_by_index.setdefault(idx, []).extend(
                Path(convert_uri_to_path(uri)) for uri in entry.files
            )

    for i in range(n_tables):
        output_df = drop_system_columns(output_tables[i].lazy()).collect()
        staged_df = drop_system_columns(
            pl.concat([
                pl.read_parquet(f) for f in files_by_index[i]
            ]).lazy()
        ).collect()
        columns = sorted(staged_df.columns)
        staged_sorted = staged_df.select(columns).sort(columns)
        output_sorted = output_df.select(columns).sort(columns)
        assert staged_sorted.equals(output_sorted), (
            f"Function output for table {tables[i]} (index {i}) "
            f"is not equivalent to staged data"
        )

    logger.debug("--- Function execution PASSED ---")

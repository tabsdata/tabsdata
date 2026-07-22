#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

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
    CDC_DATA_COL_OLD_PREFIX,
    CDC_META_FLAT,
    CDC_META_FMT,
    CDC_META_OP,
    CDC_META_SCH,
    CDC_META_SQ,
    CDC_META_TX,
    CdcOperation,
)
from tabsdata.connector.cdc.common.typing import CdcFormat
from tabsdata._utils.tableframe._common import drop_system_columns
from test_tabsdata.fntest import tdfnjob, tdfntrigger
from test_tabsdata.test_tableframe.common import pretty_pandas, pretty_polars
from test_tabsdata_cdc_common.common.sql import CdcSampler
from test_tabsdata_cdc_db2.common.sql import Db2CdcSampler

logger = logging.getLogger(__name__)

Q_POLL_INSERTS = 8
Q_POLL_UPDATES = 8
Q_POLL_DELETES = 8

TRIGGER_END_MINUTES = 30
TRIGGER_STOP_END_OFFSET_SECONDS = 60

TRIGGER_INIT_TIMEOUT_SECONDS = 60
TRIGGER_INIT_POLL_SECONDS = 0.5
TRIGGER_STOP_JOIN_TIMEOUT_SECONDS = 60

POLL_TIMEOUT_SECONDS = TRIGGER_END_MINUTES * 60
POLL_INTERVAL_SECONDS = 2


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


def count_staged_rows(staging: Path) -> int:
    working = staging.parent
    total = 0
    for parquet_file in working.glob("*.parquet"):
        # noinspection PyBroadException
        try:
            total += pl.read_parquet(parquet_file).height
        except Exception:
            pass
    return total


def count_staged_distinct_schemas(staging: Path) -> int:
    working = staging.parent
    schemas: set[str] = set()
    for parquet_file in working.glob("*.parquet"):
        # noinspection PyBroadException
        try:
            schemas.update(
                pl.read_parquet(parquet_file, columns=[CDC_META_SCH])[CDC_META_SCH]
                .unique()
                .to_list()
            )
        except Exception:
            pass
    return len(schemas)


@dataclass
class TriggerFork:
    context: Any = None
    thread: threading.Thread = None
    result: Any = None
    error: Exception = None


def fork_trigger(
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

    from test_tabsdata.fntest import find_test_name, test_name_local
    test_file, test_name = find_test_name()

    def worker():
        test_name_local.file = test_file
        test_name_local.name = test_name
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


def poll_until_distinct_schemas(
    staging: Path,
    expected_schemas: int,
    poll_timeout_seconds: int = POLL_TIMEOUT_SECONDS,
    poll_interval_seconds: int = POLL_INTERVAL_SECONDS,
) -> int:
    """Block until ``expected_schemas`` distinct schema hashes are persisted.

    Without this gate, two ALTERs that land within the same buffer-flush
    window can be coalesced into a single parquet, hiding a schema
    transition from the validator. Polling is bounded by the same
    ``POLL_TIMEOUT_SECONDS`` budget used for row-count polling — if the
    expected count never appears, the downstream validator assertion
    still surfaces the actual count and hash list.
    """
    logger.debug(
        f"--- Waiting for {expected_schemas} distinct schemas "
        f"(timeout {poll_timeout_seconds}s) ---"
    )
    poll_deadline = time.monotonic() + poll_timeout_seconds
    actual = 0
    while time.monotonic() < poll_deadline:
        actual = count_staged_distinct_schemas(staging)
        logger.debug(f"Distinct schemas captured: {actual}/{expected_schemas}")
        if actual >= expected_schemas:
            logger.debug("Expected schema count reached.")
            break
        time.sleep(poll_interval_seconds)
    else:
        logger.debug(
            f"Schema poll timeout reached at {actual}/{expected_schemas}."
        )
    return actual


def poll_until_captured(
    staging: Path,
    trigger: CdcTrigger,
    expected_rows: int,
    poll_timeout_seconds: int = POLL_TIMEOUT_SECONDS,
    poll_interval_seconds: int = POLL_INTERVAL_SECONDS,
) -> int:
    logger.debug(f"--- Waiting for CDC capture (timeout {poll_timeout_seconds}s) ---")
    poll_deadline = time.monotonic() + poll_timeout_seconds
    total_actual = 0
    while time.monotonic() < poll_deadline:
        # noinspection PyBroadException
        try:
            staged = count_staged_rows(staging)
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


def stop_trigger(
    trigger: CdcTrigger,
    fork: TriggerFork,
) -> None:
    fork.context.end = datetime.now(timezone.utc) - timedelta(seconds=TRIGGER_STOP_END_OFFSET_SECONDS)
    logger.debug("--- Trigger stop signal sent ---")

    fork.thread.join(timeout=TRIGGER_STOP_JOIN_TIMEOUT_SECONDS)
    assert not fork.thread.is_alive(), f"Trigger thread did not stop within {TRIGGER_STOP_JOIN_TIMEOUT_SECONDS}s"

    staging = Path(fork.context.staging_folder)
    final_staged = count_staged_rows(staging)
    final_buffered = trigger._buffered_rows
    logger.debug(
        f"After stop: staged={final_staged}, buffered={final_buffered}, "
        f"total={final_staged + final_buffered}"
    )

    assert final_buffered == 0, f"Buffer not fully flushed: {final_buffered} rows remaining"

    if fork.error is not None:
        raise fork.error


def build_publisher(
    trigger: td.StageTrigger,
) -> td.TabsdataFunction | Callable[..., Any]:
    @td.publisher(  # type: ignore[misc]
        source=td.Stage(use_existing_data=True),
        tables="output",
        name=str(uuid.uuid4()),
        trigger_by=trigger,
    )
    def process(tfs: list[td.TableFrame]) -> td.TableFrame:
        return td.concat(tfs)

    return process


def validate_cdc_meta_format(
    df: pl.DataFrame,
    cdc_format: CdcFormat,
) -> None:
    fmt_values = df[CDC_META_FMT].to_list()
    assert all(value == cdc_format.values_format for value in fmt_values), (
        f"Expected all {CDC_META_FMT} to be {cdc_format.values_format!r}; "
        f"got {set(fmt_values)}"
    )

    flat_values = df[CDC_META_FLAT].to_list()
    assert all(value == cdc_format.flatten_values for value in flat_values), (
        f"Expected all {CDC_META_FLAT} to be {cdc_format.flatten_values!r}; "
        f"got {set(flat_values)}"
    )


def validate_cdc_output(
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
    insert_key = sampler.tx_sort_key(insert_tx)
    update_key = sampler.tx_sort_key(update_tx)
    delete_key = sampler.tx_sort_key(delete_tx)
    assert insert_key < update_key < delete_key, (
        f"Transaction IDs should be ordered: {insert_tx} < {update_tx} < {delete_tx}"
    )


def run_dml_operations(
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


def run_poll_changes_test(
    sampler: CdcSampler,
    connection: Any,
    table: TableObject,
    trigger: CdcTrigger,
) -> None:
    expected = run_dml_operations(sampler, connection, table)
    process = build_publisher(trigger)

    def job():
        return tdfntrigger(trigger=trigger, function=process)

    fork = fork_trigger(trigger, job)
    staging = Path(fork.context.staging_folder)

    poll_until_captured(staging, trigger, expected.height)
    stop_trigger(trigger, fork)

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
    validate_cdc_meta_format(df, CdcFormat())
    validate_cdc_output(df, expected, sampler)


def run_end_to_end_test(
    sampler: CdcSampler,
    connection: Any,
    table: TableObject,
    trigger: CdcTrigger,
) -> None:
    expected = run_dml_operations(sampler, connection, table)
    process = build_publisher(trigger)

    def job():
        return tdfnjob(trigger=trigger, function=process, function_defined_on=0)

    fork = fork_trigger(trigger, job)
    staging = Path(fork.context.staging_folder)

    poll_until_captured(staging, trigger, expected.height)
    stop_trigger(trigger, fork)

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
    validate_cdc_meta_format(df, CdcFormat())
    validate_cdc_output(df, expected, sampler)

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
        pk_column = sampler.fold("pk")

        pretty_polars()
        pretty_pandas()

        logger.debug(f"--- Iteration {i + 1}/{iterations} ---")
        logger.debug(f"--- Expected Changes (iteration {i + 1}) ---")
        logger.debug(f"\n{expected}")

        trigger = trigger_factory(i, checkpoint_df)
        process = build_publisher(trigger)

        def job():
            return tdfntrigger(trigger=trigger, function=process)

        fork = fork_trigger(trigger, job)
        staging = Path(fork.context.staging_folder)

        poll_until_captured(staging, trigger, inserts_per_iteration)
        stop_trigger(trigger, fork)

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

        validate_cdc_meta_format(df, CdcFormat())

        assert df.height == expected.height, (
            f"Iteration {i + 1}: expected {expected.height} rows, got {df.height}"
        )

        ops = df[CDC_META_OP].to_list()
        assert all(op == CdcOperation.INSERT for op in ops), (
            f"Iteration {i + 1}: expected all INSERT's, "
            f"got {[op for op in ops if op != CdcOperation.INSERT]}"
        )

        actual_pks = sorted(df[pk_column].to_list())
        expected_pks = sorted(
            expected.filter(
                pl.col("op") == CdcOperation.INSERT
            )[f"new.{pk_column}"].to_list()
        )
        assert actual_pks == expected_pks, (
            f"Iteration {i + 1}: PK mismatch: {actual_pks} != {expected_pks}"
        )

        checkpoint_df = fn_trigger.context.checkpoint.read()

        logger.debug(f"--- Checkpoint (iteration {i + 1}) ---")
        logger.debug(f"\n{checkpoint_df}")
        logger.debug(f"--- Iteration {i + 1}/{iterations} PASSED ---")

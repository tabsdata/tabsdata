#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable

import polars as pl

import tabsdata as td
import pytest
from tabsdata.connector.cdc.common import CdcTrigger
from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.common.constants import (
    CDC_META_OP,
    CdcOperation,
)
from tabsdata.connector.cdc.common.typing import CdcFormat
from tabsdata._io.inputs.stage_input import (
    STAGE_MANIFEST_FILE_PATTERN,
    _deserialize_manifest,
    _read_manifest_file, Stage,
)
from tabsdata._tabsserver.function.global_utils import convert_uri_to_path
from tabsdata._utils.tableframe._common import drop_system_columns
from test_tabsdata.fntest import tdfntrigger, tdfnrun
from test_tabsdata.test_tableframe.common import pretty_pandas, pretty_polars
from test_tabsdata_cdc_common.common.shared import (
    fuzzy_eq,
    fork_trigger,
    poll_until_captured,
    stop_trigger,
    validate_cdc_meta_format,
)
from test_tabsdata_cdc_common.common.sql import CdcSampler

logger = logging.getLogger(__name__)

Q_TRANSACTIONS = 64
Q_CONTINUOUS_INSERTS = 16
Q_CONTINUOUS_UPDATES = 8
Q_CONTINUOUS_DELETES = 4
MINIMUM_TRANSACTION_TIME = 1.0

continuous_dml_params = pytest.mark.parametrize(
    ("q_tables", "q_threads"),
    [
        pytest.param(
            q_tables,
            q_threads,
            id=f"tables={q_tables}-threads={q_threads}",)
        for q_tables in [1, 2, 4]
        for q_threads in [1, 2, 4]
    ],
)


def validate_continuous_output(
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

    pk_column = sampler.fold("pk")

    for i, table in enumerate(tables):
        parquet_files = files_by_index.get(i, [])
        assert len(parquet_files) > 0, (
            f"No parquet files for table {table} (index {i})"
        )

        df = pl.concat([pl.read_parquet(parquet_file) for parquet_file in parquet_files])
        expected = sampler.t_changes(table)

        logger.debug(f"--- Table {table} (index {i}) ---")
        logger.debug(f"Actual rows: {df.height}")
        logger.debug(f"Expected rows: {expected.height}")

        validate_cdc_meta_format(df, CdcFormat())

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
                df.filter(pl.col(CDC_META_OP) == op_type)[pk_column].to_list()
            )
            expected_pks = sorted(
                expected.filter(pl.col("op") == op_type)[f"new.{pk_column}"].to_list()
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


def run_dml_transactions(
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

    fork = fork_trigger(trigger, job)

    pretty_polars()
    pretty_pandas()

    if dml_worker_threads <= 1:
        run_dml_transactions(sampler, connection, tables)
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
                run_dml_transactions(worker_sampler, worker_conn, tables)
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
    poll_until_captured(staging, trigger, total_expected)
    stop_trigger(trigger, fork)

    validate_continuous_output(staging, tables, sampler)

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

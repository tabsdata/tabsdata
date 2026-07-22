#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import polars as pl

from tabsdata.connector.cdc.common import CdcTrigger
from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.common.constants import (
    CDC_DATA_COL_OLD_PREFIX,
    CDC_META_OP,
    CDC_META_SCH,
    CdcOperation,
)
from test_tabsdata.fntest import tdfntrigger
from test_tabsdata_cdc_common.common.shared import (
    fuzzy_eq,
    build_publisher,
    fork_trigger,
    poll_until_captured,
    poll_until_distinct_schemas,
    stop_trigger,
)
from test_tabsdata_cdc_common.common.evolution import N_SCHEMA_ITERATIONS
from test_tabsdata_cdc_common.common.sql import CdcSampler

logger = logging.getLogger(__name__)

Q_SCHEMA_INSERTS_BEFORE = 256
Q_SCHEMA_INSERTS_AFTER = 256
Q_SCHEMA_UPDATES_AFTER = 64
Q_SCHEMA_DELETES_AFTER = 16

# Each ALTER iteration adds one schema on top of the pre-change baseline,
# so the persisted parquet output must show ``iteration + PRE_CHANGE_SCHEMAS``
# distinct schema hashes before the next ALTER is allowed to fire.
PRE_CHANGE_SCHEMAS = 1


@dataclass
class IterationExpected:
    changes: pl.DataFrame
    columns: list[str]
    float_columns: list[str]
    pk_column: str


def validate_schema_evolution_output(
    staging: Path,
    iterations: list[IterationExpected],
) -> None:
    workdir = staging.parent
    parquet_files = sorted(staging.glob("*.parquet")) if staging.exists() else []
    if not parquet_files:
        parquet_files = sorted(workdir.glob("*.parquet"))

    assert len(parquet_files) > 0, (
        f"No parquet files found in staging ({staging}) or work dir ({workdir})"
    )

    schema_order: list[str] = []
    files_by_schema: dict[str, list[Path]] = {}
    for pf in parquet_files:
        file_df = pl.read_parquet(pf)
        sch_values = file_df[CDC_META_SCH].unique().to_list()
        assert len(sch_values) == 1, (
            f"Parquet file {pf.name} has mixed schema hashes: {sch_values}"
        )
        sch = sch_values[0]
        if sch not in files_by_schema:
            schema_order.append(sch)
            files_by_schema[sch] = []
        files_by_schema[sch].append(pf)

    logger.debug(
        f"--- Schema evolution output: "
        f"{len(parquet_files)} files, "
        f"{len(schema_order)} distinct schemas ---"
    )

    assert len(schema_order) == len(iterations), (
        f"Expected {len(iterations)} distinct schemas, "
        f"got {len(schema_order)}: {schema_order}"
    )

    for group_idx, sch_hash in enumerate(schema_order):
        group_files = files_by_schema[sch_hash]
        expected = iterations[group_idx]
        pk_column = expected.pk_column

        group_dfs = [pl.read_parquet(f) for f in group_files]
        df = pl.concat(group_dfs)

        logger.debug(
            f"--- Schema group {group_idx} (hash={sch_hash}): "
            f"{df.height} rows from {len(group_files)} files ---"
        )

        expected_df = expected.changes
        assert df.height == expected_df.height, (
            f"Schema group {group_idx}: "
            f"expected {expected_df.height} rows, got {df.height}"
        )

        actual_ops = df[CDC_META_OP].to_list()
        expected_ops = expected_df["op"].to_list()
        for op_type in [CdcOperation.INSERT, CdcOperation.UPDATE, CdcOperation.DELETE]:
            actual_count = actual_ops.count(op_type)
            expected_count = expected_ops.count(op_type)
            assert actual_count == expected_count, (
                f"Schema group {group_idx}: "
                f"expected {expected_count} {op_type}, got {actual_count}"
            )

        for op_type in [CdcOperation.INSERT, CdcOperation.UPDATE, CdcOperation.DELETE]:
            actual_pks = sorted(
                df.filter(
                    pl.col(CDC_META_OP) == op_type
                )[pk_column].to_list()
            )
            expected_pks = sorted(
                expected_df.filter(
                    pl.col("op") == op_type
                )[f"new.{pk_column}"].to_list()
            )
            assert actual_pks == expected_pks, (
                f"Schema group {group_idx}: "
                f"PK mismatch for {op_type}: "
                f"actual={actual_pks[:10]}... expected={expected_pks[:10]}..."
            )

        for op_type in [CdcOperation.INSERT, CdcOperation.UPDATE]:
            actual_rows = df.filter(
                pl.col(CDC_META_OP) == op_type
            ).sort(pk_column)
            expected_rows = expected_df.filter(
                pl.col("op") == op_type
            ).sort(f"new.{pk_column}")
            for column in expected.columns:
                actual_values = actual_rows[column].to_list()
                expected_values = expected_rows[f"new.{column}"].to_list()
                assert fuzzy_eq(
                    actual_values, expected_values,
                    column, expected.float_columns,
                ), (
                    f"Schema group {group_idx}: "
                    f"new-value mismatch for {column} in {op_type}:\n"
                    f"  actual[:5]:   {actual_values[:5]}\n"
                    f"  expected[:5]: {expected_values[:5]}"
                )

        insert_rows = df.filter(pl.col(CDC_META_OP) == CdcOperation.INSERT)
        for column in expected.columns:
            old_col = f"{CDC_DATA_COL_OLD_PREFIX}.{column}"
            if old_col in insert_rows.columns:
                assert insert_rows[old_col].is_null().all(), (
                    f"Schema group {group_idx}: "
                    f"old {column} should be null for inserts"
                )

        logger.debug(f"--- Schema group {group_idx} PASSED ---")

    logger.debug("--- Schema evolution validation PASSED ---")


def run_schema_evolution_test(
    sampler: CdcSampler,
    connection: Any,
    table: TableObject,
    trigger: CdcTrigger,
    change_kinds: list,
    inserts_before: int = Q_SCHEMA_INSERTS_BEFORE,
    inserts_per_alter: int = Q_SCHEMA_INSERTS_AFTER,
    updates_per_alter: int = Q_SCHEMA_UPDATES_AFTER,
    deletes_per_alter: int = Q_SCHEMA_DELETES_AFTER,
) -> None:
    pk_column = sampler.fold("pk")
    iteration_expected: list[IterationExpected] = []

    sampler.run(table=table, inserts=inserts_before, updates=0, deletes=0)
    connection.commit()

    iteration_expected.append(IterationExpected(
        changes=sampler.t_changes(table).clone(),
        columns=list(sampler.columns),
        float_columns=list(sampler.float_columns),
        pk_column=pk_column,
    ))

    logger.debug(f"--- Pre-change: {inserts_before} rows inserted ---")
    logger.debug(f"Pre-change columns: {sampler.columns}")

    process = build_publisher(trigger)

    def job():
        return tdfntrigger(trigger=trigger, function=process)

    fork = fork_trigger(trigger, job)
    staging = Path(fork.context.staging_folder)

    cumulative = inserts_before
    poll_until_captured(staging, trigger, cumulative)

    for iteration in range(N_SCHEMA_ITERATIONS):
        logger.debug(f"--- ALTER iteration {iteration} ---")
        schema_changes = sampler.build_schema_changes(table, change_kinds, iteration)
        sampler.alter_table(connection, table, schema_changes)

        logger.debug(f"Post-change columns (iter {iteration}): {sampler.columns}")

        sampler._changes.pop(table, None)

        sampler.run_dynamic(
            table=table,
            inserts=inserts_per_alter,
            updates=updates_per_alter,
            deletes=deletes_per_alter,
        )
        connection.commit()

        iteration_expected.append(IterationExpected(
            changes=sampler.t_changes(table).clone(),
            columns=list(sampler.columns),
            float_columns=list(sampler.float_columns),
            pk_column=pk_column,
        ))

        iter_changes = sampler.t_changes(table).height
        cumulative += iter_changes
        logger.debug(
            f"--- Iteration {iteration}: {iter_changes} changes "
            f"(cumulative {cumulative}) ---"
        )
        poll_until_captured(staging, trigger, cumulative)
        poll_until_distinct_schemas(
            staging, iteration + 1 + PRE_CHANGE_SCHEMAS,
        )

    stop_trigger(trigger, fork)

    fn_trigger = fork.result
    fn_trigger.check()
    fn_trigger.integrity()

    validate_schema_evolution_output(
        staging=staging,
        iterations=iteration_expected,
    )


def run_schema_evolution_offset_test(
    sampler: CdcSampler,
    connection: Any,
    table: TableObject,
    trigger_factory: Callable[[int, pl.DataFrame | None], CdcTrigger],
    change_kinds: list,
    inserts_before: int = Q_SCHEMA_INSERTS_BEFORE,
    inserts_per_alter: int = Q_SCHEMA_INSERTS_AFTER,
    updates_per_alter: int = Q_SCHEMA_UPDATES_AFTER,
    deletes_per_alter: int = Q_SCHEMA_DELETES_AFTER,
) -> None:
    pk_column = sampler.fold("pk")

    sampler.run(table=table, inserts=inserts_before, updates=0, deletes=0)
    connection.commit()

    logger.debug(f"--- Iteration 0: {inserts_before} rows with initial schema ---")

    trigger = trigger_factory(0, None)
    process = build_publisher(trigger)

    def job_0():
        return tdfntrigger(trigger=trigger, function=process)

    fork = fork_trigger(trigger, job_0)
    staging = Path(fork.context.staging_folder)

    poll_until_captured(staging, trigger, inserts_before)
    stop_trigger(trigger, fork)

    fn_trigger = fork.result
    fn_trigger.check()
    fn_trigger.integrity()

    validate_schema_evolution_output(
        staging=staging,
        iterations=[IterationExpected(
            changes=sampler.t_changes(table).clone(),
            columns=list(sampler.columns),
            float_columns=list(sampler.float_columns),
            pk_column=pk_column,
        )],
    )

    checkpoint_df = fn_trigger.context.checkpoint.read()
    logger.debug(f"--- Checkpoint after iteration 0 ---")
    logger.debug(f"\n{checkpoint_df}")

    for alter_idx in range(N_SCHEMA_ITERATIONS):
        trigger_idx = alter_idx + 1

        logger.debug(f"--- ALTER iteration {alter_idx} ---")
        schema_changes = sampler.build_schema_changes(
            table, change_kinds, alter_idx,
        )
        sampler.alter_table(connection, table, schema_changes)
        logger.debug(f"Post-change columns (iter {alter_idx}): {sampler.columns}")

        sampler._changes.pop(table, None)

        sampler.run_dynamic(
            table=table,
            inserts=inserts_per_alter,
            updates=updates_per_alter,
            deletes=deletes_per_alter,
        )
        connection.commit()

        iter_expected_height = sampler.t_changes(table).height

        logger.debug(
            f"--- Iteration {trigger_idx}: {iter_expected_height} changes "
            f"with schema v{trigger_idx} ---"
        )

        iter_trigger = trigger_factory(trigger_idx, checkpoint_df)
        iter_process = build_publisher(iter_trigger)

        def make_job(t, p):
            return lambda: tdfntrigger(trigger=t, function=p)

        iter_fork = fork_trigger(
            iter_trigger, make_job(iter_trigger, iter_process),
        )
        iter_staging = Path(iter_fork.context.staging_folder)

        poll_until_captured(iter_staging, iter_trigger, iter_expected_height)
        stop_trigger(iter_trigger, iter_fork)

        fn_iter = iter_fork.result
        fn_iter.check()
        fn_iter.integrity()

        validate_schema_evolution_output(
            staging=iter_staging,
            iterations=[IterationExpected(
                changes=sampler.t_changes(table).clone(),
                columns=list(sampler.columns),
                float_columns=list(sampler.float_columns),
                pk_column=pk_column,
            )],
        )

        checkpoint_df = fn_iter.context.checkpoint.read()
        logger.debug(f"--- Checkpoint after iteration {trigger_idx} ---")
        logger.debug(f"\n{checkpoint_df}")

        logger.debug(f"--- Iteration {trigger_idx} PASSED ---")

    logger.debug("--- Schema evolution offset test PASSED ---")

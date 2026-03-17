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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import polars as pl

import tabsdata as td
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
    sampler.run(table=table, inserts=8, updates=0, deletes=0)
    connection.commit()

    sampler.run(table=table, inserts=0, updates=8, deletes=0)
    connection.commit()

    sampler.run(table=table, inserts=0, updates=0, deletes=8)
    connection.commit()

    expected = sampler.t_changes(table)
    pretty_polars()
    pretty_pandas()

    print("\n--- Expected Changes ---")
    print(expected)

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
    print("\n--- Actual Output ---")
    print(df)

    print("\n--- Actual Schema ---")
    print(df.schema)

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

    print(f"\n--- Transactions ---")
    print(f"Insert Transactions: {insert_txs.to_list()}")
    print(f"Update Transactions: {update_txs.to_list()}")
    print(f"Delete Transactions: {delete_txs.to_list()}")

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
    trigger: td.StageTrigger,
) -> None:
    expected = _run_dml_operations(sampler, connection, table)
    process = _build_publisher(trigger)

    fn_trigger = tdfntrigger(
        trigger=trigger,
        function=process,
    )

    fn_trigger.check()
    fn_trigger.integrity()

    staging = fn_trigger.staging
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
    trigger: td.StageTrigger,
) -> None:
    expected = _run_dml_operations(sampler, connection, table)
    process = _build_publisher(trigger)

    fn_trigger, fn_run = tdfnjob(
        trigger=trigger,
        function=process,
        function_defined_on=0,
    )

    fn_trigger.check()

    staging = fn_trigger.staging
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
    print("\n--- Actual Output ---")
    print(df)

    print("\n--- Actual Schema ---")
    print(df.schema)

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
    trigger_factory: Callable[[int, pl.DataFrame | None], td.StageTrigger],
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

        print(f"\n--- Iteration {i + 1}/{iterations} ---")
        print(f"\n--- Expected Changes (iteration {i + 1}) ---")
        print(expected)

        trigger = trigger_factory(i, checkpoint_df)
        process = _build_publisher(trigger)

        fn_trigger = tdfntrigger(
            trigger=trigger,
            function=process,
        )

        fn_trigger.check()
        fn_trigger.integrity()

        staging = fn_trigger.staging
        workdir = staging.parent

        parquet_files = sorted(staging.glob("*.parquet")) if staging.exists() else []
        if not parquet_files:
            parquet_files = sorted(workdir.glob("*.parquet"))

        assert len(parquet_files) > 0, (
            f"Iteration {i + 1}: no parquet files found in staging ({staging}) "
            f"or work dir ({workdir})"
        )

        df = pl.concat([pl.read_parquet(f) for f in parquet_files])

        print(f"\n--- Actual Output (iteration {i + 1}) ---")
        print(df)

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

        print(f"\n--- Checkpoint (iteration {i + 1}) ---")
        print(checkpoint_df)
        print(f"\n--- Iteration {i + 1}/{iterations} PASSED ---")


def run_end_to_end_format_test(
    sampler: CdcSampler,
    connection: Any,
    table: TableObject,
    trigger: td.StageTrigger,
    cdc_format: CdcFormatSpec,
) -> None:
    expected = _run_dml_operations(sampler, connection, table)
    process = _build_publisher(trigger)

    fn_trigger, fn_run = tdfnjob(
        trigger=trigger,
        function=process,
        function_defined_on=0,
    )

    fn_trigger.check()

    staging = fn_trigger.staging
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

        print(f"\n--- Table {table} (index {i}) ---")
        print(f"  Actual rows: {df.height}")
        print(f"  Expected rows: {expected.height}")

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
                expected.filter(
                    pl.col("op") == op_type
                )[f"new.{pk_col}"].to_list()
            )
            assert actual_pks == expected_pks, (
                f"Table {table}: PK mismatch for {op_type}: "
                f"{actual_pks} != {expected_pks}"
            )

        for op_type in [CdcOperation.INSERT, CdcOperation.UPDATE, CdcOperation.DELETE]:
            actual_rows = df.filter(
                pl.col(CDC_META_OP) == op_type
            ).sort(pk_col)
            expected_rows = expected.filter(
                pl.col("op") == op_type
            ).sort(f"new.{pk_col}")
            for column in sampler.columns:
                actual_values = actual_rows[column].to_list()
                expected_values = expected_rows[f"new.{column}"].to_list()
                assert fuzzy_eq(
                    actual_values, expected_values, column, sampler.float_columns
                ), (
                    f"Table {table}: value mismatch for {column} in {op_type}:\n"
                    f"  actual:   {actual_values}\n"
                    f"  expected: {expected_values}"
                )

    print("\n--- Continuous validation PASSED ---")


def run_continuous_dml_test(
    sampler: CdcSampler,
    connection: Any,
    tables: list[TableObject],
    trigger: td.StageTrigger,
    dml_duration_seconds: int = 60,
    safety_wait_seconds: int = 16,
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

    captured: dict[str, Any] = {}
    original_run = trigger.run

    def capturing_run(ctx):
        captured['context'] = ctx
        return original_run(ctx)

    trigger.run = capturing_run

    trigger_result: dict[str, Any] = {}

    def trigger_worker():
        try:
            trigger_result['fn_trigger'] = tdfntrigger(
                trigger=trigger, function=process,
            )
        except Exception as error:
            trigger_result['error'] = error

    thread = threading.Thread(target=trigger_worker, daemon=True)
    thread.start()

    deadline = time.monotonic() + 30
    while 'context' not in captured and time.monotonic() < deadline:
        time.sleep(0.5)
    assert 'context' in captured, "Trigger did not initialize within 30s"
    context = captured['context']

    pretty_polars()
    pretty_pandas()

    for batch in range(3):
        batch_start = time.monotonic()
        for table in tables:
            sampler.run(table=table, inserts=10, updates=0, deletes=0)
        connection.commit()
        remaining = 1.0 - (time.monotonic() - batch_start)
        if remaining > 0:
            time.sleep(remaining)

    mixed_batches = dml_duration_seconds - 3
    for batch in range(mixed_batches):
        batch_start = time.monotonic()
        for table in tables:
            sampler.run(table=table, inserts=6, updates=3, deletes=2)
        connection.commit()
        remaining = 1.0 - (time.monotonic() - batch_start)
        if remaining > 0:
            time.sleep(remaining)

    total_expected = sum(sampler.t_changes(t).height for t in tables)
    print(f"\n--- DML complete. Total expected: {total_expected} rows ---")
    for table in tables:
        expected = sampler.t_changes(table)
        print(f"  {table}: {expected.height} rows")

    staging = Path(context.staging_folder)
    poll_timeout_seconds = 120
    stable_threshold = 8
    print(f"\n--- Waiting for CDC capture (timeout {poll_timeout_seconds}s) ---")
    poll_deadline = time.monotonic() + poll_timeout_seconds
    prev_actual = -1
    stable_count = 0
    total_actual = 0
    while time.monotonic() < poll_deadline:
        # noinspection PyBroadException
        try:
            total_actual = _count_staged_rows(staging)
        except Exception:
            total_actual = 0
        print(f"  Staged rows: {total_actual}/{total_expected}")
        if total_actual >= total_expected:
            print("  All rows staged.")
            break
        if total_actual == prev_actual:
            stable_count += 1
        else:
            stable_count = 0
        prev_actual = total_actual
        if stable_count >= stable_threshold and total_actual > 0:
            print(
                f"  Staged rows stabilized at {total_actual}/{total_expected}"
                f" (remaining {total_expected - total_actual} in buffer)."
            )
            break
        time.sleep(2)
    else:
        print(
            f"  Poll timeout reached at {total_actual}/{total_expected} rows."
        )

    print(f"\n--- Safety wait ({safety_wait_seconds}s) ---")
    time.sleep(safety_wait_seconds)

    context.end = datetime.now(timezone.utc) - timedelta(seconds=60)
    print("\n--- Trigger stop signal sent ---")

    thread.join(timeout=30)
    assert not thread.is_alive(), "Trigger thread did not stop within 30s"

    if 'error' in trigger_result:
        raise trigger_result['error']

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

    print("\n--- Function execution PASSED ---")

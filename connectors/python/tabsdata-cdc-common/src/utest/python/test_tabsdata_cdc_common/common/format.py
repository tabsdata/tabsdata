#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import polars as pl

import pytest
from tabsdata.connector.cdc.common import CdcTrigger
from tabsdata.connector.cdc.common._model import TableObject
from tabsdata.connector.cdc.common.constants import (
    CDC_DATA_COL_NEW_PREFIX,
    CDC_DATA_COL_OLD_PREFIX,
    CDC_DATA_MAP_NEW,
    CDC_DATA_MAP_OLD,
    CDC_DATA_ROW_NEW,
    CDC_DATA_ROW_OLD,
    CDC_META_FLAT,
    CDC_META_FMT,
    CDC_META_OP,
    CDC_META_SQ,
    CDC_META_TX,
    CdcOperation,
)
from tabsdata.connector.cdc.common.typing import CdcFormat
from tabsdata._utils.tableframe._common import drop_system_columns
from test_tabsdata.fntest import tdfnjob
from test_tabsdata_cdc_common.common.shared import (
    build_publisher,
    fork_trigger,
    poll_until_captured,
    stop_trigger,
    run_dml_operations,
    validate_cdc_meta_format,
    validate_cdc_output,
)
from test_tabsdata_cdc_common.common.sql import CdcSampler
from test_tabsdata_cdc_db2.common.sql import Db2CdcSampler

logger = logging.getLogger(__name__)

CDC_FORMAT_PARAMS = [
    pytest.param(
        CdcFormat(values_format="columns", flatten_values=True),
        id="columns-flat",
    ),
    pytest.param(
        CdcFormat(values_format="columns", flatten_values=False),
        id="columns-prefixed",
    ),
    pytest.param(
        CdcFormat(values_format="struct", flatten_values=True),
        id="struct-flat",
    ),
    pytest.param(
        CdcFormat(values_format="struct", flatten_values=False),
        id="struct-nested",
    ),
    pytest.param(
        CdcFormat(values_format="map", flatten_values=True),
        id="map-flat",
    ),
    pytest.param(
        CdcFormat(values_format="map", flatten_values=False),
        id="map-nested",
    ),
]


def normalize_to_canonical(
    df: pl.DataFrame,
    sampler: CdcSampler,
    cdc_format: CdcFormat,
) -> pl.DataFrame:
    exprs: list[Any] = [
        pl.col(CDC_META_OP),
        pl.col(CDC_META_TX),
        pl.col(CDC_META_SQ),
        pl.col(CDC_META_FMT),
        pl.col(CDC_META_FLAT),
    ]

    for column in sampler.columns:
        if cdc_format.flatten_values:
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


def validate_cdc_map_output(
    df: pl.DataFrame,
    expected: pl.DataFrame,
    sampler: CdcSampler,
    cdc_format: CdcFormat,
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
    if cdc_format.flatten_values:
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

    if not cdc_format.flatten_values:
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

    pk_column = sampler.fold("pk")
    for op_type in [CdcOperation.INSERT, CdcOperation.UPDATE, CdcOperation.DELETE]:
        filtered = df.filter(pl.col(CDC_META_OP) == op_type)
        if cdc_format.flatten_values:
            actual_pks = sorted(filtered[pk_column].to_list())
        else:
            actual_pks = sorted(
                json.loads(v).get(pk_column)
                for v in filtered[CDC_DATA_MAP_NEW].to_list()
            )
        expected_pks = sorted(
            expected.filter(
                pl.col("op") == op_type
            )[f"new.{pk_column}"].to_list()
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
    insert_key = sampler.tx_sort_key(insert_tx)
    update_key = sampler.tx_sort_key(update_tx)
    delete_key = sampler.tx_sort_key(delete_tx)
    assert insert_key < update_key < delete_key, (
        f"Transaction IDs should be ordered: {insert_tx} < {update_tx} < {delete_tx}"
    )


def validate_cdc_format_output(
    df: pl.DataFrame,
    expected: pl.DataFrame,
    sampler: CdcSampler,
    cdc_format: CdcFormat,
) -> None:
    validate_cdc_meta_format(df, cdc_format)
    if cdc_format.values_format in ("columns", "struct"):
        logger.debug("--- Original Output ---")
        logger.debug(f"\n{df}")
        normalized = normalize_to_canonical(df, sampler, cdc_format)
        validate_cdc_output(normalized, expected, sampler)
    else:
        validate_cdc_map_output(df, expected, sampler, cdc_format)


def run_end_to_end_format_test(
    sampler: CdcSampler,
    connection: Any,
    table: TableObject,
    trigger: CdcTrigger,
    cdc_format: CdcFormat,
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
    validate_cdc_format_output(df, expected, sampler, cdc_format)

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

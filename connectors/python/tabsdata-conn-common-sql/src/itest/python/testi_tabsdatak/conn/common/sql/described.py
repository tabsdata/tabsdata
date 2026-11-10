#
# Copyright 2026 Tabsdata Inc.
#

"""A SQL source's columns typed from what its database describes, against a
live database: one table with a column per type, NULL in its first
:data:`NULL_ROWS` rows and holding a value in the last, read back through the
connector's own source plugin.

Polars used to infer a column from its first 100 rows, so a column NULL in all
of them came back typed Null and the first value after them failed the read.
Each column must now come back with the dtype its database type maps to -- in
every batch, the all-NULL ones included -- and with its value.

Each connector's itest lists its database's types as :class:`TypeCase` and
runs :func:`check` over :data:`CHUNK_SIZES`.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

import polars as pl
import pytest
from sqlalchemy import Engine, text

NULL_ROWS = 150
VALUE_ID = NULL_ROWS

CHUNK_SIZES = [
    # The first batch holds NULL rows only: the column must be typed from the
    # description, as nothing in the batch tells.
    pytest.param(100, id="batches"),
    # One batch whose first value comes after 150 NULLs, as in the report.
    pytest.param(100_000, id="one-batch"),
]


@dataclass(frozen=True)
class TypeCase:
    """One column: its database type, the SQL literal of the value written to
    it, and the dtype and value it must be read back as."""

    id: str
    ddl: str
    literal: str
    dtype: Any
    value: Any

    @property
    def column(self) -> str:
        return f"c_{self.id}"


Read = Callable[[str, int], Iterable[pl.DataFrame]]
"""Reads a query through the connector's source plugin with a chunk size,
returning every batch it produces."""


def table_for(request) -> str:
    return "td_src_described_" + re.sub(r"\W", "_", request.node.callspec.id).lower()


def provision(engine: Engine, table: str, cases: list[TypeCase]) -> None:
    columns = ", ".join(f"{case.column} {case.ddl}" for case in cases)
    literals = ", ".join(case.literal for case in cases)
    names = ", ".join(case.column for case in cases)
    drop(engine, table)
    with engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE {table} (id INTEGER, {columns})"))
        conn.execute(
            text(f"INSERT INTO {table} (id) VALUES (:id)"),
            [{"id": i} for i in range(NULL_ROWS)],
        )
        conn.execute(
            text(f"INSERT INTO {table} (id, {names}) VALUES ({VALUE_ID}, {literals})")
        )


def drop(engine: Engine, table: str) -> None:
    with engine.begin() as conn:
        try:
            conn.execute(text(f"DROP TABLE {table}"))
        except Exception:  # noqa: BLE001 -- not there yet
            conn.rollback()


def check(
    engine: Engine,
    read: Read,
    table: str,
    cases: list[TypeCase],
    chunk_size: int,
) -> None:
    provision(engine, table, cases)
    try:
        # Oracle and Db2 describe the unquoted names in upper case.
        batches = [
            batch.rename(str.lower)
            for batch in read(f"SELECT * FROM {table} ORDER BY id", chunk_size)
        ]
        _check_batches(batches, cases, chunk_size)
        _check_values(pl.concat(batches), cases)
    finally:
        drop(engine, table)


def _check_batches(
    batches: list[pl.DataFrame], cases: list[TypeCase], chunk_size: int
) -> None:
    expected_batches = math.ceil((NULL_ROWS + 1) / chunk_size)
    assert len(batches) == expected_batches, [batch.height for batch in batches]
    wrong = {
        (index, case.column): str(batch.schema[case.column])
        for index, batch in enumerate(batches)
        for case in cases
        if batch.schema[case.column] != case.dtype
    }
    assert not wrong, {
        key: f"{dtype}, not {next(c.dtype for c in cases if c.column == key[1])}"
        for key, dtype in wrong.items()
    }


def _check_values(frame: pl.DataFrame, cases: list[TypeCase]) -> None:
    assert frame.height == NULL_ROWS + 1
    nulls = frame.filter(pl.col("id") != VALUE_ID)
    assert all(nulls[case.column].null_count() == NULL_ROWS for case in cases)
    row = frame.filter(pl.col("id") == VALUE_ID).row(0, named=True)
    wrong = {
        case.column: (row[case.column], case.value)
        for case in cases
        if not _same(row[case.column], case.value)
    }
    assert not wrong, wrong


def _same(read: Any, expected: Any) -> bool:
    if isinstance(expected, float):
        return isinstance(read, float) and math.isclose(read, expected, rel_tol=1e-6)
    if isinstance(read, memoryview):
        read = read.tobytes()
    return read == expected

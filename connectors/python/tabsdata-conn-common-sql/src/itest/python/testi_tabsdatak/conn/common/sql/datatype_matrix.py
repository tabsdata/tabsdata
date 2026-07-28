#
# Copyright 2026 Tabsdata Inc.
#

"""Reusable data-type coverage matrix for the SQLAlchemy-write connectors
(db2 / mssqlserver / mysql / oracle / postgres).

Each :data:`DATATYPE_CASES` entry is one TableFrame/polars column type. The
shared :func:`write_named_datatype_table` builds a one-column parquet of that
type, drives the connector's ``DestPlugin.write_out`` in ``replace`` mode (so the
create-on-write path picks the target column type), reads the table back, and
returns the round-tripped frame.

Each connector's itest parametrizes over ``DATATYPE_CASES`` and calls it against
its live tdx-provisioned domain, naming the target table after the case. Types
that need special handling on a given engine surface here as a failing (or
xfail'd) case -- e.g. Arrow decimal landing in a CLOB on DB2.

:func:`write_datatype_table` is the pre-migration variant that invents a random
table name; it goes away once every connector passes a name of its own.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import polars as pl
from tabsdatax.itest.fixture.functions.files import PluginDestCtx
from tabsdatax.itest.fixture.functions.sql import SqlStore, scratch_table


@dataclass(frozen=True)
class DataTypeCase:
    """One TableFrame column type plus a couple of sample values (a NULL
    always included, so nullability is exercised too)."""

    id: str
    dtype: Any
    values: list[Any]


# The TableFrame scalar dtypes (tabsdata.tableframe.dtypes._ALL_DTYPES) plus
# tz-aware datetime and binary. Int128 / Float16 are intentionally omitted --
# they are upstream (Arrow/parquet) edge cases rather than DB write concerns.
DATATYPE_CASES: list[DataTypeCase] = [
    DataTypeCase("boolean", pl.Boolean, [True, False, None]),
    DataTypeCase("int8", pl.Int8, [1, -2, None]),
    DataTypeCase("int16", pl.Int16, [1, -2, None]),
    DataTypeCase("int32", pl.Int32, [1, -2, None]),
    DataTypeCase("int64", pl.Int64, [1, -2, None]),
    DataTypeCase("uint8", pl.UInt8, [1, 2, None]),
    DataTypeCase("uint16", pl.UInt16, [1, 2, None]),
    DataTypeCase("uint32", pl.UInt32, [1, 2, None]),
    DataTypeCase("uint64", pl.UInt64, [1, 2, None]),
    DataTypeCase("float32", pl.Float32, [1.5, -2.5, None]),
    DataTypeCase("float64", pl.Float64, [1.5, -2.5, None]),
    DataTypeCase(
        "decimal_10_2", pl.Decimal(10, 2), [Decimal("1.50"), Decimal("-3.14"), None]
    ),
    DataTypeCase("decimal_38_10", pl.Decimal(38, 10), [Decimal("1.0000000001"), None]),
    DataTypeCase("string", pl.String, ["alice", "", None]),
    DataTypeCase("date", pl.Date, [date(2026, 7, 14), None]),
    DataTypeCase(
        "datetime", pl.Datetime("us"), [datetime(2026, 7, 14, 9, 30, 15), None]
    ),
    DataTypeCase(
        "datetime_tz",
        pl.Datetime("us", "UTC"),
        [datetime(2026, 7, 14, 9, 30, 15, tzinfo=timezone.utc), None],
    ),
    DataTypeCase("time", pl.Time, [time(9, 30, 15), None]),
    DataTypeCase("duration", pl.Duration("us"), [timedelta(hours=1, minutes=2), None]),
    DataTypeCase("binary", pl.Binary, [b"\x00\x01\x02", b"", None]),
]


def build_parquet(path: Path, case: DataTypeCase) -> Path:
    """Write a single-column parquet of ``case.dtype`` holding ``case.values``."""
    df = pl.DataFrame({"c": pl.Series("c", case.values, dtype=case.dtype)})
    df.write_parquet(path, use_pyarrow=True)
    return path


def write_named_datatype_table(
    case: DataTypeCase,
    *,
    table: str,
    dest_plugin: Any,
    make_dest: Callable[[list[str]], Any],
    dest_conn: Any,
    sql: SqlStore,
    work_dir: Path,
) -> pl.DataFrame:
    """Create + load `table` with ``case.dtype`` through the connector's write
    plugin (``replace`` mode), returning the round-tripped frame. Drops the table
    on the way out. Raises whatever the connector raises -- a type the engine
    can't create/bind fails right here.

    The caller names the table, one per case, so a run leaves nothing
    unpredictable behind in the domain.
    """
    parquet = build_parquet(work_dir / f"{case.id}.parquet", case)
    try:
        dest_plugin.write_out(
            PluginDestCtx(work_dir),
            dest_conn,
            make_dest([table]),
            [parquet],
        )
        return sql.read_table(table)
    finally:
        sql.drop_table(table)


def write_datatype_table(
    case: DataTypeCase,
    *,
    dest_plugin: Any,
    make_dest: Callable[[list[str]], Any],
    dest_conn: Any,
    sql: SqlStore,
    work_dir: Path,
) -> pl.DataFrame:
    """:func:`write_named_datatype_table` against a randomly-named scratch table.

    The variant the connectors not yet migrated to fixed table names still use;
    it goes away with the last of them.
    """
    return write_named_datatype_table(
        case,
        table=scratch_table(),
        dest_plugin=dest_plugin,
        make_dest=make_dest,
        dest_conn=dest_conn,
        sql=sql,
        work_dir=work_dir,
    )

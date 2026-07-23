#
# Copyright 2026 Tabsdata Inc.
#

"""Row schema (``Column`` / ``Table``) and SQL rendering.

A model's ``datagenerator.py`` builds ``Table`` objects (typed columns + rows),
from which this module renders portable ``INSERT`` statements and typed SQL
literals, or (for file/object-store domains) writes one data file per table in
csv / ndjson / parquet (:func:`write_frame`).

Column ``sql_type`` strings recognised: int/bigint/smallint/tinyint, boolean,
varchar(N)/char(N)/text, date/datetime/timestamp/time, decimal(P,S),
float/double. ``auto=True`` omits the column from ``INSERT`` (left to the
database IDENTITY).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Iterable

# The data-file formats a file/object-store domain stages (one file per entity
# per format), each named ``<entity>_<file_stamp()>.<fmt>``.
FILE_FORMATS = ("csv", "ndjson", "parquet")


@dataclass
class Column:
    name: str
    sql_type: str
    nullable: bool = True
    auto: bool = False  # IDENTITY / SERIAL — skipped in INSERTs


@dataclass
class Table:
    name: str
    columns: list[Column]
    rows: list[dict]
    sql_post: list[str] = field(default_factory=list)


_DECIMAL_RE = re.compile(r"^decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.I)
_VARCHAR_RE = re.compile(r"^(?:varchar|char|nvarchar|nchar)\s*\(\s*\d+\s*\)$", re.I)


def _is_decimal(t: str):
    m = _DECIMAL_RE.match(t.strip())
    return (int(m.group(1)), int(m.group(2))) if m else None


def _is_string(t: str) -> bool:
    t = t.strip().lower()
    return t == "text" or _VARCHAR_RE.match(t) is not None


def _is_int(t: str) -> str | None:
    return {
        "tinyint": "int8",
        "smallint": "int16",
        "int": "int32",
        "integer": "int32",
        "bigint": "int64",
    }.get(t.strip().lower())


def _norm(t: str) -> str:
    return re.sub(r"\s+", "", t).lower()


_BARE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def sql_quote_ident(name: str, quote: str) -> str:
    """Wrap `name` in `quote` if it isn't a bare SQL identifier, doubling any
    embedded quote char so the name round-trips."""
    if _BARE_IDENT_RE.match(name):
        return name
    return f"{quote}{name.replace(quote, quote * 2)}{quote}"


def _sql_escape(s: str) -> str:
    return s.replace("'", "''")


def _to_date(v):
    if v is None or (isinstance(v, date) and not isinstance(v, datetime)):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        return v
    raise TypeError(f"date column got {type(v).__name__}")


def _to_datetime(v):
    if v is None:
        return v
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime.combine(v, time(0, 0))
    if isinstance(v, str):
        return v
    raise TypeError(f"datetime column got {type(v).__name__}")


def _to_time(v):
    if v is None or isinstance(v, (time, str)):
        return v
    raise TypeError(f"time column got {type(v).__name__}")


def sql_literal(v: Any, sql_type: str) -> str:
    if v is None:
        return "NULL"
    # Oracle treats '' as NULL; emit NULL for empty strings everywhere for a
    # portable INSERT.
    if isinstance(v, str) and v == "":
        return "NULL"
    t = _norm(sql_type)
    if _is_int(t) is not None:
        return str(int(v))
    if t in ("boolean", "bool"):
        return "1" if v else "0"
    if t in ("float", "double", "real"):
        return str(float(v))
    if _is_decimal(t):
        return str(v)
    if t == "date":
        d = _to_date(v)
        return f"'{d.strftime('%Y-%m-%d') if isinstance(d, date) else d}'"
    if t in ("datetime", "timestamp"):
        d = _to_datetime(v)
        return f"'{d.strftime('%Y-%m-%d %H:%M:%S') if isinstance(d, datetime) else d}'"
    if t == "time":
        d = _to_time(v)
        return f"'{d.strftime('%H:%M:%S') if isinstance(d, time) else d}'"
    if _is_string(t):
        return f"'{_sql_escape(str(v))}'"
    return f"'{_sql_escape(str(v))}'"


def iter_insert_statements(
    table_name: str,
    columns: list[Column],
    rows: list[dict],
    *,
    batch: int = 1,
    quote: str = '"',
) -> Iterable[str]:
    """Yield ``INSERT INTO ... VALUES ...`` statements (no trailing ``;``).
    ``auto`` columns are skipped. With ``batch > 1`` each statement carries up to
    ``batch`` row tuples in one multi-row ``VALUES`` clause."""
    cols = [c for c in columns if not c.auto]
    col_names = ",".join(sql_quote_ident(c.name, quote) for c in cols)
    if batch < 1:
        batch = 1
    for start in range(0, len(rows), batch):
        tuples = ",".join(
            "(" + ",".join(sql_literal(r.get(c.name), c.sql_type) for c in cols) + ")"
            for r in rows[start : start + batch]
        )
        yield f"INSERT INTO {table_name} ({col_names}) VALUES {tuples}"


# ---------------------------------------------------------------------------
# File output — one file per table, per format (for file/object-store domains)
# ---------------------------------------------------------------------------


def _frame(tbl: "Table"):
    """A polars DataFrame of `tbl` (ALL columns, incl. ``auto``), preserving
    column order even when there are no rows.

    Unlike SQL ``INSERT`` (where ``auto`` columns are skipped and the DB
    regenerates them), a file seed has no DB to regenerate anything — dropping
    ``auto`` columns would strip surrogate primary keys that other tables
    reference as foreign keys, leaving the file dataset internally unjoinable.
    datagen already populates these columns with real values, so keep them."""
    import polars as pl

    cols = list(tbl.columns)
    if tbl.rows:
        return pl.DataFrame({
            c.name: [r.get(c.name) for r in tbl.rows] for c in cols
        }).select([c.name for c in cols])
    return pl.DataFrame({c.name: [] for c in cols})


def file_stamp() -> str:
    """A filename timestamp ``YYYYMMDDThhmmss_mmm`` at the current local time —
    all files staged in one command invocation share the same stamp."""
    now = datetime.now()
    return now.strftime("%Y%m%dT%H%M%S_") + f"{now.microsecond // 1000:03d}"


def write_frame(tbl: "Table", path: str | Path, fmt: str) -> None:
    """Write `tbl`'s columns and rows (incl. ``auto`` surrogate keys) to `path`
    as `fmt` (one of :data:`FILE_FORMATS`), creating parent directories."""
    if fmt not in FILE_FORMATS:
        raise ValueError(f"unknown format {fmt!r}; expected one of {FILE_FORMATS}")
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    frame = _frame(tbl)
    if fmt == "csv":
        frame.write_csv(p)
    elif fmt == "ndjson":
        frame.write_ndjson(p)
    else:
        frame.write_parquet(p)

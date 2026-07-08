#
# Copyright 2026 Tabsdata Inc.
#

"""Shared output emitter for tabsdatax.itest.tdksystems sources.

Each source's datagen.py builds a list of `Table` objects describing
the rows it has generated, then calls :func:`write_tables` to emit
them in the requested format (sql, csv, json or parquet).

Output layout for any format::

    <out_dir>/
        _load_order.txt        # newline-separated table names in dependency order
        <table_name>.<ext>     # one file per table

Type strings recognised in :class:`Column`:

    int, bigint, smallint, tinyint, boolean,
    varchar(N), char(N), text,
    date, datetime, timestamp, time,
    decimal(P,S), float, double

Pass ``auto=True`` to omit the column from SQL INSERTs (it is left for
the database's IDENTITY mechanism). Other formats include the column
because they have no notion of auto-increment.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

# pyarrow / pandas imported lazily inside the format-specific writers so
# `--format sql` keeps working in stripped-down environments.


# ---------------------------------------------------------------------------
# Schema types
# ---------------------------------------------------------------------------


@dataclass
class Column:
    name: str
    sql_type: str
    nullable: bool = True
    auto: bool = False  # IDENTITY / SERIAL — skipped in SQL INSERTs


@dataclass
class Table:
    name: str
    columns: list[Column]
    rows: list[dict]
    # Optional extra SQL statements to emit *after* the INSERTs (only used
    # for --format sql). Useful for circular FK fix-ups.
    sql_post: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Type parsing
# ---------------------------------------------------------------------------

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
    """Lowercase / collapse whitespace so callers can be a bit loose."""
    return re.sub(r"\s+", "", t).lower()


# ---------------------------------------------------------------------------
# Public entry
# ---------------------------------------------------------------------------

VALID_FORMATS = ("sql", "csv", "json", "parquet")
_FORMAT_EXT = {"sql": "sql", "csv": "csv", "json": "json", "parquet": "parquet"}


def write_tables(
    tables: list[Table],
    out_dir: str | Path,
    fmt: str,
    source_dir: str | Path | None = None,
    chunks: int | None = None,
    sql_quote_char: str = '"',
) -> None:
    """Emit one file per table (or per chunk if `chunks` is given).

    `chunks` — if positive, split each table's rows into chunks of `chunks`
    rows. Each chunk becomes a separate file named ``<entity>-<NNNNN>.<ext>``
    with a 5-digit zero-padded counter starting at 00001. ``run-order.txt``
    lists every chunk file in load order. The SQL post-statements (if any)
    are emitted only at the tail of the *last* chunk so circular-FK fix-ups
    fire after every row is inserted.

    `sql_quote_char` — for --format sql, quote any column name that isn't
    a bare SQL identifier (e.g. contains `@` or `.`). Defaults to the ANSI
    double quote `"`, which works on Postgres / MSSQL / Oracle / Db2.
    Pass `` ` `` for MySQL, since MySQL only accepts double quotes when
    the session has ANSI_QUOTES enabled. Plain identifiers are emitted
    unquoted regardless.
    """
    if fmt not in VALID_FORMATS:
        raise ValueError(f"unknown format {fmt!r}; expected one of {VALID_FORMATS}")
    if chunks is not None and chunks < 1:
        raise ValueError(f"--chunks must be >= 1 (got {chunks})")

    out = Path(out_dir)
    if out.exists():
        raise FileExistsError(f"--out path already exists: {out}")
    out.mkdir(parents=True)

    pairs = _chunk_tables(tables, chunks)
    if fmt == "sql":

        def writer(basename, view, out_path):
            _write_sql_pair(basename, view, out_path, quote=sql_quote_char)

    else:
        writer = {
            "csv": _write_csv_pair,
            "json": _write_json_pair,
            "parquet": _write_parquet_pair,
        }[fmt]
    for basename, view in pairs:
        writer(basename, view, out)

    # `run-order.txt` lists data files in the order they should be loaded so
    # that foreign keys / NOT NULL parents are satisfied.
    ext = _FORMAT_EXT[fmt]
    (out / "run-order.txt").write_text(
        "\n".join(f"{name}.{ext}" for name, _ in pairs) + "\n"
    )

    # For --format sql, ship every <dbtype>.ddl alongside the data so the
    # env script's `load --in DIR` can create the schema before inserting.
    if fmt == "sql" and source_dir is not None:
        src = Path(source_dir)
        for ddl in sorted(src.glob("*.ddl")):
            shutil.copy2(ddl, out / ddl.name)

    # Optional column-type sidecar — written only when the generating caller
    # asks for it (kafka / saas-sql loaders set TD_GEN_SCHEMA_SIDECAR). It
    # carries the per-table Column metadata that the serialized data files
    # (json/csv) drop, so a loader can rebuild schemas / typed INSERTs. Gated
    # on the env var so ordinary sql/cloud loads never see the extra file.
    if os.environ.get("TD_GEN_SCHEMA_SIDECAR"):
        _write_schema_sidecar(tables, out)

    print(f"Wrote {len(pairs)} {fmt} file(s) to {out}", file=sys.stderr)


_SCHEMA_SIDECAR = "_schema.json"


def _write_schema_sidecar(tables: list[Table], out: Path) -> None:
    payload = {
        "tables": [
            {
                "name": t.name,
                "columns": [
                    {
                        "name": c.name,
                        "sql_type": c.sql_type,
                        "nullable": c.nullable,
                        "auto": c.auto,
                    }
                    for c in t.columns
                ],
            }
            for t in tables
        ]
    }
    (out / _SCHEMA_SIDECAR).write_text(json.dumps(payload, indent=2) + "\n")


def read_structured(data_dir: str | Path) -> list[tuple[str, list[Column], list[dict]]]:
    """Read a dir produced by ``write_tables(fmt="json")`` WITH the schema
    sidecar (i.e. generated under TD_GEN_SCHEMA_SIDECAR). Returns
    ``[(table_name, [Column, ...], [row, ...]), ...]`` in run-order.

    Used by the kafka and saas-sql loaders, which need typed columns + rows
    rather than pre-serialized files. Assumes generation without chunking
    (one ``<table>.json`` per table)."""
    data_dir = Path(data_dir)
    schema_path = data_dir / _SCHEMA_SIDECAR
    if not schema_path.is_file():
        raise FileNotFoundError(f"missing schema sidecar: {schema_path}")
    schema = json.loads(schema_path.read_text())
    cols_by_table = {
        t["name"]: [Column(**c) for c in t["columns"]] for t in schema["tables"]
    }
    result: list[tuple[str, list[Column], list[dict]]] = []
    for line in (data_dir / "run-order.txt").read_text().splitlines():
        line = line.strip()
        if not line.endswith(".json"):
            continue
        table = line[: -len(".json")]
        cols = cols_by_table.get(table)
        if cols is None:  # tolerate a chunk suffix just in case
            table = re.sub(r"-\d{5}$", "", table)
            cols = cols_by_table.get(table, [])
        rows = json.loads((data_dir / line).read_text())
        result.append((table, cols, rows))
    return result


def _chunk_tables(
    tables: list[Table],
    chunks: int | None,
) -> list[tuple[str, Table]]:
    """Yield (file_basename, table_view) pairs. table_view.name remains the
    real entity name so SQL INSERT statements still reference the correct
    table; only the file basename gets the chunk suffix."""
    pairs: list[tuple[str, Table]] = []
    for tbl in tables:
        if chunks is None:
            pairs.append((tbl.name, tbl))
            continue
        if not tbl.rows:
            # Still emit one (empty) chunk so the entity always has at least
            # one file — keeps run-order.txt consistent.
            pairs.append((
                f"{tbl.name}-00001",
                Table(
                    name=tbl.name, columns=tbl.columns, rows=[], sql_post=tbl.sql_post
                ),
            ))
            continue
        idx = 0
        for start in range(0, len(tbl.rows), chunks):
            idx += 1
            slice_rows = tbl.rows[start : start + chunks]
            is_last = start + chunks >= len(tbl.rows)
            view = Table(
                name=tbl.name,
                columns=tbl.columns,
                rows=slice_rows,
                sql_post=tbl.sql_post if is_last else [],
            )
            pairs.append((f"{tbl.name}-{idx:05d}", view))
    return pairs


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_BARE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _sql_quote_ident(name: str, quote: str) -> str:
    """Wrap `name` in `quote` if it isn't a bare SQL identifier.
    Doubles any embedded quote char so the name round-trips."""
    if _BARE_IDENT_RE.match(name):
        return name
    return f"{quote}{name.replace(quote, quote * 2)}{quote}"


def _sql_escape(s: str) -> str:
    return s.replace("'", "''")


def _to_date(v):
    if v is None or isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        return v  # already formatted
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
    if v is None:
        return v
    if isinstance(v, time):
        return v
    if isinstance(v, str):
        return v
    raise TypeError(f"time column got {type(v).__name__}")


def _sql_literal(v: Any, sql_type: str) -> str:
    if v is None:
        return "NULL"
    # Oracle treats '' as NULL; emit NULL for empty strings everywhere so
    # the same INSERT works on Oracle, MySQL, Postgres, MSSQL, and Db2.
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
    # Fallback: treat as string
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

    Columns flagged ``auto`` are skipped (left to the DB IDENTITY). With
    ``batch > 1`` each statement carries up to ``batch`` row tuples in a
    single multi-row ``VALUES`` clause — used by the Databricks / Snowflake
    loaders so a bulk insert over a slow per-statement transport issues far
    fewer round-trips. ``batch=1`` reproduces the one-INSERT-per-row form the
    ``--format sql`` files have always used."""
    cols = [c for c in columns if not c.auto]
    col_names = ",".join(_sql_quote_ident(c.name, quote) for c in cols)
    if batch < 1:
        batch = 1
    for start in range(0, len(rows), batch):
        tuples = ",".join(
            "(" + ",".join(_sql_literal(r.get(c.name), c.sql_type) for c in cols) + ")"
            for r in rows[start : start + batch]
        )
        yield f"INSERT INTO {table_name} ({col_names}) VALUES {tuples}"


def _write_sql_pair(basename: str, tbl: Table, out: Path, quote: str = '"') -> None:
    path = out / f"{basename}.sql"
    with path.open("w") as f:
        f.write(f"-- {tbl.name}: {len(tbl.rows)} rows\n")
        for stmt in iter_insert_statements(
            tbl.name, tbl.columns, tbl.rows, batch=1, quote=quote
        ):
            f.write(stmt + ";\n")
        for stmt in tbl.sql_post:
            f.write(stmt.rstrip(";") + ";\n")


# ---------------------------------------------------------------------------
# CSV / JSON helpers — coerce a row to a JSON-ready dict
# ---------------------------------------------------------------------------


def _coerce_for_serial(v: Any, sql_type: str):
    """Convert a row value to a CSV/JSON-friendly representation.

    Dates/times/datetimes → ISO strings; Decimals → float."""
    if v is None:
        return None
    t = _norm(sql_type)
    if t == "date":
        d = _to_date(v)
        return d.strftime("%Y-%m-%d") if isinstance(d, date) else d
    if t in ("datetime", "timestamp"):
        d = _to_datetime(v)
        return d.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(d, datetime) else d
    if t == "time":
        d = _to_time(v)
        return d.strftime("%H:%M:%S") if isinstance(d, time) else d
    if _is_decimal(t):
        return float(v) if not isinstance(v, str) else v
    if isinstance(v, Decimal):
        return float(v)
    if _is_int(t) is not None:
        return int(v)
    if t in ("boolean", "bool"):
        return bool(v)
    return v


def _coerce_row(row: dict, columns: list[Column]) -> dict:
    return {c.name: _coerce_for_serial(row.get(c.name), c.sql_type) for c in columns}


_PANDAS_INT_DTYPE = {
    "tinyint": "Int8",
    "smallint": "Int16",
    "int": "Int32",
    "integer": "Int32",
    "bigint": "Int64",
}


def _pandas_dtype_for(sql_type: str) -> str | None:
    t = _norm(sql_type)
    if t in _PANDAS_INT_DTYPE:
        return _PANDAS_INT_DTYPE[t]
    if t in ("boolean", "bool"):
        return "boolean"
    return None  # let pandas infer (string / float)


def _write_csv_pair(basename: str, tbl: Table, out: Path) -> None:
    import pandas as pd

    rows = [_coerce_row(r, tbl.columns) for r in tbl.rows]
    df = pd.DataFrame(rows, columns=[c.name for c in tbl.columns])
    for c in tbl.columns:
        dt = _pandas_dtype_for(c.sql_type)
        if dt is not None:
            df[c.name] = df[c.name].astype(dt)
    df.to_csv(out / f"{basename}.csv", index=False)


def _write_json_pair(basename: str, tbl: Table, out: Path) -> None:
    rows = [_coerce_row(r, tbl.columns) for r in tbl.rows]
    with (out / f"{basename}.json").open("w") as f:
        json.dump(
            rows, f, ensure_ascii=False, indent=None, separators=(",", ":"), default=str
        )
        f.write("\n")


# ---------------------------------------------------------------------------
# Parquet — keep SQL-ish types
# ---------------------------------------------------------------------------


def _arrow_type_for(sql_type: str):
    import pyarrow as pa

    t = _norm(sql_type)
    int_map = {
        "tinyint": pa.int8(),
        "smallint": pa.int16(),
        "int": pa.int32(),
        "integer": pa.int32(),
        "bigint": pa.int64(),
    }
    if t in int_map:
        return int_map[t]
    if t in ("boolean", "bool"):
        return pa.bool_()
    if t in ("float", "real"):
        return pa.float32()
    if t == "double":
        return pa.float64()
    dec = _is_decimal(t)
    if dec is not None:
        p, s = dec
        # pyarrow decimal128 supports up to 38 digits
        if p > 38:
            p = 38
        return pa.decimal128(p, s)
    if t == "date":
        return pa.date32()
    if t in ("datetime", "timestamp"):
        return pa.timestamp("us")
    if t == "time":
        return pa.time32("s")
    if _is_string(t):
        return pa.string()
    return pa.string()


def _coerce_for_arrow(v, sql_type: str):
    if v is None:
        return None
    t = _norm(sql_type)
    if t == "date":
        d = _to_date(v)
        if isinstance(d, str):
            return date.fromisoformat(d)
        return d
    if t in ("datetime", "timestamp"):
        d = _to_datetime(v)
        if isinstance(d, str):
            # accept either ISO-T or space separator
            return datetime.fromisoformat(d.replace(" ", "T"))
        return d
    if t == "time":
        d = _to_time(v)
        if isinstance(d, str):
            return time.fromisoformat(d)
        return d
    dec = _is_decimal(t)
    if dec is not None:
        if isinstance(v, Decimal):
            return v
        return Decimal(str(v))
    if _is_int(t) is not None:
        return int(v)
    if t in ("boolean", "bool"):
        return bool(v)
    if t in ("float", "double", "real"):
        return float(v)
    return v if isinstance(v, str) else str(v)


def _write_parquet_pair(basename: str, tbl: Table, out: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    fields = [
        pa.field(c.name, _arrow_type_for(c.sql_type), nullable=c.nullable)
        for c in tbl.columns
    ]
    schema = pa.schema(fields)
    column_data: dict[str, list] = {c.name: [] for c in tbl.columns}
    for r in tbl.rows:
        for c in tbl.columns:
            column_data[c.name].append(_coerce_for_arrow(r.get(c.name), c.sql_type))
    arrays = [
        pa.array(column_data[c.name], type=_arrow_type_for(c.sql_type))
        for c in tbl.columns
    ]
    table = pa.Table.from_arrays(arrays, schema=schema)
    pq.write_table(table, out / f"{basename}.parquet")

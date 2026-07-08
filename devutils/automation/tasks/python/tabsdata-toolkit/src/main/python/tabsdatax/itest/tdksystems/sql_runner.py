"""Execute SQL via SQLAlchemy — from a file or as inline text.

Splits with sqlparse (preferred) or naive `;` split, runs in one
transaction, commits at end. For Oracle, sets ISO date/timestamp NLS
formats so 'YYYY-MM-DD' / 'YYYY-MM-DD HH24:MI:SS' literals in DDL/DML
parse correctly without per-source quoting tricks.

Statements that return rows (SELECT, INSERT...RETURNING, etc.) print
their result set to stdout as a tab-separated table — header row of
column names, one row per record. Statements that don't return rows
(DDL, plain INSERT/UPDATE/DELETE) produce no output.

DBAPI / SQLAlchemy errors are converted into ``DatagenError`` so the
top-level handler prints just the database's own message instead of a
multi-frame Python traceback.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import IO

from .errors import DatagenError


def split_sql_statements(sql: str) -> list[str]:
    """Split a SQL script into individual statements — sqlparse if available,
    else a naive `;` split. Comments-only / empty fragments are dropped and a
    trailing `;` is stripped from each. Shared by the SQLAlchemy runner and
    the saas-sql (databricks/snowflake) DDL+INSERT execution paths."""
    try:
        import sqlparse

        parts = sqlparse.split(sql)
    except ImportError:
        # Degraded fallback (sqlparse is normally a hard dep): drop full-line
        # `--` comments first so a `;` inside a comment can't split a
        # statement, then naive-split on `;`.
        no_comments = "\n".join(
            ln for ln in sql.splitlines() if not ln.lstrip().startswith("--")
        )
        parts = no_comments.split(";")
    out: list[str] = []
    for st in parts:
        st = st.strip().rstrip(";").strip()
        if st:
            out.append(st)
    return out


def run_sql_text(
    uri: str,
    sql: str,
    label: str = "sql",
    out: IO[str] | None = None,
) -> None:
    """Execute the given SQL text. ``label`` is used in error messages.

    ``out`` is where row output goes (default stdout). Pass a file handle
    or `io.StringIO` to redirect."""
    if not uri:
        raise DatagenError("SQLALCHEMY_URI is empty")

    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError

    statements = split_sql_statements(sql)

    sink = out if out is not None else sys.stdout
    try:
        engine = create_engine(uri)
        with engine.begin() as conn:
            if engine.dialect.name == "oracle":
                for stmt in (
                    "ALTER SESSION SET NLS_DATE_FORMAT='YYYY-MM-DD'",
                    "ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS'",
                    "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT='YYYY-MM-DD HH24:MI:SS'",
                ):
                    conn.exec_driver_sql(stmt)
            for stmt in statements:
                stmt = stmt.rstrip(";").strip()
                if not stmt:
                    continue
                result = conn.execute(text(stmt))
                if getattr(result, "returns_rows", False):
                    _print_result(result, sink)
    except SQLAlchemyError as exc:
        orig = getattr(exc, "orig", None) or exc
        raise DatagenError(f"SQL error while running {label}: {orig}") from None


def run_sql_file(
    uri: str,
    sql_path: str | Path,
    out: IO[str] | None = None,
) -> None:
    p = Path(sql_path)
    if not p.is_file():
        raise DatagenError(f"SQL file does not exist: {p}")
    run_sql_text(uri, p.read_text(), label=p.name, out=out)


def _print_result(result, sink: IO[str]) -> None:
    """Render a SQLAlchemy result set as a tab-separated table on ``sink``.

    None becomes empty string. Other types stringified via str(). Rendering
    each result starts on a fresh line so multiple result sets in one batch
    stay visually distinct."""
    cols = list(result.keys())
    print("\t".join(cols), file=sink)
    for row in result:
        print(
            "\t".join("" if v is None else str(v) for v in row),
            file=sink,
        )

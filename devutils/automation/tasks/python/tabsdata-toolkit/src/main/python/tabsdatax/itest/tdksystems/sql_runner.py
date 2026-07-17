#
# Copyright 2026 Tabsdata Inc.
#

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

import re
import sys
from pathlib import Path
from typing import IO

from .errors import DatagenError

_SQL_COMMENT_RE = re.compile(r"/\*.*?\*/|--[^\n]*", re.DOTALL)


def _is_only_comments(fragment: str) -> bool:
    """True if `fragment` carries no executable SQL — only `--` / block comments
    and whitespace. A DBAPI driver rejects such a statement ("can't execute an
    empty query"), so these fragments are dropped rather than run."""
    return not _SQL_COMMENT_RE.sub("", fragment).strip()


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
        if st and not _is_only_comments(st):
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

    from sqlalchemy import create_engine
    from sqlalchemy.exc import SQLAlchemyError

    statements = split_sql_statements(sql)

    sink = out if out is not None else sys.stdout
    engine = create_engine(uri)
    try:
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
                # exec_driver_sql passes the raw statement to the DBAPI driver
                # with NO SQLAlchemy bind parsing — so a `:word` token in the
                # DDL/DML (e.g. `LIMIT :size` inside a comment) is literal SQL,
                # not a bind parameter demanding a value. This runs trusted
                # generated SQL, so there are no binds to interpolate anyway.
                result = conn.exec_driver_sql(stmt)
                if getattr(result, "returns_rows", False):
                    _print_result(result, sink)
    except SQLAlchemyError as exc:
        orig = getattr(exc, "orig", None) or exc
        raise DatagenError(f"SQL error while running {label}: {orig}") from None
    finally:
        # Dispose the pool so no connection to the (per-domain) database lingers:
        # a leaked connection blocks the DROP DATABASE that create's cleanup /
        # destroy runs, orphaning the database and masking the real error as a
        # later "already exists".
        engine.dispose()


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
    stay visually distinct.

    Some drivers (e.g. ClickHouse) report ``returns_rows`` for column-less
    statements such as INSERT; those yield no columns and are skipped so they
    do not emit a blank line each."""
    cols = list(result.keys())
    if not cols:
        return
    print("\t".join(cols), file=sink)
    for row in result:
        print(
            "\t".join("" if v is None else str(v) for v in row),
            file=sink,
        )

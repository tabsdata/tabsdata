#
# Copyright 2026 Tabsdata Inc.
#

"""Execute SQL via SQLAlchemy — inline text or one-transaction-per-list.

``run_sql_text`` runs a whole script in one transaction (commit at end).
``run_sql_transactions`` runs each inner list as its own transaction (one commit
each), preserving the per-transaction boundaries a change-data-capture consumer
sees as distinct transactions. DBAPI / SQLAlchemy errors are converted to
``TdxError`` so the top-level handler prints the database's own message instead
of a Python traceback.
"""

from __future__ import annotations

import re

from tabsdatax.itest.tdksystems2.errors import TdxError

_SQL_COMMENT_RE = re.compile(r"/\*.*?\*/|--[^\n]*", re.DOTALL)


def _is_only_comments(fragment: str) -> bool:
    return not _SQL_COMMENT_RE.sub("", fragment).strip()


def split_sql_statements(sql: str) -> list[str]:
    """Split a SQL script into statements — sqlparse if available, else a naive
    `;` split. Comment-only / empty fragments are dropped; a trailing `;` is
    stripped from each."""
    try:
        import sqlparse

        parts = sqlparse.split(sql)
    except ImportError:
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


def run_sql_text(uri: str, sql: str, label: str = "sql") -> None:
    """Execute the given SQL text in a single transaction."""
    if not uri:
        raise TdxError("SQLALCHEMY_URI is empty")

    from sqlalchemy import create_engine
    from sqlalchemy.exc import SQLAlchemyError

    statements = split_sql_statements(sql)
    engine = create_engine(uri)
    try:
        with engine.begin() as conn:
            for stmt in statements:
                stmt = stmt.rstrip(";").strip()
                if stmt:
                    conn.exec_driver_sql(stmt)
    except SQLAlchemyError as exc:
        orig = getattr(exc, "orig", None) or exc
        raise TdxError(f"SQL error while running {label}: {orig}") from None
    finally:
        # Dispose the pool so no connection lingers to block a later DROP DATABASE.
        engine.dispose()


def run_sql_transactions(
    uri: str, transactions: list[list[str]], label: str = "changes"
) -> int:
    """Run each inner list of statements as ONE transaction (a single commit),
    returning the number of transactions applied. All share one engine/pool."""
    if not uri:
        raise TdxError("SQLALCHEMY_URI is empty")

    from sqlalchemy import create_engine
    from sqlalchemy.exc import SQLAlchemyError

    engine = create_engine(uri)
    applied = 0
    try:
        for stmts in transactions:
            with engine.begin() as conn:
                for stmt in stmts:
                    stmt = stmt.rstrip(";").strip()
                    if stmt:
                        conn.exec_driver_sql(stmt)
            applied += 1
    except SQLAlchemyError as exc:
        orig = getattr(exc, "orig", None) or exc
        raise TdxError(f"SQL error while running {label}: {orig}") from None
    finally:
        engine.dispose()
    return applied

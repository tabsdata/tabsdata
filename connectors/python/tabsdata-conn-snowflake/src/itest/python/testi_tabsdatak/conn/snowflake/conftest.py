#
# Copyright 2026 Tabsdata Inc.
#

"""Shared Snowflake test fixtures for this package's integration and e2e suites: a
``snowflake.connector``-backed store over whichever snowflake backend the test
declared (its ``domain``) to read tables back and drop scratch tables -- the
Snowflake analogue of the SqlAlchemyStore the SQL connectors use.

Snowflake is destination-only (no ``SnowflakeSrc``) and stages internally (PUT +
COPY INTO -- no external cloud staging). It authenticates with a Personal Access
Token supplied as the ``UserPassword`` password, and folds unquoted identifiers to
upper case, so reads/writes stay in that case."""

from __future__ import annotations

import polars as pl
import pytest


class _SnowflakeStore:
    """snowflake.connector-backed verification client over the snowflake backend
    the test declared. Reads/drops in the connection's database + schema (the same
    the connector auto-qualifies bare table names into)."""

    def __init__(self, backend) -> None:
        import snowflake.connector

        self._db = backend.database
        self._schema = backend.schema
        self._conn = snowflake.connector.connect(
            account=backend.account,
            user=backend.username,
            password=backend.pat,
            warehouse=backend.warehouse,
            database=backend.database,
            schema=backend.schema,
        )

    def _fq(self, table: str) -> str:
        # Reference identifiers bare so Snowflake upper-folds them onto the stored
        # (unquoted, upper-folded) objects tdx provisions — matching the connector.
        # The scratch table is already upper-case.
        return f"{self._db}.{self._schema}.{table}"

    def read_table(self, table: str) -> pl.DataFrame:
        cur = self._conn.cursor()
        try:
            cur.execute(f"SELECT * FROM {self._fq(table)}")
            columns = [c[0] for c in cur.description]
            rows = cur.fetchall()
        finally:
            cur.close()
        return pl.DataFrame(rows, schema=columns, orient="row")

    def count(self, table: str) -> int:
        cur = self._conn.cursor()
        try:
            cur.execute(f"SELECT count(*) FROM {self._fq(table)}")
            return cur.fetchone()[0]
        finally:
            cur.close()

    def drop_table(self, table: str) -> None:
        cur = self._conn.cursor()
        try:
            cur.execute(f"DROP TABLE IF EXISTS {self._fq(table)}")
        finally:
            cur.close()


@pytest.fixture
def snowflake():
    """Factory building a ``_SnowflakeStore`` over the snowflake backend the test
    declared -- call it with ``domain_snowflake``."""
    return _SnowflakeStore

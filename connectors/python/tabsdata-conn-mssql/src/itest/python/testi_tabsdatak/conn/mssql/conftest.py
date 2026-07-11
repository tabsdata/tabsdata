#
# Copyright 2026 Tabsdata Inc.
#

"""Shared MSSQL test fixtures for this package's integration and e2e suites: a
SQLAlchemy-backed helper over whichever mssqlserver backend the test declared
(its ``domain``, which exposes ``host`` / ``port`` / ``user`` / ``password`` /
``db``) to read tables back, drop scratch tables, and stage a deterministic
source table -- the SQL analogue of the object-store adapter the file connectors
use to verify what a plugin wrote."""

from __future__ import annotations

import urllib.parse

import polars as pl
import pytest

_ODBC_DRIVER = "ODBC Driver 18 for SQL Server"


class _MSSQLSql:
    """Read a table back to verify what a plugin/function wrote, and drop scratch
    tables between round-trips. Content comparison is done via
    ``assert_frames_match`` (String-cast, order-agnostic), so the read need not
    preserve exact dtypes. SQL Server quotes identifiers with ``[brackets]``."""

    def __init__(self, backend) -> None:
        from sqlalchemy import create_engine

        user = urllib.parse.quote(backend.user, safe="")
        password = urllib.parse.quote(backend.password, safe="")
        driver = urllib.parse.quote_plus(_ODBC_DRIVER)
        uri = (
            f"mssql+pyodbc://{user}:{password}@{backend.host}:{backend.port}/{backend.db}"
            f"?driver={driver}&TrustServerCertificate=yes"
        )
        self._engine = create_engine(uri)

    def read_table(self, table: str) -> pl.DataFrame:
        from sqlalchemy import text

        with self._engine.connect() as conn:
            rows = conn.execute(text(f"SELECT * FROM [{table}]")).mappings().all()
        return pl.DataFrame([dict(row) for row in rows])

    def drop_table(self, table: str) -> None:
        from sqlalchemy import text

        with self._engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS [{table}]"))

    def create_int_table(self, table: str, rows: list[dict]) -> None:
        """Create/replace ``table`` (integer ``id`` + text ``name``) seeded with
        ``rows`` -- a deterministic source table for the incremental
        (``initial_values``) e2e test."""
        from sqlalchemy import text

        with self._engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS [{table}]"))
            conn.execute(text(f"CREATE TABLE [{table}] (id INTEGER, name VARCHAR(64))"))
            conn.execute(
                text(f"INSERT INTO [{table}] (id, name) VALUES (:id, :name)"), rows
            )


@pytest.fixture
def sql():
    """Factory building a SQL helper over the mssqlserver backend the test
    declared -- call it with ``domain_mssql``."""
    return _MSSQLSql

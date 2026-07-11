#
# Copyright 2026 Tabsdata Inc.
#

"""Shared MySQL test fixtures for this package's integration and e2e suites: a
SQLAlchemy-backed helper over whichever mysql backend the test declared (its
``domain``, which exposes ``host`` / ``port`` / ``user`` / ``password`` / ``db``)
to read tables back and drop scratch tables -- the SQL analogue of the
object-store adapter the file connectors use to verify what a plugin wrote."""

from __future__ import annotations

import urllib.parse

import polars as pl
import pytest


class _MySQLSql:
    """Read a table back to verify what a plugin/function wrote, and drop scratch
    tables between round-trips. Content comparison is done via
    ``assert_frames_match`` (String-cast, order-agnostic), so the read need not
    preserve exact dtypes. MySQL quotes identifiers with backticks."""

    def __init__(self, backend) -> None:
        from sqlalchemy import create_engine

        user = urllib.parse.quote(backend.user, safe="")
        password = urllib.parse.quote(backend.password, safe="")
        uri = (
            f"mysql+pymysql://{user}:{password}"
            f"@{backend.host}:{backend.port}/{backend.db}"
        )
        self._engine = create_engine(uri)

    def read_table(self, table: str) -> pl.DataFrame:
        from sqlalchemy import text

        with self._engine.connect() as conn:
            rows = conn.execute(text(f"SELECT * FROM `{table}`")).mappings().all()
        return pl.DataFrame([dict(row) for row in rows])

    def drop_table(self, table: str) -> None:
        from sqlalchemy import text

        with self._engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS `{table}`"))

    def create_int_table(self, table: str, rows: list[dict]) -> None:
        """Create/replace ``table`` (integer ``id`` + text ``name``) seeded with
        ``rows`` -- a deterministic source table for the incremental
        (``initial_values``) e2e test."""
        from sqlalchemy import text

        with self._engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS `{table}`"))
            conn.execute(text(f"CREATE TABLE `{table}` (id INTEGER, name VARCHAR(64))"))
            conn.execute(
                text(f"INSERT INTO `{table}` (id, name) VALUES (:id, :name)"), rows
            )


@pytest.fixture
def sql():
    """Factory building a SQL helper over the mysql backend the test declared
    -- call it with ``domain_mysql``."""
    return _MySQLSql

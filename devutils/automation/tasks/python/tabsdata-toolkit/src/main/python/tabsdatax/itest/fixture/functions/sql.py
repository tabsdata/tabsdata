#
# Copyright 2026 Tabsdata Inc.
#

"""Helpers shared by the SQL connectors (postgres, mysql, mssql, and the rest of
the ``queries``/``tables`` family): a SQLAlchemy-backed store to seed/verify
tables and resolve the credentials a connection needs.

Connector-agnostic: the only per-connector inputs are the SQLAlchemy URI and the
dialect's identifier delimiters. Object-store/file helpers live in :mod:`.files`;
generic harness in :mod:`.harness`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

import polars as pl
import pyarrow.parquet as pq
from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.spi import TableFileInput


class SqlStore(Protocol):
    """Seed and verify tables directly (bypassing the connector) -- the SQL
    analogue of the object-store adapter the file connectors use."""

    def quote(self, identifier: str) -> str: ...

    def read_table(self, table: str) -> pl.DataFrame: ...

    def drop_table(self, table: str) -> None: ...

    def create_int_table(self, table: str, rows: list[dict]) -> None: ...


class SqlAlchemyStore(SqlStore):
    """A SQLAlchemy-backed :class:`SqlStore` shared by the SQL connectors. The
    per-connector inputs are the credential-bearing ``uri`` and the dialect's
    identifier ``delimiters`` (``('"', '"')`` postgres, backticks mysql,
    ``('[', ']')`` mssql). Content comparison is done via ``assert_frames_match``
    (String-cast, order-agnostic), so reads need not preserve exact dtypes."""

    def __init__(self, uri: str, *, delimiters: tuple[str, str]) -> None:
        from sqlalchemy import create_engine

        self._engine = create_engine(uri)
        self._open, self._close = delimiters

    def quote(self, identifier: str) -> str:
        return f"{self._open}{identifier}{self._close}"

    def read_table(self, table: str) -> pl.DataFrame:
        from sqlalchemy import text

        with self._engine.connect() as conn:
            rows = (
                conn
                .execute(text(f"SELECT * FROM {self.quote(table)}"))
                .mappings()
                .all()
            )
        return pl.DataFrame([dict(row) for row in rows])

    def drop_table(self, table: str) -> None:
        from sqlalchemy import text

        with self._engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {self.quote(table)}"))

    def create_int_table(self, table: str, rows: list[dict]) -> None:
        """Create/replace ``table`` (integer ``id`` + text ``name``) seeded with
        ``rows`` -- a deterministic source table for the incremental
        (``initial_values``) e2e test."""
        from sqlalchemy import text

        q = self.quote(table)
        with self._engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {q}"))
            conn.execute(text(f"CREATE TABLE {q} (id INTEGER, name VARCHAR(64))"))
            conn.execute(text(f"INSERT INTO {q} (id, name) VALUES (:id, :name)"), rows)


def sql_credentials(domain) -> UserPassword:
    """The ``UserPassword`` every SQL connection uses, from the backend's
    ``user`` / ``password`` -- the harness registers the two secrets."""
    return UserPassword(user=Secret(domain.user), password=Secret(domain.password))


def read_parquet_slot(tfi: TableFileInput) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(tfi.file))


def write_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path

#
# Copyright 2026 Tabsdata Inc.
#

"""Helpers shared by the SQL connectors (postgres, mysql, mssql, and the rest of
the ``queries``/``tables`` family): a SQLAlchemy-backed store to seed/verify
tables, entity/credential resolution, and generators for the scalar publisher a
SQL ``Src`` needs (one query -> one positional frame, unlike the array-mode a
file ``Src`` gets).

Connector-agnostic: the only per-connector inputs are the SQLAlchemy URI, the
dialect's identifier delimiters, and (for the publishers) the import line and
constructor expression for the connector's own ``Src``. Object-store/file
helpers live in :mod:`.files`; generic harness in :mod:`.harness`.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Protocol

import polars as pl
import pyarrow.parquet as pq
import pytest
from tabsdatak.api import Secret
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.sdk.tabsdata_server import FunctionInfo
from tabsdatak.spi import TableFileInput

from tabsdatax.itest.fixture.functions.harness import write_module


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


def entity_name(domain) -> str:
    """A deterministic provisioned table name, or skip if the domain is unseeded.
    SQL domains list entities as bare table names (unlike the object-path stems
    :func:`.files.entity_stem` handles)."""
    if not domain.entities:
        pytest.skip(
            f"domain {domain.name!r} has no entities (model={domain.model!r}); "
            f"provision the domain with data before running this test",
        )
    return sorted(domain.entities)[0]


def scratch_table(prefix: str = "td_it") -> str:
    """A unique scratch table name, so a re-run never collides with a prior one."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def read_parquet_slot(tfi: TableFileInput) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(tfi.file))


def write_parquet(path: Path, rows: list[dict]) -> Path:
    pl.DataFrame(rows).write_parquet(path, use_pyarrow=True)
    return path


def write_sql_publisher(
    project_root: Path, *, source_import: str, source_expr: str, output_table: str
) -> FunctionInfo:
    """A publisher reading one query into ``output_table``. A SQL ``Src`` maps
    each query to a single positional ``TableFrame`` (not the array-mode a file
    ``Src`` gets), so a one-query source yields one frame."""
    module = write_module(
        project_root,
        "e2e_publisher",
        "from tabsdatak.api import publisher, TableFrameSpec\n"
        f"{source_import}\n"
        "\n"
        f"@publisher(source={source_expr}, output_tables=[{output_table!r}])\n"
        "def e2e_pub(frame: TableFrameSpec) -> TableFrameSpec:\n"
        "    return frame\n",
    )
    return module.e2e_pub


def write_watermark_publisher(
    project_root: Path,
    *,
    source_import: str,
    source_expr: str,
    output_table: str,
    bind: str,
    advance_to: int,
) -> FunctionInfo:
    """A publisher that advances an ``initial_values`` watermark: it declares the
    optional trailing ``ctx: TrxCtx`` and calls ``ctx.set_attr(bind, advance_to)``
    so the next run re-reads the persisted value. ``source_expr`` carries the
    seed (``initial_values={...}``)."""
    module = write_module(
        project_root,
        "e2e_publisher",
        "from tabsdatak.api import publisher, TableFrameSpec, TrxCtx\n"
        f"{source_import}\n"
        "\n"
        f"@publisher(source={source_expr}, output_tables=[{output_table!r}])\n"
        "def e2e_pub(frame: TableFrameSpec, ctx: TrxCtx) -> TableFrameSpec:\n"
        f"    ctx.set_attr({bind!r}, {advance_to!r})\n"
        "    return frame\n",
    )
    return module.e2e_pub

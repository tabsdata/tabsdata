#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import tzinfo
from typing import Any, Literal

import sqlalchemy.types as sa_types
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.elements import quoted_name

logger = logging.getLogger(__name__)

LargeColumnKind = Literal["text", "object"]
"""How a large column travels through CDC. `"text"`: the database's
capture machinery delivers the value as any other column, so it is
emitted as is, subject to the `max_large_text_length` limit.
`"object"`: the value is not captured, or only with extra setup on the
database, so the column is emitted with a null value. Bounded strings
(`VARCHAR(n)` and friends) are regular columns, however large `n`."""

CdcIngesterType = Literal[
    "binlog",
    "sqlreplica",
    "wal2json",
]
"""The CDC capture mechanism a dialect drives: `"binlog"` (MySQL),
`"sqlreplica"` (Db2 SQL Replication) or `"wal2json"` (Postgres
logical replication)."""


class Dialect(ABC):
    """Per-database hooks the shared CDC loop needs from a dialect.

    :categories: spi

    Each SQL CDC connector supplies one concrete subclass to bridge the
    dialect-agnostic ingester to its database: identifier quoting and
    case-folding, catalog column introspection, large-column
    classification, the server timezone and the mapping of
    capture-specific dtypes onto SQLAlchemy types.
    """

    @property
    @abstractmethod
    def open_quote(self) -> str:
        """The identifier opening-quote character (e.g. a double quote or backtick)."""

    @property
    @abstractmethod
    def close_quote(self) -> str:
        """The identifier closing-quote character."""

    @abstractmethod
    def default_case_fold(self, name: str) -> str:
        """Fold an unquoted identifier to the dialect's default case."""

    def quote_identifier(self, name: str) -> str:
        """Quote `name` verbatim for use in a statement, doubling any
        embedded closing-quote character."""
        escaped = name.replace(self.close_quote, self.close_quote * 2)
        return f"{self.open_quote}{escaped}{self.close_quote}"

    @abstractmethod
    def get_catalog_columns(
        self,
        connection: Any,
        schema: str,
        table: str,
    ) -> list[str]:
        """Return the column names of `schema.table` from the catalog,
        in definition order."""

    @abstractmethod
    def large_column_kind(self, satype: Any) -> LargeColumnKind | None:
        """Classify a SQLAlchemy type as a `"text"` or `"object"` large
        column (see `LargeColumnKind`), or `None` for a regular column."""

    @abstractmethod
    def server_timezone(self, engine: Any) -> tzinfo | None:
        """Return the database server's timezone, or `None` if
        unknown; used to interpret naive CDC timestamps."""

    def cdc_to_sa(
        self,
        ingester_type: CdcIngesterType,
        cdc_dtype: Any,
    ) -> sa_types.TypeEngine:
        """Map a capture-specific dtype onto a SQLAlchemy type.

        The default raises `NotImplementedError`; a dialect overrides
        it for each `ingester_type` it supports.
        """
        raise NotImplementedError(
            f"{type(self).__name__}.cdc_to_sa is not implemented "
            f"for ingester type {ingester_type!r}"
        )

    def default_schema(self, inspector: Inspector) -> str:
        """The schema used to qualify an UNqualified table name -- the
        connection's effective default schema, matching how batch SQL
        resolves an unqualified table.

        The default returns SQLAlchemy's reported default schema, which is
        correct for dialects whose default schema is the one an unqualified
        name resolves against (e.g. MySQL's current database, Postgres'
        ``current_schema()``). A dialect whose effective default differs --
        e.g. Db2, where the ``currentSchema=`` connection attribute governs
        resolution but SQLAlchemy still reports the authorization-id schema --
        overrides this.
        """
        return inspector.default_schema_name

    def parse_qualified_table(
        self, spec: str
    ) -> tuple[quoted_name | None, quoted_name]:
        """Split a `schema.table` spec into quoted schema and table
        identifiers, honouring the dialect's quoting and doubled-quote
        escaping.

        An UNqualified `table` spec (no separator) yields a ``None`` schema;
        the caller fills it from the connection's default schema (see
        :meth:`default_schema`), matching batch-SQL resolution of unqualified
        names. Raises `ValueError` on a malformed spec."""
        raw = spec
        spec = spec.strip()
        oq = self.open_quote
        cq = self.close_quote

        def token(s: str, start: int) -> tuple[quoted_name, int]:
            if start >= len(s):
                raise ValueError(
                    f"Invalid qualified table spec: {raw!r}; unexpected end of string"
                )
            if s[start] == oq:
                pos = start + 1
                while True:
                    end = s.find(cq, pos)
                    if end == -1:
                        raise ValueError(
                            f"Invalid qualified table spec: {raw!r}; "
                            f"unclosed {oq} at position {start}"
                        )
                    if end + 1 < len(s) and s[end + 1] == cq:
                        pos = end + 2
                        continue
                    break
                escaped = s[start + 1 : end]
                name = escaped.replace(cq + cq, cq)
                if not name:
                    raise ValueError(
                        f"Invalid qualified table spec: {raw!r}; "
                        f"empty quoted identifier at position {start}"
                    )
                return quoted_name(name, quote=True), end + 1
            else:
                end = s.find(".", start)
                if end == -1:
                    end = len(s)
                name = s[start:end].strip()
                if not name:
                    raise ValueError(
                        f"Invalid qualified table spec: {raw!r}; "
                        f"empty identifier at position {start}"
                    )
                if oq in name or cq in name:
                    raise ValueError(
                        f"Invalid qualified table spec: {raw!r}; "
                        f"unquoted identifier contains quote "
                        f"character at position {start}"
                    )
                return quoted_name(name, quote=False), end

        first, idx = token(spec, 0)
        if idx >= len(spec):
            # Unqualified: the single token is the table; the schema is filled
            # from the connection default by the caller (see default_schema).
            return None, first
        if spec[idx] != ".":
            raise ValueError(
                f"Invalid qualified table spec: {raw!r}, expected '.' after schema"
            )
        table, idx = token(spec, idx + 1)
        if idx != len(spec):
            raise ValueError(
                f"Invalid qualified table spec: {raw!r}, "
                f"unexpected content after table name"
            )
        return first, table

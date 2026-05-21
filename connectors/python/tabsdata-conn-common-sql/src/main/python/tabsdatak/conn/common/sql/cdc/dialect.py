"""Per-database dialect surface that the CDC ingester depends on.

Subclasses live in per-database connectors (``conn-mysql``,
``conn-postgres``, ``conn-db2``) and bridge dialect-specific
catalog introspection + identifier folding + raw-type mapping to
SQLAlchemy. Kept inside ``cdc/`` (rather than at the top of
``conn-common-sql``) because right now the CDC layer is the only
consumer; if/when the batch SQL path also needs this abstract,
promote it then.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import tzinfo
from typing import Any, Literal

import sqlalchemy.types as sa_types
from sqlalchemy.sql.elements import quoted_name


# ``CdcIngesterType`` discriminates which raw-type set
# ``Dialect.cdc_to_sa`` should expect. ``binlog`` -> MySQL
# pymysqlreplication TYPE codes; ``wal2json`` -> Postgres logical
# decoding type names; ``sqlreplica`` -> IBM SQL Replication.
CdcIngesterType = Literal["binlog", "sqlreplica", "wal2json"]


class Dialect(ABC):
    """Methods every per-database dialect implements for the CDC
    ingester. The new dialects own only the bits that differ across
    databases -- the ingester / sentinels / runner are dialect-
    agnostic and live in this package."""

    @property
    @abstractmethod
    def open_quote(self) -> str: ...

    @property
    @abstractmethod
    def close_quote(self) -> str: ...

    @abstractmethod
    def default_case_fold(self, name: str) -> str:
        """Normalize an identifier the same way the database's
        unquoted-identifier rules do (upper-fold for Oracle/Db2,
        lower-fold for Postgres, unchanged for MySQL)."""

    @abstractmethod
    def parse_qualified_table(
        self, tablespec: str
    ) -> tuple[quoted_name, quoted_name]:
        """Parse ``"schema.table"`` (or its dialect-specific quoted
        equivalent) into ``(schema, table)`` ``quoted_name`` pairs.
        Raises ``ValueError`` when the spec is malformed."""

    @abstractmethod
    def get_catalog_columns(
        self,
        connection: Any,
        schema: str,
        table: str,
    ) -> list[str]:
        """Return the catalog's column names for ``schema.table`` in
        catalog order. Used to cross-check SQLAlchemy's introspection
        (catalog-vs-SA disagreements raise SQL_CDC_4/5)."""

    @abstractmethod
    def is_large_column(self, satype: Any) -> bool:
        """``True`` when ``satype`` represents a BLOB / CLOB / LOB
        family member that should be excluded from CDC ingestion."""

    @abstractmethod
    def server_timezone(self, engine: Any) -> tzinfo | None:
        """The database server's session timezone (used to localize
        naive datetimes coming out of the change stream). ``None``
        if the dialect has no notion or it cannot be discovered."""

    def cdc_to_sa(
        self,
        ingester_type: CdcIngesterType,
        cdc_dtype: Any,
    ) -> sa_types.TypeEngine:
        """Map a dialect's raw CDC type code (pymysqlreplication
        TYPE constants, wal2json type names, ...) to a SQLAlchemy
        type. Subclasses override; the base raises
        ``NotImplementedError`` so a connector that hasn't wired up
        a particular ingester surfaces it loudly."""
        raise NotImplementedError(
            f"Dialect {type(self).__name__}: cdc_to_sa not implemented "
            f"for ingester_type={ingester_type!r}"
        )

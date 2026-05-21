"""Abstract CDC ingester.

Concrete subclasses (``BinlogIngester`` for MySQL, ``WalIngester``
for Postgres, ``CaptureIngester`` for Db2) implement the change-
stream specifics; the methods on this base handle the dialect-
agnostic parts: table identity, schema introspection, schema
registry initialisation, server-tz localisation."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable
from datetime import datetime, tzinfo
from typing import Any

import sqlalchemy as sa
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.elements import quoted_name

from tabsdatak.conn.common.sql.cdc._column import TrackedColumn
from tabsdatak.conn.common.sql.cdc._model import (
    CdcChange,
    CdcOffset,
    TableObject,
    TrackedTable,
)
from tabsdatak.conn.common.sql.cdc._schema import SchemaRegistry
from tabsdatak.conn.common.sql.cdc.dialect import CdcIngesterType, Dialect
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat, QualifiedTablesSpec

logger = logging.getLogger(__name__)


class CdcIngester(ABC):
    """Reads changes from a source database's change stream and
    yields ``CdcChange`` events. Subclasses own ``poll_changes``
    (their wire format), ``base_offset`` / ``commit_offset`` (their
    offset shape), ``open_session`` / ``close_session`` (their
    connection lifecycle), and ``dialect`` (their per-database
    catalog/identifier rules).
    """

    def __init__(self, tables: QualifiedTablesSpec):
        self._tables = tables
        self._tracked_tables: dict[str, TrackedTable] = {}
        self._schema_registry: SchemaRegistry | None = None
        self._server_tz: tzinfo | None = None

    @property
    @abstractmethod
    def ingester_type(self) -> CdcIngesterType: ...

    @property
    def tables(self) -> QualifiedTablesSpec:
        return self._tables

    @property
    def tracked_tables(self) -> dict[str, TrackedTable]:
        return self._tracked_tables

    @property
    def schema_registry(self) -> SchemaRegistry | None:
        return self._schema_registry

    @property
    def server_tz(self) -> tzinfo | None:
        return self._server_tz

    def localize_values(self, values: dict | None) -> dict | None:
        """Best-effort tz attachment for naive datetimes coming out
        of the change stream. No-op when the dialect did not
        provide a server timezone."""
        if values is None or self._server_tz is None:
            return values
        for key, value in values.items():
            if isinstance(value, datetime) and value.tzinfo is None:
                values[key] = value.replace(tzinfo=self._server_tz)
        return values

    # ------------------------------------------------------------------
    # Per-dialect lifecycle: subclasses own these
    # ------------------------------------------------------------------

    @abstractmethod
    def open_session(self) -> Any:
        """Open the connection / replication stream used during
        ``poll_changes``. The runner calls this once per run."""

    @abstractmethod
    def close_session(self, session: Any) -> None:
        """Tear down the session opened in ``open_session``."""

    @property
    @abstractmethod
    def dialect(self) -> Dialect: ...

    @abstractmethod
    def base_offset(self) -> CdcOffset:
        """Build the per-ingester ``CdcOffset`` seed -- the starting
        position the buffer sentinel hands to the runner. Subclasses
        translate the user-supplied ``start_from`` here."""

    @abstractmethod
    def commit_offset(self, offset: CdcOffset) -> None:
        """Persist the current offset back into the change stream
        (acks, replica progress markers). Called after each
        successful disk flush so the source can prune."""

    @abstractmethod
    def poll_changes(self, session: Any) -> Iterable[CdcChange]:
        """Yield each new ``CdcChange`` available since the last
        call. Iterators are short-lived (one poll cycle each); the
        runner calls this repeatedly inside its loop."""

    # ------------------------------------------------------------------
    # Identifier + catalog helpers (dialect-aware but dialect-agnostic)
    # ------------------------------------------------------------------

    def resolve_identifier(self, name: quoted_name) -> str:
        """Apply the dialect's case-folding to an unquoted
        identifier; pass quoted identifiers through verbatim."""
        if name.quote:
            return str(name)
        return self.dialect.default_case_fold(str(name))

    def gather_table_identity(
        self,
        tablespec: str,
        inspector: sa.engine.reflection.Inspector,
    ) -> TrackedTable:
        """Parse ``tablespec`` into a ``(schema, table)`` pair via
        the dialect, verify it exists on the connected DB, and seed
        a ``TrackedTable`` whose ``data_table`` and ``cdc_table``
        both point at the source. Capture-target connectors override
        and divert ``cdc_table`` elsewhere."""
        try:
            schema, table = self.dialect.parse_qualified_table(tablespec)
        except ValueError as error:
            raise SqlCdcErrorCode.SQL_CDC_1.exception(
                table=tablespec, reason=str(error),
            ) from error

        db_schema = self.resolve_identifier(schema)
        db_table = self.resolve_identifier(table)

        sa_dialect = inspector.bind.dialect
        normalized_schema = sa_dialect.normalize_name(
            quoted_name(db_schema, quote=False)
        )
        normalized_table = sa_dialect.normalize_name(
            quoted_name(db_table, quote=False)
        )

        if not inspector.has_table(
            normalized_table,
            schema=normalized_schema,
        ):
            raise SqlCdcErrorCode.SQL_CDC_2.exception(
                schema=db_schema, table=db_table,
            )

        table_object = TableObject(schema=db_schema, table=db_table)
        return TrackedTable(
            data_table=table_object,
            cdc_table=table_object,
        )

    def gather_table_structure(
        self,
        tracked_table: TrackedTable,
        inspector: sa.engine.reflection.Inspector,
        before_img_prefix: str | None = None,
    ) -> None:
        """Fill ``tracked_table.columns`` and ``.pk`` from
        SQLAlchemy + the dialect's catalog view, cross-checking
        catalog vs SA column counts and ordering. Large columns
        (LOB family) are dropped per ``dialect.is_large_column``.
        ``before_img_prefix`` -- used by IBM SQL Replication -- maps
        each column to its old-image sibling."""
        sa_dialect = inspector.bind.dialect
        db_schema = tracked_table.data_table.schema
        db_table = tracked_table.data_table.table

        normalized_schema = sa_dialect.normalize_name(
            quoted_name(db_schema, quote=False)
        )
        normalized_table = sa_dialect.normalize_name(
            quoted_name(db_table, quote=False)
        )

        sa_columns = inspector.get_columns(
            normalized_table,
            schema=normalized_schema,
        )
        if not sa_columns:
            raise SqlCdcErrorCode.SQL_CDC_3.exception(
                schema=db_schema, table=db_table,
            )

        with inspector.bind.connect() as connection:
            db_columns = self.dialect.get_catalog_columns(
                connection,
                db_schema,
                db_table,
            )

        if len(db_columns) != len(sa_columns):
            raise SqlCdcErrorCode.SQL_CDC_4.exception(
                schema=db_schema, table=db_table,
                catalog=len(db_columns), sa=len(sa_columns),
            )

        for db_column, sa_column in zip(db_columns, sa_columns):
            if db_column.upper() != sa_column["name"].upper():
                raise SqlCdcErrorCode.SQL_CDC_5.exception(
                    schema=db_schema, table=db_table,
                    catalog=db_column, sa=sa_column["name"],
                )

        columns = []
        for db_column, sa_column in zip(db_columns, sa_columns):
            if self.dialect.is_large_column(sa_column["type"]):
                logger.info(
                    "Excluding large column %r (%s) from %s.%s",
                    db_column, type(sa_column["type"]).__name__,
                    db_schema, db_table,
                )
                continue
            oname = f"{before_img_prefix}{db_column}" if before_img_prefix else None
            columns.append(
                TrackedColumn(
                    name=db_column,
                    satype=sa_column["type"],
                    oname=oname,
                    sa_name=sa_dialect.normalize_name(db_column),
                    sa_oname=(sa_dialect.normalize_name(oname) if oname else None),
                )
            )

        tracked_table.columns = columns
        tracked_table.build_pa_dtypes()
        tracked_table.pk = self.gather_table_pk(
            inspector,
            normalized_table,
            normalized_schema,
            sa_columns,
            db_columns,
        )

    @staticmethod
    def gather_table_pk(
        inspector: sa.engine.reflection.Inspector,
        normalized_table: str,
        normalized_schema: str,
        sa_columns: list[ReflectedColumn],
        db_columns: list[str],
    ) -> list[str]:
        pk_info = inspector.get_pk_constraint(
            table_name=normalized_table,
            schema=normalized_schema,
        )
        inspector_pk = pk_info.get("constrained_columns", []) if pk_info else []
        # Cross-map the SA-name PK columns to their catalog names so
        # downstream emits the same identifier the source uses.
        inspector_to_catalog = {
            sa_col["name"]: db_col
            for sa_col, db_col in zip(sa_columns, db_columns)
        }
        return [inspector_to_catalog.get(pk, pk) for pk in inspector_pk]

    # ------------------------------------------------------------------
    # Schema registry init + tablespec normalisation
    # ------------------------------------------------------------------

    def init_schema_registry(self, cdc_format: CdcFormat) -> None:
        """One-shot init of the schema registry. Called by the
        runner after ``collect_tables`` so subsequent change-stream
        DDL events have a place to register new versions."""
        ingester_type = self.ingester_type

        def cdc_type_mapper(raw_type: Any) -> Any:
            return self.dialect.cdc_to_sa(ingester_type, raw_type)

        self._schema_registry = SchemaRegistry(
            type_mapper=cdc_type_mapper,
            cdc_format=cdc_format,
        )

    def prepare_inspector(self, inspector: sa.engine.reflection.Inspector) -> None:
        """Override hook for dialects that need to tweak the
        SQLAlchemy inspector before collection (e.g. opening a
        catalog cursor)."""
        pass

    def resolve_tablespecs(self) -> list[str]:
        """Normalise ``self._tables`` to a list -- ``Src.tables``
        is ``str | list[str]`` per ``QualifiedTablesSpec``."""
        tablespecs = self._tables
        if isinstance(tablespecs, str):
            tablespecs = [tablespecs]
        return tablespecs

    def collect_tables(
        self,
        session: Any,
        cdc_format: CdcFormat = CdcFormat(),
    ) -> dict[str, TrackedTable]:
        """Walk every tablespec, gather identity + structure, and
        return the populated ``self._tracked_tables``. Called once
        by the runner on startup."""
        inspector: Inspector = sa.inspect(session)
        self.prepare_inspector(inspector)
        result: dict[str, TrackedTable] = {}

        for tablespec in self.resolve_tablespecs():
            tracked_table = self.gather_table_identity(tablespec, inspector)
            self.gather_table_structure(tracked_table, inspector)
            result[tracked_table.qualified] = tracked_table

        self._tracked_tables = result
        self.init_schema_registry(cdc_format)

        # Seed each tracked table's schema-version 0 from the
        # catalog snapshot. The registry then versions further
        # log-stream DDL events on top.
        registry = self._schema_registry
        if registry is not None:
            for table_key, tracked_table in result.items():
                registry.register_from_catalog(table_key, tracked_table)

        return result

    def harvest_tables(
        self,
        session: Any,
        cdc_format: CdcFormat = CdcFormat(),
    ) -> dict[str, TrackedTable]:
        """Default ``harvest_tables`` == ``collect_tables``.
        Connectors that need additional setup (capture-table
        registration, snapshot priming) override and call
        ``super().harvest_tables`` first."""
        return self.collect_tables(session, cdc_format)

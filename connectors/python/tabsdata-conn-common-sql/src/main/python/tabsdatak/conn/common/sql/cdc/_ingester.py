#
# Copyright 2026 Tabs Data Inc.
#

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

from tabsdatak.conn.common.sql.cdc._model import (
    CdcChange,
    CdcOffset,
    TableObject,
    TrackedColumn,
    TrackedTable,
)
from tabsdatak.conn.common.sql.cdc._schema import SchemaRegistry
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat, QualifiedTablesSpec
from tabsdata.connector.sql.common._generic.dialect import (
    CdcIngesterType,
    Dialect,
)
from tabsdata.exceptions import CdcError, ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class CdcIngester(ABC):
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
        if values is None or self._server_tz is None:
            return values
        for key, value in values.items():
            if isinstance(value, datetime) and value.tzinfo is None:
                values[key] = value.replace(tzinfo=self._server_tz)
        return values

    def open_session(self) -> Any: ...

    @abstractmethod
    def close_session(self, session: Any) -> None: ...

    @property
    @abstractmethod
    def dialect(self) -> Dialect: ...

    def resolve_identifier(self, name: quoted_name) -> str:
        if name.quote:
            return str(name)
        return self.dialect.default_case_fold(str(name))

    def gather_table_identity(
        self,
        tablespec: str,
        inspector: sa.engine.reflection.Inspector,
    ) -> TrackedTable:
        try:
            schema, table = self.dialect.parse_qualified_table(tablespec)
        except ValueError as error:
            raise CdcError(
                ErrorCode.CDC1,
                tablespec,
                error,
            ) from error

        db_schema = self.resolve_identifier(schema)
        db_table = self.resolve_identifier(table)

        sa_dialect = inspector.bind.dialect
        normalized_schema = sa_dialect.normalize_name(
            quoted_name(db_schema, quote=False)
        )
        normalized_table = sa_dialect.normalize_name(quoted_name(db_table, quote=False))

        if not inspector.has_table(
            normalized_table,
            schema=normalized_schema,
        ):
            raise CdcError(
                ErrorCode.CDC2,
                db_schema,
                db_table,
            )

        table_object = TableObject(
            schema=db_schema,
            table=db_table,
        )
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
        sa_dialect = inspector.bind.dialect
        db_schema = tracked_table.data_table.schema
        db_table = tracked_table.data_table.table

        normalized_schema = sa_dialect.normalize_name(
            quoted_name(db_schema, quote=False)
        )
        normalized_table = sa_dialect.normalize_name(quoted_name(db_table, quote=False))

        sa_columns = inspector.get_columns(
            normalized_table,
            schema=normalized_schema,
        )
        if not sa_columns:
            raise CdcError(
                ErrorCode.CDC3,
                db_schema,
                db_table,
            )

        with inspector.bind.connect() as connection:
            db_columns = self.dialect.get_catalog_columns(
                connection,
                db_schema,
                db_table,
            )

        if len(db_columns) != len(sa_columns):
            raise CdcError(
                ErrorCode.CDC7,
                db_schema,
                db_table,
                len(db_columns),
                len(sa_columns),
            )

        for db_column, sa_column in zip(db_columns, sa_columns):
            if db_column.upper() != sa_column["name"].upper():
                raise CdcError(
                    ErrorCode.CDC8,
                    db_schema,
                    db_table,
                    db_column,
                    sa_column["name"],
                )

        columns = []
        for db_column, sa_column in zip(db_columns, sa_columns):
            if self.dialect.is_large_column(sa_column["type"]):
                logger.info(
                    f"Excluding large column '{db_column}' "
                    f"({type(sa_column['type']).__name__}) "
                    f"from {db_schema}.{db_table}"
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
        inspector_to_catalog = {
            sa_col["name"]: db_col for sa_col, db_col in zip(sa_columns, db_columns)
        }
        return [inspector_to_catalog.get(pk, pk) for pk in inspector_pk]

    def init_schema_registry(self, cdc_format: CdcFormat) -> None:
        ingester_type = self.ingester_type

        def cdc_type_mapper(raw_type: Any) -> Any:
            return self.dialect.cdc_to_sa(ingester_type, raw_type)

        self._schema_registry = SchemaRegistry(
            type_mapper=cdc_type_mapper,
            cdc_format=cdc_format,
        )

    def prepare_inspector(self, inspector: sa.engine.reflection.Inspector) -> None:
        pass

    def resolve_tablespecs(self) -> list[str]:
        tablespecs = self._tables
        if isinstance(tablespecs, str):
            tablespecs = [tablespecs]
        return tablespecs

    def collect_tables(
        self,
        session: Any,
        cdc_format: CdcFormat = CdcFormat(),
    ) -> dict[str, TrackedTable]:
        inspector: Inspector = sa.inspect(session)
        self.prepare_inspector(inspector)
        result: dict[str, TrackedTable] = {}

        for tablespec in self.resolve_tablespecs():
            tracked_table = self.gather_table_identity(tablespec, inspector)
            self.gather_table_structure(tracked_table, inspector)
            result[tracked_table.qualified] = tracked_table

        self._tracked_tables = result
        self.init_schema_registry(cdc_format)
        for qualified, tracked_table in result.items():
            self._schema_registry.register_from_catalog(qualified, tracked_table)

        return result

    def harvest_tables(
        self,
        session: Any,
        cdc_format: CdcFormat = CdcFormat(),
    ) -> dict[str, TrackedTable]:
        inspector: Inspector = sa.inspect(session)
        self.prepare_inspector(inspector)
        result: dict[str, TrackedTable] = {}

        for tablespec in self.resolve_tablespecs():
            tracked_table = self.gather_table_identity(tablespec, inspector)
            result[tracked_table.qualified] = tracked_table

        self._tracked_tables = result
        self.init_schema_registry(cdc_format)

        return result

    @abstractmethod
    def base_offset(self) -> CdcOffset: ...

    @abstractmethod
    def commit_offset(self, offset: CdcOffset) -> None: ...

    @abstractmethod
    def poll_changes(self, session: Any) -> Iterable[CdcChange]: ...

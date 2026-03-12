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
from sqlalchemy.sql.elements import quoted_name

from tabsdata.connector.cdc.common._model import (
    CdcChange,
    TableObject,
    TrackedColumn,
    TrackedTable,
)
from tabsdata.connector.cdc.common.typing import CdcFormatSpec, QualifiedTablesSpec
from tabsdata.connector.sql.common._generic.dialect import Dialect as SqlDialect
from tabsdata.exceptions import CdcError, ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class CdcIngester(ABC):
    def __init__(self, tables: QualifiedTablesSpec):
        self._tables = tables
        self._tracked_tables: dict[str, TrackedTable] = {}
        self._server_tz: tzinfo | None = None

    @property
    def tables(self) -> QualifiedTablesSpec:
        return self._tables

    @property
    def tracked_tables(self) -> dict[str, TrackedTable]:
        return self._tracked_tables

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

    @abstractmethod
    def open_session(self) -> Any: ...

    @abstractmethod
    def close_session(self, session: Any) -> None: ...

    @property
    @abstractmethod
    def dialect(self) -> SqlDialect: ...

    def resolve_identifier(self, name: quoted_name) -> str:
        if name.quote:
            return str(name)
        return self.dialect.default_case_fold(str(name))

    # noinspection DuplicatedCode
    def collect_tables(
        self,
        session: Any,
        cdc_format: CdcFormatSpec = CdcFormatSpec(),
    ) -> dict[str, TrackedTable]:
        tablespecs = self._tables
        if isinstance(tablespecs, str):
            tablespecs = [tablespecs]

        inspector: sa.engine.reflection.Inspector = sa.inspect(session)
        sa_dialect: sa.engine.interfaces.Dialect = inspector.bind.dialect
        result: dict[str, TrackedTable] = {}

        for tablespec in tablespecs:
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
                raise CdcError(
                    ErrorCode.CDC2,
                    db_schema,
                    db_table,
                )

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

            with session.connect() as connection:
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

            columns = [
                TrackedColumn(
                    name=db_column,
                    satype=sa_column["type"],
                    sa_name=sa_dialect.normalize_name(db_column),
                )
                for db_column, sa_column in zip(db_columns, sa_columns)
            ]

            pk_info = inspector.get_pk_constraint(
                table_name=normalized_table,
                schema=normalized_schema,
            )
            inspector_pk = pk_info.get("constrained_columns", []) if pk_info else []
            inspector_to_catalog = {
                sa_column["name"]: db_column
                for sa_column, db_column in zip(sa_columns, db_columns)
            }
            pk_columns = [inspector_to_catalog.get(pk, pk) for pk in inspector_pk]

            table_object = TableObject(
                schema=db_schema,
                table=db_table,
            )
            tracked_table = TrackedTable(
                data_table=table_object,
                cdc_table=table_object,
                columns=columns,
                pk=pk_columns,
                cdc_format=cdc_format,
            )
            result[table_object.qualified] = tracked_table

        self._tracked_tables = result
        return result

    @abstractmethod
    def poll_changes(self, session: Any) -> Iterable[CdcChange]: ...

    @abstractmethod
    def confirm_position(self) -> None: ...

    @abstractmethod
    def retrieve_position(self) -> Any: ...

    @abstractmethod
    def save_position(self, position: Any) -> None: ...

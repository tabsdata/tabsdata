#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

import sqlalchemy as sa
from sqlalchemy.sql.elements import quoted_name

from tabsdata.connector.cdc.common._model import (
    CdcEvent,
    TableObject,
    TrackedColumn,
    TrackedTable,
)
from tabsdata.connector.cdc.common.typing import QualifiedTablesSpec
from tabsdata.connector.sql.common._generic.dialect import Dialect as SqlDialect
from tabsdata.exceptions import CdcError, ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class CdcIngester(ABC):
    def __init__(self, tables: QualifiedTablesSpec):
        self._tables = tables

    @property
    def tables(self) -> QualifiedTablesSpec:
        return self._tables

    @abstractmethod
    def open_session(self) -> Any: ...

    @abstractmethod
    def close_session(self, session: Any) -> None: ...

    @property
    @abstractmethod
    def dialect(self) -> SqlDialect: ...

    def _canonicalize(self, name: quoted_name) -> quoted_name:
        if name.quote and str(name) == self.dialect.default_case_fold(str(name)):
            return quoted_name(str(name), quote=False)
        return name

    # noinspection DuplicatedCode
    def collect_tables(self, session: Any) -> dict[str, TrackedTable]:
        table_specs = self._tables
        if isinstance(table_specs, str):
            table_specs = [table_specs]

        inspector: sa.engine.reflection.Inspector = sa.inspect(session)
        sa_dialect: sa.engine.interfaces.Dialect = inspector.bind.dialect
        result: dict[str, TrackedTable] = {}

        for table_spec in table_specs:
            try:
                schema, table = self.dialect.parse_qualified_table(table_spec)
            except ValueError as error:
                raise CdcError(
                    ErrorCode.CDC1,
                    table_spec,
                    error,
                ) from error

            # If a quoted identifier matches the database's default case
            # fold, strip the quote flag so normalize_name produces the
            # same key as the unquoted equivalent.  For example, in DB2
            # "DB2INST1" (quoted uppercase) and db2inst1 (unquoted) refer
            # to the same object and should yield the same dict key.
            schema = self._canonicalize(schema)
            table = self._canonicalize(table)

            normalized_schema = sa_dialect.normalize_name(schema)
            normalized_table = sa_dialect.normalize_name(table)

            if not inspector.has_table(
                normalized_table,
                schema=normalized_schema,
            ):
                raise CdcError(
                    ErrorCode.CDC2,
                    normalized_schema,
                    normalized_table,
                )

            columns_info = inspector.get_columns(
                normalized_table,
                schema=normalized_schema,
            )
            if not columns_info:
                raise CdcError(
                    ErrorCode.CDC3,
                    normalized_schema,
                    normalized_table,
                )

            pk_info = inspector.get_pk_constraint(
                normalized_table,
                schema=normalized_schema,
            )
            pk_columns = pk_info.get("constrained_columns", []) if pk_info else []

            columns = [
                TrackedColumn(
                    name=column_info["name"],
                    satype=column_info["type"],
                )
                for column_info in columns_info
            ]

            table_object = TableObject(
                schema=normalized_schema,
                table=normalized_table,
            )
            tracked_table = TrackedTable(
                data_table=table_object,
                cdc_table=table_object,
                columns=columns,
                pk=pk_columns,
            )
            result[table_object.qualified] = tracked_table

        return result

    @abstractmethod
    def poll_events(self, session: Any) -> list[CdcEvent]: ...

    @abstractmethod
    def retrieve_position(self) -> Any: ...

    @abstractmethod
    def save_position(self, position: Any) -> None: ...

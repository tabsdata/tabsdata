#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import Any

import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.sql.elements import quoted_name

from tabsdata.connector.cdc.common._ingester import (
    CdcIngester,
)
from tabsdata.connector.cdc.common._model import (
    CdcChange,
    TableObject,
    TrackedColumn,
    TrackedTable,
)
from tabsdata.connector.cdc.common.typing import CdcFormatSpec, QualifiedTablesSpec
from tabsdata.connector.cdc.db2._connector import Db2CdcConn
from tabsdata.connector.cdc.db2.typing import Db2CdcStartFromSpec
from tabsdata.connector.sql.common._database.db2.dialect import Db2Dialect
from tabsdata.exceptions import CdcError, ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ASN_SCHEMA = "ASN"


class SqlReplicationIngester(CdcIngester):
    def __init__(
        self,
        conn: Db2CdcConn,
        tables: QualifiedTablesSpec,
        start_from: Db2CdcStartFromSpec | None = None,
    ):
        super().__init__(tables)
        self._conn = conn
        self._start_from = start_from

    @property
    def conn(self) -> Db2CdcConn:
        return self._conn

    @property
    def start_from(self) -> Db2CdcStartFromSpec | None:
        return self._start_from

    @property
    def dialect(self) -> Db2Dialect:
        return Db2Dialect()

    def open_session(self) -> Any:
        return self._conn._connect()

    def close_session(self, session: Any) -> None:
        session.dispose()

    # noinspection DuplicatedCode
    def collect_tables(
        self,
        session: Any,
        cdc_format: CdcFormatSpec = CdcFormatSpec(),
    ) -> dict[str, TrackedTable]:
        table_specs = self._tables
        if isinstance(table_specs, str):
            table_specs = [table_specs]

        inspector: sa.engine.reflection.Inspector = sa.inspect(session)
        sa_dialect: sa.engine.interfaces.Dialect = inspector.bind.dialect
        result: dict[str, TrackedTable] = {}

        for table_spec in table_specs:
            try:
                cd_schema, cd_table = self.dialect.parse_qualified_table(table_spec)
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
            cd_schema = self._canonicalize(cd_schema)
            cd_table = self._canonicalize(cd_table)

            normalized_cd_schema = sa_dialect.normalize_name(cd_schema)
            normalized_cd_table = sa_dialect.normalize_name(cd_table)

            with session.connect() as connection:
                registration = connection.execute(
                    text(
                        f"select "
                        f"source_owner, "
                        f"source_table, "
                        f"before_img_prefix "
                        f"from {ASN_SCHEMA}.ibmsnap_register "
                        f"where cd_owner = :cd_owner "
                        f"and cd_table = :cd_table"
                    ),
                    {
                        "cd_owner": normalized_cd_schema.upper(),
                        "cd_table": normalized_cd_table.upper(),
                    },
                ).fetchone()

            if registration is None:
                raise CdcError(
                    ErrorCode.CDC4,
                    normalized_cd_schema,
                    normalized_cd_table,
                    ASN_SCHEMA,
                )

            source_owner = registration[0].strip()
            source_table = registration[1].strip()
            before_img_prefix = registration[2].strip() if registration[2] else None

            normalized_source_schema = sa_dialect.normalize_name(
                quoted_name(source_owner, quote=False)
            )
            normalized_source_table = sa_dialect.normalize_name(
                quoted_name(source_table, quote=False)
            )

            if not inspector.has_table(
                normalized_source_table,
                schema=normalized_source_schema,
            ):
                raise CdcError(
                    ErrorCode.CDC2,
                    normalized_source_schema,
                    normalized_source_table,
                )

            columns_info = inspector.get_columns(
                normalized_source_table,
                schema=normalized_source_schema,
            )
            if not columns_info:
                raise CdcError(
                    ErrorCode.CDC3,
                    normalized_source_schema,
                    normalized_source_table,
                )

            pk_info = inspector.get_pk_constraint(
                normalized_source_table,
                schema=normalized_source_schema,
            )
            pk_columns = pk_info.get("constrained_columns", []) if pk_info else []

            columns = [
                TrackedColumn(
                    name=column_info["name"],
                    satype=column_info["type"],
                    iname=(
                        f"{before_img_prefix}{column_info['name']}"
                        if before_img_prefix
                        else None
                    ),
                )
                for column_info in columns_info
            ]

            data_table = TableObject(
                schema=normalized_source_schema,
                table=normalized_source_table,
            )
            cdc_table = TableObject(
                schema=normalized_cd_schema,
                table=normalized_cd_table,
            )
            tracked_table = TrackedTable(
                data_table=data_table,
                cdc_table=cdc_table,
                columns=columns,
                pk=pk_columns,
                cdc_format=cdc_format,
            )
            result[data_table.qualified] = tracked_table

        return result

    def poll_changes(self, session: Any) -> list[CdcChange]:
        raise NotImplementedError

    def retrieve_position(self) -> Any:
        raise NotImplementedError

    def save_position(self, position: Any) -> None:
        raise NotImplementedError

#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from collections.abc import Generator
from typing import Any

import polars as pl
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.sql.elements import quoted_name

from tabsdata.connector.cdc.common._ingester import (
    CdcIngester,
)
from tabsdata.connector.cdc.common._model import (
    CdcChange,
    CdcDataChange,
    TableObject,
    TrackedColumn,
    TrackedTable,
)
from tabsdata.connector.cdc.common.constants import DOT, CdcOperation
from tabsdata.connector.cdc.common.typing import (
    CdcFormatSpec,
    QualifiedTablesSpec,
    TimestampPositionSpec,
)
from tabsdata.connector.cdc.db2._connector import Db2CdcConn
from tabsdata.connector.cdc.db2.typing import (
    CommitSeqPositionSpec,
    Db2CdcStartFromSpec,
    TableCommitSeqPositionSpec,
)
from tabsdata.connector.sql.common._database.db2.dialect import Db2Dialect
from tabsdata.exceptions import CdcError, ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class IngesterOperations:
    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"


OPERATION_MAP = {
    IngesterOperations.INSERT: CdcOperation.INSERT,
    IngesterOperations.UPDATE: CdcOperation.UPDATE,
    IngesterOperations.DELETE: CdcOperation.DELETE,
}

ASN_SCHEMA = "ASN"

IBMSNAP_OPERATION = "IBMSNAP_OPERATION"
IBMSNAP_COMMITSEQ = "IBMSNAP_COMMITSEQ"
IBMSNAP_INTENTSEQ = "IBMSNAP_INTENTSEQ"
IBMSNAP_LOGMARKER = "IBMSNAP_LOGMARKER"
CD_NEW_SYNCHPOINT = "CD_NEW_SYNCHPOINT"

COMMITSEQ_ORIGIN = b"\x00" * 16

FETCH_SIZE = 10000


class Offset:
    def __init__(self, commits: dict[str, bytes] | None = None):
        self.commits: dict[str, bytes] = dict(commits) if commits else {}

    def copy(self) -> Offset:
        return Offset(commits=self.commits)

    @classmethod
    def load(cls, df: pl.DataFrame | None) -> Offset | None:
        if df is None or df.height != 1:
            return None
        row = df.row(0, named=True)
        return cls(commits={column: row[column] for column in df.columns})


class SqlReplicationIngester(CdcIngester):
    def __init__(
        self,
        conn: Db2CdcConn,
        tables: QualifiedTablesSpec,
        start_from: Db2CdcStartFromSpec,
        offset: pl.DataFrame | None = None,
    ):
        super().__init__(tables)
        self._conn = conn
        self._start_from = start_from
        self._offset: Offset | None = Offset.load(offset)
        self._mark: Offset | None = self._offset.copy() if offset else None

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
        engine = self._conn._connect()
        tz = self.dialect.server_timezone(engine)
        self._server_tz = tz
        logger.info(f"Db2 server timezone: {self._server_tz}")
        self.init_reader(engine)
        return engine

    def close_session(self, session: Any) -> None:
        if session is not None:
            try:
                session.dispose()
            except Exception as exception:
                logger.warning(
                    f"Failed to dispose session: {session} - exception: {exception}"
                )

    def init_reader(self, session: Any) -> None:
        if self._mark is not None:
            return
        commits: dict[str, bytes] = {}
        match self._start_from:
            case CommitSeqPositionSpec(seq=seq):
                for table in self._tracked_tables.values():
                    commits[table.cdc_table.qualified] = bytes.fromhex(seq)
            case TableCommitSeqPositionSpec(seqs=seqs):
                for table, seq in seqs.items():
                    commits[table] = bytes.fromhex(seq)
            case TimestampPositionSpec(ts=ts):
                with session.connect() as connection:
                    row = connection.execute(
                        text(
                            f"select max({IBMSNAP_COMMITSEQ}) "
                            f"from {ASN_SCHEMA}.IBMSNAP_UOW "
                            f"where {IBMSNAP_LOGMARKER} <= :ts"
                        ),
                        {"ts": ts},
                    ).fetchone()
                seq = row[0] if row and row[0] else COMMITSEQ_ORIGIN
                for table in self._tracked_tables.values():
                    commits[table.cdc_table.qualified] = seq
            case "origin":
                for table in self._tracked_tables.values():
                    commits[table.cdc_table.qualified] = COMMITSEQ_ORIGIN
            case _:
                raise CdcError(ErrorCode.CDC6, self._start_from)
        self._mark = Offset(commits=commits)

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

        self._tracked_tables = result
        return result

    def confirm_position(self) -> None:
        pass

    def retrieve_position(self) -> Offset | None:
        return self._mark

    def save_position(self, position: Any) -> None:
        if isinstance(position, Offset):
            self._mark = position

    @staticmethod
    def query_table_watermark(
        connection: Any,
        tracked: TrackedTable,
    ) -> bytes | None:
        row = connection.execute(
            text(
                f"select {CD_NEW_SYNCHPOINT} "
                f"from {ASN_SCHEMA}.ibmsnap_register "
                f"where cd_owner = :cd_owner "
                f"and cd_table = :cd_table"
            ),
            {
                "cd_owner": tracked.cdc_table.schema.upper(),
                "cd_table": tracked.cdc_table.table.upper(),
            },
        ).fetchone()
        return row[0] if row and row[0] else None

    def query_table_changes(
        self,
        connection: Any,
        tracked: TrackedTable,
        commitseq: bytes,
        watermark: bytes,
    ) -> Generator[CdcDataChange, None, None]:
        column_names = [column.name for column in tracked.columns]
        column_inames = [column.iname for column in tracked.columns if column.iname]
        select_columns = (
            [IBMSNAP_OPERATION, IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ]
            + column_names
            + column_inames
        )
        select_clause = ", ".join(select_columns)
        query = text(
            f"select {select_clause} "
            f"from {tracked.cdc_table.schema}{DOT}{tracked.cdc_table.table} "
            f"where {IBMSNAP_COMMITSEQ} > :commitseq "
            f"and {IBMSNAP_COMMITSEQ} <= :watermark "
            f"order by {IBMSNAP_COMMITSEQ}, "
            f"{IBMSNAP_INTENTSEQ}"
        )
        rows = connection.execute(
            query,
            {
                "commitseq": commitseq,
                "watermark": watermark,
            },
        )
        rows = rows.yield_per(FETCH_SIZE)
        for row in rows:
            row_map = row._mapping
            db_op = str(row_map[IBMSNAP_OPERATION]).strip()
            op = OPERATION_MAP.get(db_op)
            if op is None:
                raise CdcError(
                    ErrorCode.CDC5,
                    db_op,
                    "asn",
                    tracked.data_table.qualified,
                )
            commit_seq = row_map[IBMSNAP_COMMITSEQ]
            intent_seq = row_map[IBMSNAP_INTENTSEQ]
            new_values = {col.name: row_map.get(col.name) for col in tracked.columns}
            self.localize_values(new_values)
            if op == CdcOperation.UPDATE:
                old_values = {}
                for column in tracked.columns:
                    if column.iname and column.iname in row_map:
                        old_values[column.name] = row_map[column.iname]
                    else:
                        old_values[column.name] = None
                self.localize_values(old_values)
            else:
                old_values = {column.name: None for column in tracked.columns}
            yield CdcDataChange(
                operation=op,
                transaction=str(commit_seq),
                sequence=int(intent_seq),
                table=tracked.data_table.qualified,
                new_values=new_values,
                old_values=old_values,
            )

    def poll_changes(self, session: Any) -> Generator[CdcChange, None, None]:
        with session.connect() as connection:
            for tracked in self._tracked_tables.values():
                table = tracked.cdc_table.qualified
                commit = self._mark.commits.get(table, COMMITSEQ_ORIGIN)
                watermark = self.query_table_watermark(connection, tracked)
                if watermark is None or watermark <= commit:
                    continue
                yield from self.query_table_changes(
                    connection=connection,
                    tracked=tracked,
                    commitseq=commit,
                    watermark=watermark,
                )
                self._mark.commits[table] = watermark

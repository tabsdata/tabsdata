#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import time
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
from tabsdata.connector.cdc.common.constants import (
    DEFAULT_BLOCKING_TIMEOUT_SECONDS,
    DOT,
    CdcOperation,
)
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


class SystemColumn:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.sa_name: str | None = None

    @property
    def quoted(self) -> str:
        return f'"{self.db_name}"'

    def normalize(self, sa_dialect: Any) -> None:
        self.sa_name = sa_dialect.normalize_name(self.db_name)


IBMSNAP_OPERATION = SystemColumn("IBMSNAP_OPERATION")
IBMSNAP_COMMITSEQ = SystemColumn("IBMSNAP_COMMITSEQ")
IBMSNAP_INTENTSEQ = SystemColumn("IBMSNAP_INTENTSEQ")
IBMSNAP_LOGMARKER = SystemColumn("IBMSNAP_LOGMARKER")
CD_NEW_SYNCHPOINT = SystemColumn("CD_NEW_SYNCHPOINT")

SYSTEM_COLUMNS = [
    IBMSNAP_OPERATION,
    IBMSNAP_COMMITSEQ,
    IBMSNAP_INTENTSEQ,
    IBMSNAP_LOGMARKER,
    CD_NEW_SYNCHPOINT,
]

COMMITSEQ_ORIGIN = b"\x00" * 16

FETCH_SIZE = 10000
POLL_INTERVAL_SECONDS = 1.0


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
        blocking_timeout_seconds: float = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        offset: pl.DataFrame | None = None,
    ):
        super().__init__(tables)
        self._conn = conn
        self._start_from = start_from
        self._offset: Offset | None = Offset.load(offset)
        self._blocking_timeout_seconds = blocking_timeout_seconds
        self._mark: Offset | None = self._offset.copy() if offset else None

    @property
    def conn(self) -> Db2CdcConn:
        return self._conn

    @property
    def start_from(self) -> Db2CdcStartFromSpec | None:
        return self._start_from

    @property
    def blocking_timeout_seconds(self) -> float | None:
        return self._blocking_timeout_seconds

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
                            f"select max({IBMSNAP_COMMITSEQ.quoted}) "
                            f"from {ASN_SCHEMA}.IBMSNAP_UOW "
                            f"where {IBMSNAP_LOGMARKER.quoted} <= :ts"
                        ),
                        {"ts": ts},
                    ).fetchone()
                seq = row[0] if row and row[0] else COMMITSEQ_ORIGIN
                for table in self._tracked_tables.values():
                    commits[table.cdc_table.qualified] = seq
            case "head":
                for table in self._tracked_tables.values():
                    commits[table.cdc_table.qualified] = COMMITSEQ_ORIGIN
            case "tail":
                with session.connect() as connection:
                    watermarks = self.query_table_watermarks(connection)
                    row = connection.execute(
                        text(
                            f"select max({IBMSNAP_COMMITSEQ.quoted}) "
                            f"from {ASN_SCHEMA}.IBMSNAP_UOW"
                        )
                    ).fetchone()
                globalseq = row[0] if row and row[0] else COMMITSEQ_ORIGIN
                for table in self._tracked_tables.values():
                    qualified = table.cdc_table.qualified
                    commits[qualified] = watermarks.get(qualified, globalseq)
            case _:
                raise CdcError(ErrorCode.CDC6, self._start_from)
        self._mark = Offset(commits=commits)

    # noinspection DuplicatedCode
    def collect_tables(
        self,
        session: Any,
        cdc_format: CdcFormatSpec = CdcFormatSpec(),
    ) -> dict[str, TrackedTable]:
        qualified_tables = self._tables
        if isinstance(qualified_tables, str):
            qualified_tables = [qualified_tables]

        inspector: sa.engine.reflection.Inspector = sa.inspect(session)
        sa_dialect: sa.engine.interfaces.Dialect = inspector.bind.dialect
        for column in SYSTEM_COLUMNS:
            column.normalize(sa_dialect)
        result: dict[str, TrackedTable] = {}

        for qualified_table in qualified_tables:
            try:
                cd_schema, cd_table = self.dialect.parse_qualified_table(
                    qualified_table
                )
            except ValueError as error:
                raise CdcError(
                    ErrorCode.CDC1,
                    qualified_table,
                    error,
                ) from error

            real_cd_schema = self.resolve_identifier(cd_schema)
            real_cd_table = self.resolve_identifier(cd_table)

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
                        "cd_owner": real_cd_schema,
                        "cd_table": real_cd_table,
                    },
                ).fetchone()

            if registration is None:
                raise CdcError(
                    ErrorCode.CDC4,
                    real_cd_schema,
                    real_cd_table,
                    ASN_SCHEMA,
                )

            source_owner = registration[0].strip()
            source_table = registration[1].strip()
            before_img_prefix = registration[2].strip() if registration[2] else None

            normalized_source_schema = sa_dialect.normalize_name(
                quoted_name(
                    value=source_owner,
                    quote=False,
                )
            )
            normalized_source_table = sa_dialect.normalize_name(
                quoted_name(
                    value=source_table,
                    quote=False,
                )
            )

            if not inspector.has_table(
                table_name=normalized_source_table,
                schema=normalized_source_schema,
            ):
                raise CdcError(
                    ErrorCode.CDC2,
                    source_owner,
                    source_table,
                )

            columns_info = inspector.get_columns(
                table_name=normalized_source_table,
                schema=normalized_source_schema,
            )
            if not columns_info:
                raise CdcError(
                    ErrorCode.CDC3,
                    source_owner,
                    source_table,
                )

            with session.connect() as connection:
                catalog_columns = self.dialect.get_catalog_columns(
                    connection=connection,
                    schema=source_owner,
                    table=source_table,
                )

            if len(catalog_columns) != len(columns_info):
                raise CdcError(
                    ErrorCode.CDC7,
                    source_owner,
                    source_table,
                    len(catalog_columns),
                    len(columns_info),
                )

            # Verify catalog and inspector columns match (case-insensitive).
            for catalog_name, col_info in zip(catalog_columns, columns_info):
                if catalog_name.upper() != col_info["name"].upper():
                    raise CdcError(
                        ErrorCode.CDC8,
                        source_owner,
                        source_table,
                        catalog_name,
                        col_info["name"],
                    )

            columns = []
            for catalog_name, col_info in zip(catalog_columns, columns_info):
                iname = (
                    f"{before_img_prefix}{catalog_name}" if before_img_prefix else None
                )
                columns.append(
                    TrackedColumn(
                        name=catalog_name,
                        satype=col_info["type"],
                        iname=iname,
                        sa_name=sa_dialect.normalize_name(name=catalog_name),
                        sa_iname=(
                            sa_dialect.normalize_name(name=iname) if iname else None
                        ),
                    )
                )

            pk_info = inspector.get_pk_constraint(
                table_name=normalized_source_table,
                schema=normalized_source_schema,
            )
            inspector_pk = pk_info.get("constrained_columns", []) if pk_info else []
            inspector_to_catalog = {
                col_info["name"]: catalog_name
                for col_info, catalog_name in zip(columns_info, catalog_columns)
            }
            pk_columns = [inspector_to_catalog.get(pk, pk) for pk in inspector_pk]

            data_table = TableObject(
                schema=source_owner,
                table=source_table,
            )
            cdc_table = TableObject(
                schema=real_cd_schema,
                table=real_cd_table,
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

    def query_table_watermarks(
        self,
        connection: Any,
    ) -> dict[str, bytes]:
        watermarks: dict[str, bytes] = {}
        if not self._tracked_tables:
            return watermarks
        blocks = []
        index_to_qualified: dict[int, str] = {}
        for i, table in enumerate(self._tracked_tables.values()):
            cd_schema = table.cdc_table.schema
            cd_table = table.cdc_table.table
            blocks.append(
                f"select {i} as idx, "
                f"max({IBMSNAP_COMMITSEQ.quoted}) as mcs "
                f'from "{cd_schema}"."{cd_table}"'
            )
            index_to_qualified[i] = table.cdc_table.qualified
        query = text(" union all ".join(blocks))
        rows = connection.execute(query).fetchall()
        for row in rows:
            table_index, table_commitseq = row[0], row[1]
            if table_commitseq and table_commitseq > COMMITSEQ_ORIGIN:
                watermarks[index_to_qualified[table_index]] = table_commitseq
        return watermarks

    def query_table_changes(
        self,
        connection: Any,
        tracked: TrackedTable,
        commitseq: bytes,
        watermark: bytes,
    ) -> Generator[CdcDataChange, None, None]:
        column_names = [f'"{column.name}"' for column in tracked.columns]
        column_inames = [
            f'"{column.iname}"' for column in tracked.columns if column.iname
        ]
        select_columns = (
            [
                IBMSNAP_OPERATION.quoted,
                IBMSNAP_COMMITSEQ.quoted,
                IBMSNAP_INTENTSEQ.quoted,
            ]
            + column_names
            + column_inames
        )
        select_clause = ", ".join(select_columns)
        cd_qualified = f"{tracked.cdc_table.schema}{DOT}{tracked.cdc_table.table}"
        query = text(
            f"select {select_clause} "
            f"from {cd_qualified} "
            f"where {IBMSNAP_COMMITSEQ.quoted} > :commitseq "
            f"and {IBMSNAP_COMMITSEQ.quoted} <= :watermark "
            f"order by {IBMSNAP_COMMITSEQ.quoted}, "
            f"{IBMSNAP_INTENTSEQ.quoted}"
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
            db_op = str(row_map[IBMSNAP_OPERATION.sa_name]).strip()
            op = OPERATION_MAP.get(db_op)
            if op is None:
                raise CdcError(
                    ErrorCode.CDC5,
                    db_op,
                    "asn",
                    tracked.data_table.qualified,
                )
            commit_seq = row_map[IBMSNAP_COMMITSEQ.sa_name]
            intent_seq = row_map[IBMSNAP_INTENTSEQ.sa_name]
            new_values = {col.name: row_map.get(col.sa_name) for col in tracked.columns}
            self.localize_values(new_values)
            if op == CdcOperation.UPDATE:
                old_values = {}
                for column in tracked.columns:
                    if column.sa_iname and column.sa_iname in row_map:
                        old_values[column.name] = row_map[column.sa_iname]
                    else:
                        old_values[column.name] = None
                self.localize_values(old_values)
            else:
                old_values = {column.name: None for column in tracked.columns}
            yield CdcDataChange(
                operation=op,
                transaction=str(commit_seq),
                sequence=int.from_bytes(intent_seq, byteorder="big"),
                table=tracked.data_table.qualified,
                new_values=new_values,
                old_values=old_values,
            )

    def poll_changes(self, session: Any) -> Generator[CdcChange, None, None]:
        qualified: str | None = None
        deadline = time.monotonic() + self._blocking_timeout_seconds
        try:
            with session.connect() as connection:
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return
                    watermarks = self.query_table_watermarks(connection)
                    for table in self._tracked_tables.values():
                        qualified = table.cdc_table.qualified
                        commit = self._mark.commits.get(qualified, COMMITSEQ_ORIGIN)
                        watermark = watermarks.get(qualified)
                        if watermark is None or watermark <= commit:
                            continue
                        yield from self.query_table_changes(
                            connection=connection,
                            tracked=table,
                            commitseq=commit,
                            watermark=watermark,
                        )
                        self._mark.commits[qualified] = watermark
                    sleep = min(
                        POLL_INTERVAL_SECONDS,
                        max(deadline - time.monotonic(), 0),
                    )
                    if sleep <= 0:
                        return
                    time.sleep(sleep)
        except Exception as error:
            logger.error(
                f"Error while polling changes: "
                f"exception when polling table={qualified}: "
                f"{type(error).__name__}: {error}",
                exc_info=True,
            )
            raise

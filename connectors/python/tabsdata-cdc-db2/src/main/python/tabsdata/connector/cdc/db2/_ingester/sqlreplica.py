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

from tabsdata.connector.cdc.common._column import TrackedColumn
from tabsdata.connector.cdc.common._ingester import (
    CdcIngester,
)
from tabsdata.connector.cdc.common._model import (
    CdcChange,
    CdcDataChange,
    CdcOffset,
    TableObject,
    TrackedTable,
)
from tabsdata.connector.cdc.common.constants import (
    DEFAULT_BLOCKING_TIMEOUT_SECONDS,
    DOT,
    CdcOperation,
)
from tabsdata.connector.cdc.common.typing import (
    QualifiedTablesSpec,
    TimestampPosition,
)
from tabsdata.connector.cdc.db2._connector import Db2CdcConn
from tabsdata.connector.cdc.db2._matching import match_cd_columns
from tabsdata.connector.cdc.db2.typing import (
    CommitSeqPosition,
    Db2CdcStartFromSpec,
    TableCommitSeqPosition,
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

SYSTEM_COLUMNS = [
    IBMSNAP_OPERATION,
    IBMSNAP_COMMITSEQ,
    IBMSNAP_INTENTSEQ,
    IBMSNAP_LOGMARKER,
]

COMMITSEQ_ORIGIN = b"\x00" * 16

FETCH_SIZE = 10000
POLL_INTERVAL_SECONDS = 1.0


class CommitSeqOffset(CdcOffset):
    def __init__(self, commits: dict[str, bytes] | None = None):
        self.commits: dict[str, bytes] = dict(commits) if commits else {}

    def update(self, other: CdcOffset) -> None:
        if not isinstance(other, CommitSeqOffset):
            raise CdcError(
                ErrorCode.CDC12,
                CommitSeqOffset.__name__,
                type(other).__name__,
            )
        self.commits.update(other.commits)

    def write(self) -> dict:
        return {table: commitseq.hex() for table, commitseq in self.commits.items()}

    @classmethod
    def read(cls, data: dict) -> CommitSeqOffset:
        return cls(
            commits={
                table: bytes.fromhex(commitseq) for table, commitseq in data.items()
            }
        )

    def copy(self) -> CommitSeqOffset:
        return CommitSeqOffset(commits=self.commits)

    def upload(self) -> pl.DataFrame:
        return pl.DataFrame({
            table: [commitseq] for table, commitseq in self.commits.items()
        })

    @classmethod
    def download(cls, df: pl.DataFrame | None) -> CommitSeqOffset | None:
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
        blocking_timeout_seconds: int = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        offset: pl.DataFrame | None = None,
    ):
        super().__init__(tables)
        self._conn = conn
        self._start_from = start_from
        self._blocking_timeout_seconds = blocking_timeout_seconds
        self._offset: CommitSeqOffset | None = CommitSeqOffset.download(offset)
        self._mark: CommitSeqOffset | None = self._offset.copy() if offset else None
        self._base_offset: CommitSeqOffset | None = None
        self._before_img_prefixes: dict[str, str | None] = {}

    @property
    def ingester_type(self) -> str:
        return "sqlreplica"

    @property
    def conn(self) -> Db2CdcConn:
        return self._conn

    @property
    def start_from(self) -> Db2CdcStartFromSpec | None:
        return self._start_from

    @property
    def blocking_timeout_seconds(self) -> int | None:
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
                session = None
            except Exception as exception:
                logger.warning(
                    f"Failed to dispose session: {session} - exception: {exception}"
                )

    def init_reader(self, session: Any) -> None:
        if self._mark is not None:
            self._base_offset = self._mark.copy()
            return
        commits: dict[str, bytes] = {}
        match self._start_from:
            case CommitSeqPosition(seq=seq):
                for table in self._tracked_tables.values():
                    commits[table.cdc_table.qualified] = bytes.fromhex(seq)
            case TableCommitSeqPosition(seqs=seqs):
                for table, seq in seqs.items():
                    commits[table] = bytes.fromhex(seq)
            case TimestampPosition(ts=ts):
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
        self._mark = CommitSeqOffset(commits=commits)
        self._base_offset = self._mark.copy()

    def gather_table_identity(
        self,
        tablespec: str,
        inspector: sa.engine.reflection.Inspector,
    ) -> TrackedTable:
        sa_dialect = inspector.bind.dialect

        try:
            cd_schema, cd_table = self.dialect.parse_qualified_table(tablespec)
        except ValueError as error:
            raise CdcError(
                ErrorCode.CDC1,
                tablespec,
                error,
            ) from error

        real_cd_schema = self.resolve_identifier(cd_schema)
        real_cd_table = self.resolve_identifier(cd_table)

        with inspector.bind.connect() as connection:
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
            quoted_name(value=source_owner, quote=False)
        )
        normalized_source_table = sa_dialect.normalize_name(
            quoted_name(value=source_table, quote=False)
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
        )
        self._before_img_prefixes[data_table.qualified] = before_img_prefix
        return tracked_table

    def gather_table_structure(
        self,
        tracked_table: TrackedTable,
        inspector: sa.engine.reflection.Inspector,
        before_img_prefix: str | None = None,
    ) -> None:
        prefix = (
            before_img_prefix
            or self._before_img_prefixes[tracked_table.data_table.qualified]
        )
        super().gather_table_structure(tracked_table, inspector, prefix)

    def prepare_inspector(self, inspector: sa.engine.reflection.Inspector) -> None:
        sa_dialect = inspector.bind.dialect
        for column in SYSTEM_COLUMNS:
            column.normalize(sa_dialect)

    def extract_raw_schema(
        self,
        session: Any,
        tracked: TrackedTable,
        before_img_prefix: str | None,
    ) -> tuple[tuple[str, str, Any], ...]:
        inspector = sa.inspect(session)
        sa_dialect = inspector.bind.dialect
        normalized_schema = sa_dialect.normalize_name(
            quoted_name(
                tracked.cdc_table.schema,
                quote=False,
            )
        )
        normalized_table = sa_dialect.normalize_name(
            quoted_name(
                tracked.cdc_table.table,
                quote=False,
            )
        )
        sa_columns = inspector.get_columns(
            normalized_table,
            schema=normalized_schema,
        )

        with inspector.bind.connect() as connection:
            db_columns = self.dialect.get_catalog_columns(
                connection,
                tracked.cdc_table.schema,
                tracked.cdc_table.table,
            )

        if len(db_columns) != len(sa_columns):
            raise CdcError(
                ErrorCode.CDC7,
                tracked.cdc_table.schema,
                tracked.cdc_table.table,
                len(db_columns),
                len(sa_columns),
            )

        cd_columns = [
            (
                db_column.strip(),
                sa_column["type"],
            )
            for db_column, sa_column in zip(db_columns, sa_columns)
        ]
        matched = match_cd_columns(cd_columns, before_img_prefix)

        columns = []
        for name, sa_type in matched.after_columns_list:
            oname = matched.before_columns_map.get(name)
            columns.append(
                TrackedColumn(
                    name=name,
                    satype=sa_type,
                    oname=oname,
                    sa_name=sa_dialect.normalize_name(name),
                    sa_oname=(sa_dialect.normalize_name(oname) if oname else None),
                )
            )
        tracked.columns = columns
        tracked.build_pa_dtypes()
        tracked.pk = self.gather_table_pk(
            inspector,
            normalized_table,
            normalized_schema,
            sa_columns,
            db_columns,
        )

        return tuple(
            (name, repr(sa_type), sa_type)
            for name, sa_type in matched.after_columns_list
        )

    def base_offset(self) -> CommitSeqOffset:
        return self._base_offset

    def commit_offset(self, offset: CdcOffset) -> None:
        if not isinstance(offset, CommitSeqOffset):
            raise CdcError(
                ErrorCode.CDC12,
                CommitSeqOffset.__name__,
                type(offset).__name__,
            )

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
        schema_version: Any = None,
    ) -> Generator[CdcDataChange, None, None]:
        column_names = [f'"{column.name}"' for column in tracked.columns]
        column_inames = [
            f'"{column.oname}"' for column in tracked.columns if column.oname
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
                    if column.sa_oname and column.sa_oname in row_map:
                        old_values[column.name] = row_map[column.sa_oname]
                    else:
                        old_values[column.name] = None
                self.localize_values(old_values)
            else:
                old_values = {column.name: None for column in tracked.columns}
            yield CdcDataChange(
                operation=op,
                transaction=commit_seq.hex(),
                sequence=int.from_bytes(intent_seq, byteorder="big"),
                table=tracked.data_table.qualified,
                new_values=new_values,
                old_values=old_values,
                offset=CommitSeqOffset(
                    commits={tracked.cdc_table.qualified: commit_seq}
                ),
                schema=schema_version,
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

                        before_prefix = self._before_img_prefixes[
                            table.data_table.qualified
                        ]
                        raw_schema = self.extract_raw_schema(
                            session, table, before_prefix
                        )
                        schema_version = self._schema_registry.register_from_log(
                            table.data_table.qualified, raw_schema
                        )

                        yield from self.query_table_changes(
                            connection=connection,
                            tracked=table,
                            commitseq=commit,
                            watermark=watermark,
                            schema_version=schema_version,
                        )
                        self._mark.commits[qualified] = watermark
                    sleep = min(
                        POLL_INTERVAL_SECONDS,
                        max(deadline - time.monotonic(), 0.0),
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

#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import time
from collections.abc import Generator
from typing import Any

import polars as pl
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent, HeartbeatLogEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from tabsdata.connector.cdc.common._ingester import (
    CdcIngester,
)
from tabsdata.connector.cdc.common._model import (
    CdcChange,
    CdcDataChange,
    CdcOffset,
)
from tabsdata.connector.cdc.common.constants import (
    DEFAULT_BLOCKING_TIMEOUT_SECONDS,
    DOT,
    CdcOperation,
)
from tabsdata.connector.cdc.common.typing import QualifiedTablesSpec
from tabsdata.connector.cdc.mysql._connection import MySQLCdcConn
from tabsdata.connector.cdc.mysql.typing import (
    BinlogPositionSpec,
    GtidPositionSpec,
    MySQLCdcStartFromSpec,
    TimestampPositionSpec,
)
from tabsdata.connector.sql.common._database.mysql.dialect import MySQLDialect
from tabsdata.exceptions import CdcError, ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class IngesterOperations:
    INSERT = WriteRowsEvent
    UPDATE = UpdateRowsEvent
    DELETE = DeleteRowsEvent


BINLOG_START_POSITION = 4
HEARTBEAT_INTERVAL_SECONDS = 1

OPERATION_MAP = {
    IngesterOperations.INSERT: CdcOperation.INSERT,
    IngesterOperations.UPDATE: CdcOperation.UPDATE,
    IngesterOperations.DELETE: CdcOperation.DELETE,
}


class GtidOffset(CdcOffset):
    def __init__(self, gtid: str):
        self.gtid = gtid

    def update(self, other: CdcOffset) -> None:
        if not isinstance(other, GtidOffset):
            raise CdcError(
                ErrorCode.CDC12,
                GtidOffset.__name__,
                type(other).__name__,
            )
        self.gtid = other.gtid

    def write(self) -> dict:
        return {"gtid": self.gtid}

    @classmethod
    def read(cls, data: dict) -> GtidOffset:
        return cls(gtid=data["gtid"])

    def copy(self) -> GtidOffset:
        return GtidOffset(gtid=self.gtid)

    def upload(self) -> pl.DataFrame:
        return pl.DataFrame({"gtid": [self.gtid]})

    @classmethod
    def download(cls, df: pl.DataFrame | None) -> GtidOffset | None:
        if df is None or df.height != 1:
            return None
        return cls(gtid=df["gtid"][0])


class BinlogIngester(CdcIngester):
    def __init__(
        self,
        conn: MySQLCdcConn,
        tables: QualifiedTablesSpec,
        start_from: MySQLCdcStartFromSpec,
        server_id: int,
        blocking_timeout_seconds: int = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        offset: GtidOffset | None = None,
    ):
        super().__init__(tables)
        self._conn = conn
        self._start_from = start_from
        self._server_id = server_id
        self._blocking_timeout_seconds = blocking_timeout_seconds
        self._offset: GtidOffset | None = GtidOffset.download(offset)
        self._mark: GtidOffset | None = self._offset.copy() if offset else None
        self._base_offset: GtidOffset | None = None
        self._stream: BinLogStreamReader | None = None

    @property
    def conn(self) -> MySQLCdcConn:
        return self._conn

    @property
    def start_from(self) -> MySQLCdcStartFromSpec | None:
        return self._start_from

    @property
    def server_id(self) -> int:
        return self._server_id

    @property
    def blocking_timeout_seconds(self) -> int | None:
        return self._blocking_timeout_seconds

    @property
    def dialect(self) -> MySQLDialect:
        return MySQLDialect()

    def open_session(self) -> Any:
        engine = self._conn._connect()
        tz = self.dialect.server_timezone(engine)
        self._server_tz = tz
        logger.info(f"MySQL server timezone: {self._server_tz}")
        self.init_stream()
        return engine

    def close_session(self, session: Any) -> None:
        if self._stream is not None:
            try:
                self._stream.close()
                self._stream = None
            except Exception as exception:
                logger.warning(
                    f"Failed to close stream: {self._stream} - exception: {exception}"
                )
        if session is not None:
            try:
                session.dispose()
                session = None
            except Exception as exception:
                logger.warning(
                    f"Failed to dispose session: {session} - exception: {exception}"
                )

    def init_stream(self) -> None:
        stream_connection_settings = self.stream_connection_settings()
        stream_schemas, stream_tables = self.schema_table_filter()
        stream_kwargs: dict[str, Any] = {
            "connection_settings": stream_connection_settings,
            "server_id": self._server_id,
            "blocking": True,
            "resume_stream": True,
            "slave_heartbeat": min(
                self._blocking_timeout_seconds, HEARTBEAT_INTERVAL_SECONDS
            ),
            "only_events": [
                WriteRowsEvent,
                UpdateRowsEvent,
                DeleteRowsEvent,
                GtidEvent,
                HeartbeatLogEvent,
            ],
        }
        if stream_schemas:
            stream_kwargs["only_schemas"] = stream_schemas
        if stream_tables:
            stream_kwargs["only_tables"] = stream_tables

        start_from = self._start_from
        if self._mark is not None:
            start_from = GtidPositionSpec(gtid=self._mark.gtid)

        match start_from:
            case GtidPositionSpec(gtid=gtid):
                stream_kwargs["auto_position"] = self.as_gtid_set(gtid)
                if self._mark is None:
                    self._mark = GtidOffset(gtid=gtid)
            case BinlogPositionSpec(file=file, pos=pos):
                stream_kwargs["log_file"] = file
                stream_kwargs["log_pos"] = pos
                if self._mark is None:
                    self._mark = GtidOffset(gtid="")
            case TimestampPositionSpec(ts=ts):
                stream_kwargs["skip_to_timestamp"] = ts
                if self._mark is None:
                    self._mark = GtidOffset(gtid="")
            case "head":
                stream_kwargs["log_file"] = self.first_binlog_file()
                stream_kwargs["log_pos"] = BINLOG_START_POSITION
                self._mark = GtidOffset(gtid="")
            case "tail":
                file, pos = self.last_binlog_position()
                stream_kwargs["log_file"] = file
                stream_kwargs["log_pos"] = pos
                self._mark = GtidOffset(gtid="")
            case _:
                raise CdcError(ErrorCode.CDC6, start_from)
        self._base_offset = self._mark.copy()
        self._stream = BinLogStreamReader(**stream_kwargs)
        logger.info("BinLogStreamReader initialized")

    @staticmethod
    def as_gtid_set(gtid: str) -> str:
        """Convert a single GTID 'uuid:N' to a GTID set range 'uuid:1-N'.

        MySQL auto_position interprets 'uuid:N' as a set containing only
        transaction N. To express 'all transactions up to N', we need the
        range format 'uuid:1-N'.
        """
        if not gtid:
            return gtid
        uuid, seq = gtid.rsplit(":", 1)
        if "-" in seq:
            return gtid
        return f"{uuid}:1-{seq}"

    def first_binlog_file(self) -> str:
        connection_settings = self.stream_connection_settings()
        conn = pymysql.connect(
            host=connection_settings["host"],
            port=connection_settings["port"],
            user=connection_settings.get("user"),
            passwd=connection_settings.get("passwd"),
            db=connection_settings.get("db"),
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute("show binary logs")
                row = cursor.fetchone()
                if not row:
                    raise CdcError(ErrorCode.CDCMYSQL3, "head")
                return row[0]
        finally:
            conn.close()

    def last_binlog_position(self) -> tuple[str, int]:
        connection_settings = self.stream_connection_settings()
        conn = pymysql.connect(
            host=connection_settings["host"],
            port=connection_settings["port"],
            user=connection_settings.get("user"),
            passwd=connection_settings.get("passwd"),
            db=connection_settings.get("db"),
        )
        try:
            with conn.cursor() as cursor:
                row = self.query_binlog_status(cursor)
                if not row:
                    raise CdcError(ErrorCode.CDCMYSQL3, "tail")
                return row[0], row[1]
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def query_binlog_status(cursor: Any) -> Any:
        try:
            cursor.execute("show binary log status")
            return cursor.fetchone()
        except pymysql.err.ProgrammingError:
            cursor.execute("show master status")
            return cursor.fetchone()

    def stream_connection_settings(self) -> dict:
        connection_settings = self._conn._settings()
        settings: dict[str, Any] = {
            "host": connection_settings.host,
            "port": connection_settings.port,
        }
        if connection_settings.user:
            settings["user"] = connection_settings.user
        if connection_settings.password:
            settings["passwd"] = connection_settings.password
        if connection_settings.dbname:
            settings["db"] = connection_settings.dbname
        settings["read_timeout"] = int(self._blocking_timeout_seconds)
        return settings

    def schema_table_filter(self) -> tuple[list[str] | None, list[str] | None]:
        if not self._tracked_tables:
            return None, None
        schemas = set()
        tables = set()
        for table in self._tracked_tables.values():
            schemas.add(table.data_table.schema)
            tables.add(f"{table.data_table.schema}{DOT}{table.data_table.table}")
        return list(schemas), list(tables)

    def base_offset(self) -> GtidOffset:
        return self._base_offset

    def commit_offset(self, offset: CdcOffset) -> None:
        if not isinstance(offset, GtidOffset):
            raise CdcError(
                ErrorCode.CDC12,
                GtidOffset.__name__,
                type(offset).__name__,
            )

    def as_qualified_table(self, schema: str, table: str) -> str | None:
        qualified = f"{schema}{DOT}{table}"
        if qualified in self._tracked_tables:
            return qualified
        return None

    def poll_changes(self, session: Any) -> Generator[CdcChange, None, None]:
        deadline = time.monotonic() + self._blocking_timeout_seconds
        gtid: str | None = None
        seq: int = 0
        try:
            for event in self._stream:
                if isinstance(event, GtidEvent):
                    if gtid is not None:
                        self._mark = GtidOffset(gtid=gtid)
                    if time.monotonic() > deadline:
                        break
                    gtid = event.gtid
                    seq = 0
                elif isinstance(event, HeartbeatLogEvent):
                    if time.monotonic() > deadline:
                        break
                elif isinstance(
                    event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)
                ):
                    qualified = self.as_qualified_table(event.schema, event.table)
                    if qualified is None:
                        continue
                    op = OPERATION_MAP[type(event)]
                    for row in event.rows:
                        seq += 1
                        match event:
                            case WriteRowsEvent():
                                new_values = row["values"]
                                old_values = {column: None for column in new_values}
                            case UpdateRowsEvent():
                                new_values = row["after_values"]
                                old_values = row["before_values"]
                            case DeleteRowsEvent():
                                new_values = row["values"]
                                old_values = {column: None for column in new_values}
                            case _:
                                raise CdcError(
                                    ErrorCode.CDC5,
                                    type(event).__name__,
                                    "binlog",
                                    qualified,
                                )
                        new_values = self.filter_values(qualified, new_values)
                        old_values = self.filter_values(qualified, old_values)
                        self.localize_values(new_values)
                        self.localize_values(old_values)
                        yield CdcDataChange(
                            operation=op,
                            transaction=gtid,
                            sequence=seq,
                            table=qualified,
                            new_values=new_values,
                            old_values=old_values,
                            offset=GtidOffset(gtid=gtid),
                        )
        except Exception as error:
            logger.error(
                f"Error while polling changes: "
                f"exception when polling gtid={gtid} @ seq={seq}: "
                f"{type(error).__name__}: {error}",
                exc_info=True,
            )
            raise

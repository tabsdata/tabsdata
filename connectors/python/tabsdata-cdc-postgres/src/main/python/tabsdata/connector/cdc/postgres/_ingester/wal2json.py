#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import time
from collections.abc import Generator, Iterable
from datetime import date, datetime, time as dt_time, timezone
from decimal import Decimal
from typing import Any

import orjson
import polars as pl
import pyarrow as pa

from tabsdata.connector.cdc.common._dictionary import SchemaVersion
from tabsdata.connector.cdc.common._ingester import CdcIngester
from tabsdata.connector.cdc.common._model import (
    CdcChange,
    CdcDataChange,
    CdcOffset,
)
from tabsdata.connector.cdc.common.constants import (
    COMMA,
    DEFAULT_BLOCKING_TIMEOUT_SECONDS,
    DOT,
    CdcOperation,
)
from tabsdata.connector.cdc.common.typing import QualifiedTablesSpec
from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn
from tabsdata.connector.cdc.postgres.typing import (
    LsnPosition,
    PostgresCdcStartFromSpec,
    ReplicationSlotBehaviorSpec,
)
from tabsdata.connector.sql.common._database.postgres.dialect import PostgresDialect
from tabsdata.exceptions import CdcError, ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class IngesterV1Operations:
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


V1_OPERATION_MAP = {
    IngesterV1Operations.INSERT: CdcOperation.INSERT,
    IngesterV1Operations.UPDATE: CdcOperation.UPDATE,
    IngesterV1Operations.DELETE: CdcOperation.DELETE,
}


class V2Actions:
    BEGIN = "B"
    COMMIT = "C"
    TRUNCATE = "T"
    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"


class IngesterV2Operations:
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


V2_OPERATION_MAP = {
    V2Actions.INSERT: CdcOperation.INSERT,
    V2Actions.UPDATE: CdcOperation.UPDATE,
    V2Actions.DELETE: CdcOperation.DELETE,
}


WAL2JSON_PLUGIN = "wal2json"
DEFAULT_REPLICATION_SLOT_BEHAVIOR = "reuse"
POLL_INTERVAL_SECONDS = 1.0
PENDING_TIMEOUT_MULTIPLIER = 16


class Wal2JsonFormatVersion:
    WAL2JSON_FORMAT_V1: int = 1
    WAL2JSON_FORMAT_V2: int = 2


DEFAULT_WAL2JSON_FORMAT_VERSION: int = Wal2JsonFormatVersion.WAL2JSON_FORMAT_V2


class LsnOffset(CdcOffset):
    def __init__(self, lsn: int):
        self.lsn = lsn

    def update(self, other: CdcOffset) -> None:
        if not isinstance(other, LsnOffset):
            raise CdcError(
                ErrorCode.CDC12,
                LsnOffset.__name__,
                type(other).__name__,
            )
        self.lsn = other.lsn

    def write(self) -> dict:
        return {"lsn": self.lsn}

    @classmethod
    def read(cls, data: dict) -> LsnOffset:
        return cls(lsn=data["lsn"])

    def copy(self) -> LsnOffset:
        return LsnOffset(lsn=self.lsn)

    def upload(self) -> pl.DataFrame:
        return pl.DataFrame({"lsn": [self.lsn]})

    @classmethod
    def download(cls, df: pl.DataFrame | None) -> LsnOffset | None:
        if df is None or df.height != 1:
            return None
        return cls(lsn=df["lsn"][0])


class Wal2JsonIngester(CdcIngester):
    def __init__(
        self,
        conn: PostgresCdcConn,
        tables: QualifiedTablesSpec,
        start_from: PostgresCdcStartFromSpec,
        replication_slot: str,
        replication_slot_behavior: ReplicationSlotBehaviorSpec = (
            DEFAULT_REPLICATION_SLOT_BEHAVIOR
        ),
        blocking_timeout_seconds: int = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        publication_name: str | None = None,
        offset: LsnOffset | None = None,
        wal2json_format_version: int = DEFAULT_WAL2JSON_FORMAT_VERSION,
    ):
        super().__init__(tables)
        self._conn = conn
        self._start_from = start_from
        self._replication_slot = replication_slot
        self._replication_slot_behavior = replication_slot_behavior
        self._blocking_timeout_seconds = blocking_timeout_seconds
        self._publication_name = publication_name
        self._offset: LsnOffset | None = LsnOffset.download(offset)
        self._mark: LsnOffset | None = self._offset.copy() if offset else None
        self._base_offset: LsnOffset | None = None
        self._wal2json_format_version = wal2json_format_version
        self._replicator: Any | None = None
        self._cursor: Any | None = None

    @property
    def ingester_type(self) -> str:
        return "wal2json"

    @property
    def conn(self) -> PostgresCdcConn:
        return self._conn

    @property
    def start_from(self) -> PostgresCdcStartFromSpec | None:
        return self._start_from

    @property
    def replication_slot(self) -> str:
        return self._replication_slot

    @property
    def replication_slot_behavior(self) -> ReplicationSlotBehaviorSpec:
        return self._replication_slot_behavior

    @property
    def blocking_timeout_seconds(self) -> int | None:
        return self._blocking_timeout_seconds

    @property
    def publication_name(self) -> str | None:
        return self._publication_name

    @property
    def dialect(self) -> PostgresDialect:
        return PostgresDialect()

    def open_session(self) -> Any:
        engine = self._conn._connect()
        tz = self.dialect.server_timezone(engine)
        self._server_tz = tz
        logger.info(f"PostgreSQL server timezone: {self._server_tz}")
        self.init_replication()
        return engine

    def close_session(self, session: Any) -> None:
        if self._cursor is not None:
            try:
                self._cursor.close()
                self._cursor = None
            except Exception as exception:
                logger.warning(
                    f"Failed to close cursor: {self._cursor} - exception: {exception}"
                )
        if self._replicator is not None:
            try:
                self._replicator.close()
                self._replicator = None
            except Exception as exception:
                logger.warning(
                    f"Failed to close replicator: "
                    f"{self._replicator} - exception: {exception}"
                )
        if session is not None:
            try:
                session.dispose()
                session = None
            except Exception as exception:
                logger.warning(
                    f"Failed to dispose session: {session}, exception: {exception}"
                )

    def init_replication(self) -> None:
        import psycopg2

        replication_connection_settings = self.replication_connection_settings()
        self._replicator = psycopg2.connect(**replication_connection_settings)
        self._cursor = self._replicator.cursor()
        if self._replication_slot_behavior == "create":
            try:
                self._cursor.create_replication_slot(
                    self._replication_slot,
                    output_plugin=WAL2JSON_PLUGIN,
                )
                logger.info(f"Created replication slot: '{self._replication_slot}'")
            except psycopg2.errors.DuplicateObject:
                logger.info(
                    f"Replication slot '{self._replication_slot}' "
                    f"already exists; reusing it."
                )
        else:
            logger.info(f"Reusing replication slot: '{self._replication_slot}'")
        if self._mark is None:
            match self._start_from:
                case LsnPosition(lsn=lsn):
                    self._mark = LsnOffset(lsn=lsn)
                case "head":
                    self._mark = LsnOffset(lsn=0)
                case "tail":
                    self._mark = LsnOffset(lsn=self.current_wal_lsn())
                case _:
                    raise CdcError(ErrorCode.CDC6, self._start_from)
        options = {
            "format-version": str(self._wal2json_format_version),
            "include-transaction": "true",
            "include-xids": "true",
            "include-timestamp": "true",
            "add-tables": self.schema_table_filter(),
        }
        if self._publication_name:
            options["publication_names"] = self._publication_name
        self._base_offset = self._mark.copy()
        self._cursor.start_replication(
            slot_name=self._replication_slot,
            decode=True,
            start_lsn=self._mark.lsn,
            options=options,
        )
        logger.info("Replication slot Replicator and Cursor initialized")

    def current_wal_lsn(self) -> int:
        import psycopg2

        connection_settings = self.replication_connection_settings()
        conn = psycopg2.connect(**connection_settings)
        try:
            with conn.cursor() as cursor:
                cursor.execute("select pg_current_wal_lsn() - '0/0'::pg_lsn")
                row = cursor.fetchone()
                if not row or row[0] is None:
                    raise CdcError(ErrorCode.CDCPOSTGRES4, "tail")
                return int(row[0])
        finally:
            conn.close()

    def replication_connection_settings(self) -> dict:
        from psycopg2.extras import LogicalReplicationConnection

        connection_settings = self._conn._settings()
        settings: dict[str, Any] = {
            "host": connection_settings.host,
            "port": connection_settings.port,
            "dbname": connection_settings.dbname,
            "connection_factory": LogicalReplicationConnection,
        }
        if connection_settings.user:
            settings["user"] = connection_settings.user
        if connection_settings.password:
            settings["password"] = connection_settings.password
        return settings

    def schema_table_filter(self) -> str:
        if not self._tracked_tables:
            return "*.*"
        tables = []
        for table in self._tracked_tables.values():
            tables.append(f"{table.data_table.schema}{DOT}{table.data_table.table}")
        return COMMA.join(tables)

    def base_offset(self) -> LsnOffset:
        return self._base_offset

    def commit_offset(self, offset: CdcOffset) -> None:
        if not isinstance(offset, LsnOffset):
            raise CdcError(
                ErrorCode.CDC12,
                LsnOffset.__name__,
                type(offset).__name__,
            )
        if self._cursor is None:
            raise CdcError(ErrorCode.CDCPOSTGRES5)
        if self._mark is not None:
            offset.lsn = self._mark.lsn
        self._cursor.send_feedback(
            flush_lsn=offset.lsn,
            reply=True,
        )
        logger.debug(f"Confirmed LSN position: {offset.lsn}")

    def as_qualified_table(self, schema: str, table: str) -> str | None:
        qualified = f"{schema}{DOT}{table}"
        if qualified in self._tracked_tables:
            return qualified
        return None

    def poll_changes(self, session: Any) -> Generator[CdcChange, None, None]:
        match self._wal2json_format_version:
            case Wal2JsonFormatVersion.WAL2JSON_FORMAT_V1:
                yield from self.poll_changes_v1(session)
            case Wal2JsonFormatVersion.WAL2JSON_FORMAT_V2:
                yield from self.poll_changes_v2(session)
            case _:
                raise CdcError(
                    ErrorCode.CDCPOSTGRES3,
                    self._wal2json_format_version,
                )

    def read_next_message(
        self,
        pending: bool,
        timeout: float,
        deadline: float,
    ) -> Any | None:
        effective_deadline = timeout if pending else deadline
        while True:
            remaining = effective_deadline - time.monotonic()
            if remaining <= 0:
                return None
            message = self._cursor.read_message()
            if message is not None:
                return message
            sleep = min(POLL_INTERVAL_SECONDS, max(remaining, 0.0))
            if sleep <= 0:
                return None
            time.sleep(sleep)

    def resolve_table(
        self,
        change: dict,
    ) -> str | None:
        schema = change.get("schema")
        table = change.get("table")
        if schema is None or table is None:
            return None
        qualified = self.as_qualified_table(schema, table)
        if qualified is None:
            logger.debug(f"Skipping untracked table {schema}{DOT}{table}")
        return qualified

    def build_change(
        self,
        qualified: str,
        op: str,
        xid: str,
        sequence: int,
        timestamp: datetime,
        schema_version: SchemaVersion,
        new_values: dict,
        old_values: dict,
        offset: LsnOffset | None = None,
    ) -> CdcDataChange:
        if new_values:
            new_values = {column: value for column, value in new_values.items()}
        if old_values:
            old_values = {column: value for column, value in old_values.items()}
        columns = schema_version.columns
        null_row = {column.name: None for column in columns}
        if not new_values:
            new_values = null_row.copy()
        if not old_values:
            old_values = null_row.copy()
        self.coerce_values(new_values, columns)
        self.coerce_values(old_values, columns)
        self.localize_values(new_values)
        self.localize_values(old_values)
        return CdcDataChange(
            operation=op,
            transaction=xid,
            sequence=sequence,
            timestamp=timestamp,
            table=qualified,
            new_values=new_values,
            old_values=old_values,
            offset=offset,
            segment=offset.lsn if offset is not None else 0,
            ordinal=sequence,
            schema=schema_version,
        )

    @staticmethod
    def extract_raw_schema_v1(change: dict) -> tuple[tuple[str, str, str], ...]:
        names = change.get("columnnames")
        types = change.get("columntypes")
        if not names or not types:
            old_keys = change.get("oldkeys", {})
            names = old_keys.get("keynames")
            types = old_keys.get("keytypes")
        if not names or not types:
            raise CdcError(ErrorCode.CDCPOSTGRES9, change)
        return tuple((name, dtype, dtype) for name, dtype in zip(names, types))

    @staticmethod
    def extract_raw_schema_v2(payload: dict) -> tuple[tuple[str, str, str], ...]:
        columns = payload.get("columns") or payload.get("identity")
        if not columns:
            raise CdcError(ErrorCode.CDCPOSTGRES9, payload)
        return tuple(
            (column["name"], column["type"], column["type"]) for column in columns
        )

    @staticmethod
    def coerce_values(values: dict, columns: Iterable) -> None:
        column_types = {column.name: column.patype for column in columns}
        for column, value in values.items():
            pa_type = column_types.get(column)
            if pa_type is None:
                raise CdcError(ErrorCode.CDCPOSTGRES8, column)
            values[column] = Wal2JsonIngester.coerce_value(value, pa_type)

    @staticmethod
    def coerce_value(value: Any, pa_type: pa.DataType) -> Any:
        if value is None:
            return None
        if isinstance(pa_type, pa.Decimal128Type):
            if not isinstance(value, Decimal):
                return Decimal(str(value))
        elif pa.types.is_integer(pa_type):
            if not isinstance(value, int) or isinstance(value, bool):
                return int(value)
        elif pa.types.is_floating(pa_type):
            if not isinstance(value, float):
                return float(value)
        elif pa.types.is_boolean(pa_type):
            if not isinstance(value, bool):
                if isinstance(value, str):
                    return value.lower() in ("t", "true", "1", "yes")
                return bool(value)
        elif pa.types.is_date(pa_type):
            if isinstance(value, str):
                return date.fromisoformat(value)
        elif pa.types.is_timestamp(pa_type):
            if isinstance(value, str):
                return datetime.fromisoformat(value)
        elif pa.types.is_time(pa_type):
            if isinstance(value, str):
                return dt_time.fromisoformat(value)
        elif pa.types.is_binary(pa_type):
            if isinstance(value, str):
                return bytes.fromhex(value.removeprefix("\\x"))
        return value

    def poll_changes_v1(self, _session: Any) -> Generator[CdcChange, None, None]:
        deadline = time.monotonic() + self._blocking_timeout_seconds
        xid: str | None = None
        sequence: int = 0
        try:
            while True:
                message = self.read_next_message(
                    pending=False,
                    timeout=deadline,
                    deadline=deadline,
                )
                if message is None:
                    break
                payload = orjson.loads(message.payload)
                xid = str(payload["xid"])
                timestamp = datetime.fromisoformat(payload["timestamp"]).astimezone(
                    timezone.utc
                )
                changes = payload["change"]
                lsn_offset = LsnOffset(lsn=message.data_start)
                sequence = 0
                for change in changes:
                    sequence += 1
                    data_change = self.process_change_v1(
                        change=change,
                        xid=xid,
                        sequence=sequence,
                        timestamp=timestamp,
                        offset=lsn_offset,
                    )
                    if data_change is not None:
                        yield data_change
                self._mark = lsn_offset.copy()
        except Exception as error:
            logger.error(
                f"Error while polling changes: "
                f"exception when polling xid={xid} @ seq={sequence}: "
                f"{type(error).__name__}: {error}",
                exc_info=True,
            )
            raise

    def process_change_v1(
        self,
        change: dict,
        xid: str,
        sequence: int,
        timestamp: datetime,
        offset: LsnOffset | None = None,
    ) -> CdcDataChange | None:
        qualified = self.resolve_table(change)
        if qualified is None:
            return None
        raw_schema = self.extract_raw_schema_v1(change)
        schema_version = self._schema_registry.register_from_log(qualified, raw_schema)
        kind = change.get("kind")
        op = V1_OPERATION_MAP.get(kind)
        if op is None:
            raise CdcError(ErrorCode.CDC5, kind, "wal2json", qualified)
        match op:
            case CdcOperation.INSERT:
                new_values = self.extract_new_values_v1(change)
                old_values = {column: None for column in new_values}
            case CdcOperation.UPDATE:
                new_values = self.extract_new_values_v1(change)
                old_values = self.extract_old_values_v1(change)
            case CdcOperation.DELETE:
                old_values_raw = self.extract_old_values_v1(change)
                new_values_raw = self.extract_new_values_v1(change)
                new_values = old_values_raw or new_values_raw or {}
                old_values = {column: None for column in new_values}
            case _:
                raise CdcError(ErrorCode.CDC5, kind, "wal2json", qualified)
        return self.build_change(
            qualified=qualified,
            op=op,
            xid=xid,
            sequence=sequence,
            timestamp=timestamp,
            schema_version=schema_version,
            new_values=new_values,
            old_values=old_values,
            offset=offset,
        )

    @staticmethod
    def extract_new_values_v1(change: dict) -> dict:
        columns = change.get("columnnames")
        values = change.get("columnvalues")
        if columns and values:
            return dict(zip(columns, values))
        return {}

    @staticmethod
    def extract_old_values_v1(change: dict) -> dict:
        old_values = change.get("oldkeys")
        if not old_values:
            return {}
        columns = old_values.get("keynames")
        values = old_values.get("keyvalues")
        if columns and values:
            return dict(zip(columns, values))
        return {}

    def poll_changes_v2(self, _session: Any) -> Generator[CdcChange, None, None]:
        deadline = time.monotonic() + self._blocking_timeout_seconds
        timeout = time.monotonic() + (
            self._blocking_timeout_seconds * PENDING_TIMEOUT_MULTIPLIER
        )
        xid: str | None = None
        timestamp: datetime | None = None
        sequence: int = 0
        try:
            while True:
                pending = xid is not None
                message = self.read_next_message(
                    pending=pending,
                    timeout=timeout,
                    deadline=deadline,
                )
                if message is None:
                    if pending:
                        raise CdcError(ErrorCode.CDCPOSTGRES6, xid)
                    break
                payload = orjson.loads(message.payload)
                action = payload.get("action")
                if action == V2Actions.BEGIN:
                    xid = str(payload["xid"])
                    timestamp = datetime.fromisoformat(payload["timestamp"]).astimezone(
                        timezone.utc
                    )
                    sequence = 0
                    continue
                if action == V2Actions.COMMIT:
                    self._mark = LsnOffset(lsn=message.wal_end)
                    xid = None
                    continue
                if action == V2Actions.TRUNCATE:
                    continue
                lsn_offset = LsnOffset(lsn=message.data_start)
                sequence += 1
                data_change = self.process_change_v2(
                    payload=payload,
                    xid=xid,
                    sequence=sequence,
                    timestamp=timestamp,
                    offset=lsn_offset,
                )
                if data_change is not None:
                    yield data_change
        except Exception as error:
            logger.error(
                f"Error while polling changes: "
                f"exception when polling xid={xid} @ seq={sequence}: "
                f"{type(error).__name__}: {error}",
                exc_info=True,
            )
            raise

    def process_change_v2(
        self,
        payload: dict,
        xid: str,
        sequence: int,
        timestamp: datetime,
        offset: LsnOffset | None = None,
    ) -> CdcDataChange | None:
        qualified = self.resolve_table(payload)
        if qualified is None:
            return None
        raw_schema = self.extract_raw_schema_v2(payload)
        schema_version = self._schema_registry.register_from_log(qualified, raw_schema)
        action = payload.get("action")
        op = V2_OPERATION_MAP.get(action)
        if op is None:
            raise CdcError(ErrorCode.CDC5, action, "wal2json", qualified)
        match op:
            case CdcOperation.INSERT:
                new_values = self.extract_new_values_v2(payload)
                old_values = {column: None for column in new_values}
            case CdcOperation.UPDATE:
                new_values = self.extract_new_values_v2(payload)
                old_values = self.extract_old_values_v2(payload)
            case CdcOperation.DELETE:
                old_values_raw = self.extract_old_values_v2(payload)
                new_values_raw = self.extract_new_values_v2(payload)
                new_values = old_values_raw or new_values_raw or {}
                old_values = {column: None for column in new_values}
            case _:
                raise CdcError(ErrorCode.CDC5, action, "wal2json", qualified)
        return self.build_change(
            qualified=qualified,
            op=op,
            xid=xid,
            sequence=sequence,
            timestamp=timestamp,
            schema_version=schema_version,
            new_values=new_values,
            old_values=old_values,
            offset=offset,
        )

    @staticmethod
    def extract_new_values_v2(change: dict) -> dict:
        columns = change.get("columns")
        if not columns:
            return {}
        return {column["name"]: column["value"] for column in columns}

    @staticmethod
    def extract_old_values_v2(change: dict) -> dict:
        identity = change.get("identity")
        if not identity:
            return {}
        return {column["name"]: column["value"] for column in identity}

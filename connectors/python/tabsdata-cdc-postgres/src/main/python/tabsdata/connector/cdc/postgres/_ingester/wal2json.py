#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import time
from collections.abc import Generator
from typing import Any

import orjson
import polars as pl
import psycopg2
from psycopg2.extras import LogicalReplicationConnection

from tabsdata.connector.cdc.common._ingester import CdcIngester
from tabsdata.connector.cdc.common._model import (
    CdcChange,
    CdcDataChange,
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
    LsnPositionSpec,
    PostgresCdcStartFromSpec,
    ReplicationSlotBehaviorSpec,
)
from tabsdata.connector.sql.common._database.postgres.dialect import PostgresDialect
from tabsdata.exceptions import CdcError, ErrorCode

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class IngesterOperations:
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


OPERATION_MAP = {
    IngesterOperations.INSERT: CdcOperation.INSERT,
    IngesterOperations.UPDATE: CdcOperation.UPDATE,
    IngesterOperations.DELETE: CdcOperation.DELETE,
}

WAL2JSON_PLUGIN = "wal2json"
DEFAULT_REPLICATION_SLOT_BEHAVIOR = "reuse"


class Offset:
    def __init__(self, lsn: int):
        self.lsn = lsn

    def copy(self) -> Offset:
        return Offset(lsn=self.lsn)

    @classmethod
    def load(cls, df: pl.DataFrame | None) -> Offset | None:
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
        blocking_timeout_seconds: float = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        publication_name: str | None = None,
        offset: Offset | None = None,
    ):
        super().__init__(tables)
        self._conn = conn
        self._start_from = start_from
        self._replication_slot = replication_slot
        self._replication_slot_behavior = replication_slot_behavior
        self._blocking_timeout_seconds = blocking_timeout_seconds
        self._publication_name = publication_name
        self._offset: Offset | None = Offset.load(offset)
        self._mark: Offset | None = self._offset.copy() if offset else None
        self._replicator: Any | None = None
        self._cursor: Any | None = None

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
    def blocking_timeout_seconds(self) -> float | None:
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
            except Exception as exception:
                logger.warning(
                    f"Failed to dispose session: {session}, exception: {exception}"
                )

    def init_replication(self) -> None:
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
                case LsnPositionSpec(lsn=lsn):
                    self._mark = Offset(lsn=lsn)
                case "origin":
                    self._mark = Offset(lsn=0)
                case _:
                    raise CdcError(ErrorCode.CDC6, self._start_from)
        options = {
            "format-version": "2",
            "include-transaction": "true",
            "include-timestamp": "true",
            "add-tables": self.schema_table_filter(),
        }
        if self._publication_name:
            options["publication_names"] = self._publication_name
        self._cursor.start_replication(
            slot_name=self._replication_slot,
            decode=True,
            start_lsn=self._mark.lsn,
            options=options,
        )
        logger.info("Replication slot Replicator and Cursor initialized")

    def replication_connection_settings(self) -> dict:
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

    def confirm_position(self) -> None:
        if self._cursor is not None and self._mark is not None:
            self._cursor.send_feedback(
                flush_lsn=self._mark.lsn,
                reply=True,
            )
            logger.debug("Confirmed LSN position: %s", self._mark.lsn)

    def retrieve_position(self) -> Offset | None:
        return self._mark

    def save_position(self, position: Any) -> None:
        if isinstance(position, Offset):
            self._mark = position

    def as_qualified_table(self, schema: str, table: str) -> str | None:
        qualified = f"{schema}.{table}"
        if qualified in self._tracked_tables:
            return qualified
        return None

    @staticmethod
    def extract_new_values(change: dict) -> dict:
        columns = change.get("columnnames", [])
        values = change.get("columnvalues", [])
        return dict(zip(columns, values))

    @staticmethod
    def extract_old_values(change: dict) -> dict | None:
        old_values = change.get("oldkeys", {})
        columns = old_values.get("keynames", [])
        values = old_values.get("keyvalues", [])
        if columns and values:
            return dict(zip(columns, values))
        return None

    def poll_changes(self, session: Any) -> Generator[CdcChange, None, None]:
        deadline = time.monotonic() + self._blocking_timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            tx = self._cursor.read_message(timeout=remaining)
            if tx is None:
                break
            payload = orjson.loads(tx.payload)
            xid = str(payload["xid"])
            changes = payload["change"]
            sequence = 0
            for change in changes:
                kind = change["kind"]
                schema = change["schema"]
                table = change["table"]
                qualified = self.as_qualified_table(schema, table)
                if qualified is None:
                    continue
                op = OPERATION_MAP.get(kind)
                if op is None:
                    raise CdcError(
                        ErrorCode.CDC5,
                        kind,
                        "wal2json",
                        qualified,
                    )
                sequence += 1
                match kind:
                    case IngesterOperations.INSERT:
                        new_values = self.extract_new_values(change)
                        old_values = {column: None for column in new_values}
                    case IngesterOperations.UPDATE:
                        new_values = self.extract_new_values(change)
                        old_values = self.extract_old_values(change)
                    case IngesterOperations.DELETE:
                        new_values = self.extract_new_values(change)
                        old_values = {column: None for column in new_values}
                    case _:
                        raise CdcError(
                            ErrorCode.CDC5,
                            kind,
                            "wal2json",
                            qualified,
                        )
                self.localize_values(new_values)
                self.localize_values(old_values)
                yield CdcDataChange(
                    operation=op,
                    transaction=xid,
                    sequence=sequence,
                    table=qualified,
                    new_values=new_values,
                    old_values=old_values,
                )
            self._mark = Offset(lsn=tx.wal_end)

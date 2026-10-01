#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import sqlalchemy as sa

from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion
from tabsdatak.conn.common.sql.cdc._ingester import CdcIngester
from tabsdatak.conn.common.sql.cdc._large import LargeColumnPolicy
from tabsdatak.conn.common.sql.cdc._model import (
    CdcOffset,
    InitialLoadSession,
    TrackedTable,
)
from tabsdatak.conn.common.sql.cdc._sentinel.disk import DiskSentinel
from tabsdatak.conn.common.sql.cdc._shaper import CdcRowShaper
from tabsdatak.conn.common.sql.cdc.constants import (
    CDC_INITIAL_LOAD_TX,
    OP_INSERT,
    OP_LOAD,
)
from tabsdatak.conn.common.sql.cdc.typing import CdcFormat

logger = logging.getLogger(__name__)

# A position is never negative, so this segment cannot collide with one:
# it both marks a row as coming from the initial load and sorts it before
# every captured change on the ``(seg, ord)`` cursor the stage publisher
# filters on.
INITIAL_LOAD_SEGMENT = -1

# The LOAD control row opens each table, so the first snapshot row is 1.
INITIAL_LOAD_HEADER_ORDINAL = 0


class InitialLoader:
    """Reads a consistent snapshot of the tracked tables into the disk
    sentinel, as CDC rows: one ``LOAD`` control row per table followed by
    its rows marked as inserts.

    The rows come from the tables themselves, not from the change log: a
    log does not retain enough history to reconstruct the present. The log
    takes over at the session's offset, which is the position the snapshot
    is consistent at.
    """

    def __init__(
        self,
        *,
        ingester: CdcIngester,
        disk_sentinel: DiskSentinel,
        cdc_format: CdcFormat,
        large_column_policy: LargeColumnPolicy,
        fetch_size: int,
        chunk_size: int,
    ):
        self._ingester = ingester
        self._disk_sentinel = disk_sentinel
        self._cdc_format = cdc_format
        self._large_column_policy = large_column_policy
        self._fetch_size = fetch_size
        self._chunk_size = chunk_size

    def load(self, session: InitialLoadSession) -> CdcOffset:
        """Read every tracked table through ``session`` and return the
        offset change capture resumes from. The caller stages the
        accumulated files; nothing is published from here, so a failure
        mid-load leaves no manifest behind."""
        # Read after the scan, per the session contract: the position is
        # valid once every table has been yielded. Sessions fix it when
        # they pin, which for some is at open and for others on the first
        # yield, and reading it earlier sees whatever preceded the pin.
        tracked_tables = self._ingester.tracked_tables
        timestamp = datetime.now(tz=timezone.utc)
        for tracked_table, connection in session.tables(tracked_tables):
            self._load_table(tracked_table, connection, timestamp)
        return session.offset

    def _load_table(
        self,
        tracked_table: TrackedTable,
        connection: Any,
        timestamp: datetime,
    ) -> None:
        qualified = tracked_table.qualified
        schema = self._ingester.schema_registry.version(qualified, 0)
        shaper = CdcRowShaper(schema, self._cdc_format)

        self._disk_sentinel.process_chunk(
            qualified,
            schema.hash,
            shaper.table(
                operation=OP_LOAD.value,
                transaction=CDC_INITIAL_LOAD_TX,
                sequences=[INITIAL_LOAD_HEADER_ORDINAL],
                timestamp=timestamp,
                segment=INITIAL_LOAD_SEGMENT,
                ordinals=[INITIAL_LOAD_HEADER_ORDINAL],
                columns={},
            ),
        )

        ordinal = INITIAL_LOAD_HEADER_ORDINAL
        rows = 0
        for chunk in self._read_chunks(tracked_table, connection, schema):
            ordinals = list(range(ordinal + 1, ordinal + 1 + chunk.num_rows))
            ordinal = ordinals[-1]
            rows += chunk.num_rows
            self._disk_sentinel.process_chunk(
                qualified,
                schema.hash,
                shaper.table(
                    operation=OP_INSERT.value,
                    transaction=CDC_INITIAL_LOAD_TX,
                    sequences=ordinals,
                    timestamp=timestamp,
                    segment=INITIAL_LOAD_SEGMENT,
                    ordinals=ordinals,
                    columns={
                        name: chunk.column(name).combine_chunks()
                        for name in chunk.schema.names
                    },
                ),
            )
        self._disk_sentinel.flush_chunks(qualified)
        logger.info("Initial load read %d row(s) from %s", rows, qualified)

    @staticmethod
    def _statement(tracked_table: TrackedTable, schema: SchemaVersion):
        table = sa.table(
            tracked_table.data_table.table,
            *[sa.column(column.name) for column in schema.columns],
            schema=tracked_table.data_table.schema,
        )
        return sa.select(*table.columns)

    def _read_chunks(
        self,
        tracked_table: TrackedTable,
        connection: Any,
        schema: SchemaVersion,
    ):
        arrow_schema = pa.schema([
            pa.field(column.name, column.patype) for column in schema.columns
        ])
        result = connection.execution_options(
            stream_results=True, yield_per=self._fetch_size
        ).execute(self._statement(tracked_table, schema))
        pending: list[Any] = []
        for partition in result.partitions(self._fetch_size):
            pending.extend(partition)
            while len(pending) >= self._chunk_size:
                chunk, pending = (
                    pending[: self._chunk_size],
                    pending[self._chunk_size :],
                )
                yield self._to_arrow(chunk, schema, arrow_schema, tracked_table)
        if pending:
            yield self._to_arrow(pending, schema, arrow_schema, tracked_table)

    def _to_arrow(
        self,
        rows: list[Any],
        schema: SchemaVersion,
        arrow_schema: pa.Schema,
        tracked_table: TrackedTable,
    ) -> pa.Table:
        columns = {
            column.name: [row[index] for row in rows]
            for index, column in enumerate(schema.columns)
        }
        self._large_column_policy.apply_to_columns(
            table=tracked_table.qualified,
            schema=schema,
            columns=columns,
        )
        return pa.table(
            {
                column.name: pa.array(columns[column.name], type=column.patype)
                for column in schema.columns
            },
            schema=arrow_schema,
        )

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import replace

import polars as pl

from tabsdatak.common._actions.dataquality.record import Check
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputEntry,
    OutputManifest,
    TableStats,
)
from tabsdatak.server._tdrunner.function._handlers.columns import ColumnPolicy
from tabsdatak.server._tdrunner.function._handlers.state import StateOutput
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.tableframe import TableFrame

TABLES_SEPARATOR = ", "

Output = TableFrame | None


class OutputError(Exception):
    pass


class SinkError(OutputError):
    table: str
    path: str
    detail: str

    def __init__(self, table: str, path: str, detail: str) -> None:
        super().__init__(f"Cannot write {table!r} to {path}: {detail}")
        self.table = table
        self.path = path
        self.detail = detail


class StatsError(OutputError):
    table: str
    path: str
    detail: str

    def __init__(self, table: str, path: str, detail: str) -> None:
        super().__init__(f"Cannot read the stats of {table!r} at {path}: {detail}")
        self.table = table
        self.path = path
        self.detail = detail


class Outputs:
    policy: ColumnPolicy

    def __init__(self, policy: ColumnPolicy) -> None:
        self.policy = policy

    def sink(
        self,
        results: list[Output],
        tables: list[str],
        targets: list[TableFile],
        *,
        checks: list[Check] | None = None,
        state: StateOutput | None = None,
    ) -> OutputManifest:
        plans: list[pl.LazyFrame] = []
        written: list[tuple[str, TableFile]] = []
        columnless: set[str] = set()
        for table, value, target in zip(tables, results, targets):
            if value is None:
                continue
            if not value._user_columns():
                columnless.add(table)
            try:
                plans.append(self.policy.sink(value, target))
            except Exception as error:
                raise SinkError(table, target.uri, str(error)) from error
            written.append((table, target))
        if state is not None:
            try:
                plans.append(state.source.copy(state.target))
            except Exception as error:
                raise SinkError(state.name, state.target.uri, str(error)) from error
            written.append((state.name, state.target))
        pending = checks or []
        if plans or pending:
            try:
                collected = pl.collect_all(
                    [*plans, *(check.verdict for check in pending)],
                    engine="streaming",
                )
            except Exception as error:
                raise SinkError(
                    TABLES_SEPARATOR.join(table for table, _ in written),
                    TABLES_SEPARATOR.join(target.uri for _, target in written),
                    str(error),
                ) from error
            for check, verdict in zip(pending, collected[len(plans) :]):
                check.enforce(verdict)
        entries = {
            table: OutputEntry(
                name=table,
                uri=target.uri,
                stats=self.stats(table, target),
            )
            for table, target in written
        }
        for table in columnless:
            entries[table] = self.unknown(entries[table])
        return OutputManifest(
            outputs=tuple(
                entries.get(table, OutputEntry(name=table)) for table in tables
            ),
            state=None if state is None else entries[state.name],
        )

    @staticmethod
    def unknown(entry: OutputEntry) -> OutputEntry:
        if entry.stats is None or entry.stats.row_count > 0:
            return entry
        return replace(entry, stats=replace(entry.stats, schema_hash=None))

    def stats(self, table: str, target: TableFile) -> TableStats:
        try:
            return target.stats()
        except Exception as error:
            raise StatsError(table, target.uri, str(error)) from error

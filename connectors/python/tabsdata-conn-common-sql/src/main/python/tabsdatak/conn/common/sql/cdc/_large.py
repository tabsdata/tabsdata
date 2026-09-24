#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Iterable, Iterator

from tabsdatak.conn.common.sql.cdc._dictionary import SchemaVersion
from tabsdatak.conn.common.sql.cdc._model import CdcChange, CdcDataChange
from tabsdatak.conn.common.sql.cdc.error import SqlCdcErrorCode


class LargeColumnPolicy:
    def __init__(self, max_text_length: int):
        self._max_text_length = max_text_length

    def apply(self, changes: Iterable[CdcChange]) -> Iterator[CdcChange]:
        for change in changes:
            if isinstance(change, CdcDataChange):
                self.apply_to(change)
            yield change

    def apply_to(self, change: CdcDataChange) -> None:
        for column in change.schema.large_columns:
            for values in (change.new_values, change.old_values):
                if not values or column.name not in values:
                    continue
                match column.kind:
                    case "object":
                        values[column.name] = None
                    case "text":
                        self.check_text_length(
                            change.table, column.name, values[column.name]
                        )
                    case _:
                        raise ValueError(
                            f"Unsupported large column kind: {column.kind!r}"
                        )

    def apply_to_columns(
        self,
        *,
        table: str,
        schema: SchemaVersion,
        columns: dict[str, list],
    ) -> None:
        for column in schema.large_columns:
            values = columns.get(column.name)
            if values is None:
                continue
            match column.kind:
                case "object":
                    columns[column.name] = [None] * len(values)
                case "text":
                    for value in values:
                        self.check_text_length(table, column.name, value)
                case _:
                    raise ValueError(f"Unsupported large column kind: {column.kind!r}")

    def check_text_length(self, table: str, column: str, value: object) -> None:
        if not isinstance(value, (str, bytes, bytearray)):
            return
        if len(value) > self._max_text_length:
            raise SqlCdcErrorCode.CDC_20.exception(
                column=column,
                table=table,
                length=len(value),
                limit=self._max_text_length,
            )

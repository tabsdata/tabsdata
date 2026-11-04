#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    InputTable,
    InputTableVersions,
    Location,
    Request,
)
from tabsdatak.server._tdrunner.function._commons.manifests.manifest import Manifest

SINGLE_VERSION_POS = -1


@dataclass(frozen=True)
class InputRecord:
    location: Location | None = None
    metadata_location: Location | None = None
    input_idx: int | None = None
    version_pos: int = SINGLE_VERSION_POS
    execution_id: str | None = None
    transaction_id: str | None = None
    physical_table_data_version_id: str | None = None
    triggered_on: int | None = None

    @classmethod
    def of(cls, table: InputTable) -> InputRecord:
        return cls(
            location=table.location,
            input_idx=table.input_idx,
            version_pos=table.version_pos,
            execution_id=cls.text(table.execution_id),
            transaction_id=cls.text(table.transaction_id),
            physical_table_data_version_id=cls.text(
                table.physical_table_data_version_id
            ),
            triggered_on=table.triggered_on,
        )

    @staticmethod
    def text(value: str | int | None) -> str | None:
        if value is None:
            return None
        return str(value)

    def version(self):
        from tabsdatak.tableframe._engine.model.frame import DataVersion

        if (
            self.execution_id is None
            or self.transaction_id is None
            or self.physical_table_data_version_id is None
            or self.triggered_on is None
        ):
            return None
        return (
            DataVersion
            .builder()
            .with_execution(self.execution_id)
            .with_transaction(self.transaction_id)
            .with_version(self.physical_table_data_version_id)
            .with_timestamp(self.triggered_on)
            .build()
        )


@dataclass(frozen=True)
class InputEntry:
    name: str
    table_pos: int
    is_empty: bool = False
    tables: tuple[InputRecord, ...] = ()


@dataclass(frozen=True)
class InputManifest(Manifest):
    inputs: tuple[InputEntry, ...]

    @classmethod
    def of(
        cls,
        names: list[str],
        records: list[list[InputRecord]],
    ) -> InputManifest:
        return cls(
            inputs=tuple(
                InputEntry(
                    name=name,
                    table_pos=position,
                    tables=tuple(tables),
                )
                for position, (name, tables) in enumerate(zip(names, records))
            ),
        )

    @classmethod
    def from_request(cls, request: Request) -> InputManifest:
        entries: list[InputEntry] = []
        for entry in request.input:
            is_empty = isinstance(entry, InputTableVersions) and entry.is_empty
            if is_empty:
                records: tuple[InputRecord, ...] = ()
            else:
                records = tuple(InputRecord.of(table) for table in entry.tables)
            entries.append(
                InputEntry(
                    name=entry.tables[0].table,
                    table_pos=entry.tables[0].table_pos,
                    is_empty=is_empty,
                    tables=records,
                )
            )
        return cls(inputs=tuple(entries))

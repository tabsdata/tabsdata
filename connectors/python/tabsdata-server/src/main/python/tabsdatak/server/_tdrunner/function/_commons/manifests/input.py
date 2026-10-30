#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

from tabsdatak.server._tdrunner.function._commons.document.request import Location
from tabsdatak.server._tdrunner.function._commons.manifests.manifest import Manifest
from tabsdatak.spi import MetadataSpec

SINGLE_VERSION_POS = -1


@dataclass(frozen=True)
class InputRecord:
    file: str | None = None
    location: Location | None = None
    schema: Location | None = None
    metadata: MetadataSpec | None = None
    input_idx: int | None = None
    version_pos: int = SINGLE_VERSION_POS
    execution_id: str | None = None
    transaction_id: str | None = None
    physical_table_data_version_id: str | None = None
    triggered_on: int | None = None

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

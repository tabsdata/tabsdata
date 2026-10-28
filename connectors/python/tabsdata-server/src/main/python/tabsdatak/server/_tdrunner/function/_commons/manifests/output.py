#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

from tabsdatak.server._tdrunner.function._commons.manifests.manifest import Manifest


@dataclass(frozen=True)
class TableStats:
    column_count: int
    row_count: int
    schema_hash: str
    data_bytes: int


@dataclass(frozen=True)
class OutputEntry:
    name: str
    uri: str | None = None
    stats: TableStats | None = None


@dataclass(frozen=True)
class OutputManifest(Manifest):
    outputs: tuple[OutputEntry, ...]
    state: OutputEntry | None = None

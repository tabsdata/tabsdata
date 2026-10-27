#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

from tabsdatak.server._tdrunner.function._commons.manifests.manifest import Manifest


@dataclass(frozen=True)
class OutputEntry:
    name: str
    file: str | None = None


@dataclass(frozen=True)
class OutputManifest(Manifest):
    outputs: tuple[OutputEntry, ...]

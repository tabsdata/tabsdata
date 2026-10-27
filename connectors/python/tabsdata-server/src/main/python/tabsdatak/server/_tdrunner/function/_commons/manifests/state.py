#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

from tabsdatak.server._tdrunner.function._commons.manifests.manifest import Manifest


@dataclass(frozen=True)
class StateEntry:
    name: str
    files: tuple[str, ...] = ()


@dataclass(frozen=True)
class StateManifest(Manifest):
    state: tuple[StateEntry, ...]

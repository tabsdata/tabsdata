#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from tabsdatak._tdruntime.python.binary import Binary


class TabsdataEdition(str, Enum):
    OPEN_SOURCE = "Open Source"
    ENTERPRISE = "Enterprise"


class InstallRecipe(str, Enum):
    REGISTRY = "Registry"
    WHEEL = "Wheel"
    FROZEN = "Frozen"
    EDITABLE = "Editable"


SUPPORTED_INSTALL_RECIPES: frozenset[InstallRecipe] = frozenset({
    InstallRecipe.REGISTRY,
    InstallRecipe.EDITABLE,
})


@dataclass(frozen=True)
class PackageRecipe:
    name: str
    recipe: InstallRecipe
    version: str | None
    location: Path | None
    binaries: tuple[Binary, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class VenvRecipe:
    name: str
    packages: tuple[PackageRecipe, ...]

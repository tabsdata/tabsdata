#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from tabsdatak.common._interpreter.python.binary import Binary


class TabsdataEdition(str, Enum):
    OPEN_SOURCE = "Open Source"
    ENTERPRISE = "Enterprise"


class InstallRecipe(str, Enum):
    EDITABLE = "Editable"
    FROZEN = "Frozen"
    INDEX = "Index"
    URL = "Url"
    VCS = "Vcs"
    WHEEL = "Wheel"


SUPPORTED_INSTALL_RECIPES: frozenset[InstallRecipe] = frozenset({
    InstallRecipe.EDITABLE,
    InstallRecipe.FROZEN,
    InstallRecipe.INDEX,
    InstallRecipe.URL,
    InstallRecipe.VCS,
    InstallRecipe.WHEEL,
})


@dataclass(frozen=True)
class PackageRecipe:
    name: str
    recipe: InstallRecipe
    version: str | None
    location: Path | None
    binaries: tuple[Binary, ...] = field(default_factory=tuple)
    url: str | None = None


@dataclass(frozen=True)
class VenvRecipe:
    name: str
    packages: tuple[PackageRecipe, ...]
    requirements: Path | None = None

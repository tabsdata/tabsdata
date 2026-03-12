#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PackageDescriptor:
    name: str
    display: str

    @property
    def package(self) -> str:
        prefix = "tabsdata-"
        if self.name.startswith(prefix):
            return self.name[len(prefix) :]
        return self.name

    @property
    def module(self) -> tuple[str, ...]:
        return tuple(self.package.split("-"))

    @property
    def namespace(self) -> str:
        return ".".join(("tabsdata", "connector", *self.module))

    @property
    def manifest_path(self) -> str:
        return os.path.join(
            "src",
            "main",
            "python",
            "tabsdata",
            "connector",
            *self.module,
            "assets",
            "manifest",
        )

    @property
    def spinner_message(self) -> str:
        return f"Building package '{self.name}' ({self.display})..."


def load_package_descriptor(setup_file: str) -> PackageDescriptor:
    setup_folder = Path(setup_file).parent
    pyproject_path = setup_folder / "pyproject.toml"
    if not pyproject_path.exists():
        raise FileNotFoundError(f"pyproject.toml not found at {pyproject_path}")
    with open(pyproject_path, "rb") as f:
        pyproject_toml = tomllib.load(f)
    tabsdata = pyproject_toml["tool"]["tabsdata"]
    return PackageDescriptor(
        name=tabsdata["name"],
        display=tabsdata["display"],
    )

#
#  Copyright 2026 Tabsdata Inc.
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

    def module_variants(self) -> tuple[tuple[str, ...], ...]:
        if not self.module:
            return ((),)
        *head, leaf = self.module
        return (
            (*head, leaf),
            (*head, f"_{leaf}"),
        )

    def namespace_parts(self, root: str) -> tuple[str, ...]:
        if self.module == ("api",):
            return (root,)
        infix = ("connector",) if root == "tabsdata" else ()
        for variant in self.module_variants():
            parts = (root, *infix, *variant)
            source_root = os.path.join("src", "main", "python", *parts)
            if os.path.isdir(source_root):
                return parts
        return (root, *infix, *self.module)

    def namespace(self, root: str) -> str:
        return ".".join(self.namespace_parts(root))

    def manifest_path(self, root: str) -> str:
        parts = self.namespace_parts(root)
        return os.path.join("src", "main", "python", *parts, "assets", "manifest")

    @property
    def primary_manifest_path(self) -> str:
        for root in ("tabsdatak", "tabsdata"):
            candidate_path = self.manifest_path(root)
            if os.path.isdir(candidate_path):
                return candidate_path
        return self.manifest_path("tabsdata")

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

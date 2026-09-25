#
# Copyright 2026 Tabsdata Inc.
#

from dataclasses import dataclass
from pathlib import Path

import yaml
from packaging.version import Version

from tabsdatax.cmake.kupgrader.common.emission import header_of, render
from tabsdatax.cmake.kupgrader.common.errors import KupgraderError

INVENTORY_SCHEMA = 1


class VersionError(KupgraderError):
    pass


@dataclass
class Entry:
    version: str
    upgrader: bool


@dataclass
class Resolution:
    current: str
    previous: str | None
    entries: list[Entry]

    @property
    def exhausted(self) -> bool:
        return self.previous is None


def read_current(version_file: Path) -> str:
    if not version_file.is_file():
        raise VersionError(f"The version file '{version_file}' does not exist.")
    current = version_file.read_text().strip()
    if not current:
        raise VersionError(f"The version file '{version_file}' is empty.")
    return current


def read_inventory(upgrades_file: Path) -> list[Entry]:
    if not upgrades_file.is_file():
        raise VersionError(f"The upgrades file '{upgrades_file}' does not exist.")
    document = yaml.safe_load(upgrades_file.read_text()) or {}

    schema = document.get("schema")
    if schema != INVENTORY_SCHEMA:
        raise VersionError(
            f"The upgrades file '{upgrades_file}' declares schema '{schema}', but only "
            f"schema '{INVENTORY_SCHEMA}' is supported."
        )

    versions = document.get("versions")
    if not versions:
        raise VersionError(f"The upgrades file '{upgrades_file}' declares no versions.")

    return [
        Entry(version=str(entry["version"]), upgrader=bool(entry["upgrader"]))
        for entry in versions
    ]


def resolve(current: str, entries: list[Entry]) -> Resolution:
    newest = entries[0]
    if newest.version == current:
        previous = entries[1].version if len(entries) > 1 else None
    else:
        previous = newest.version
    return Resolution(current=current, previous=previous, entries=entries)


def declares_upgrader(resolution: Resolution) -> bool:
    return any(
        entry.version == resolution.current and entry.upgrader
        for entry in resolution.entries
    )


def with_upgrader(resolution: Resolution) -> list[Entry]:
    older = [
        entry for entry in resolution.entries if entry.version != resolution.current
    ]
    return [Entry(version=resolution.current, upgrader=True), *older]


def write_inventory(upgrades_file: Path, entries: list[Entry]) -> None:
    newest_first = sorted(
        entries, key=lambda entry: Version(entry.version), reverse=True
    )
    document = {
        "schema": INVENTORY_SCHEMA,
        "versions": [
            {"version": entry.version, "upgrader": entry.upgrader}
            for entry in newest_first
        ],
    }
    upgrades_file.write_text(render(document, header_of(upgrades_file)))

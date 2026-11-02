#
# Copyright 2026 Tabsdata Inc.
#

from dataclasses import dataclass
from pathlib import Path

import yaml
from packaging.version import InvalidVersion, Version

from tabsdatax.cmake.kupgrader.common.emission import header_of, render
from tabsdatax.cmake.kupgrader.common.errors import KupgraderError

INVENTORY_SCHEMA = 1
INITIAL_MICRO = 0


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


def parse_version(version: str) -> Version:
    try:
        return Version(version)
    except InvalidVersion as error:
        raise VersionError(f"'{version}' is not a valid version.") from error


def resolve(current: str, entries: list[Entry]) -> Resolution:
    newest = entries[0]
    if parse_version(current) < parse_version(newest.version):
        raise VersionError(
            f"The current version '{current}' is older than the newest version "
            f"'{newest.version}' in the upgrades inventory. Bump the version file or "
            "fix the inventory."
        )
    if newest.version == current:
        previous = entries[1].version if len(entries) > 1 else None
    else:
        previous = newest.version
    return Resolution(current=current, previous=previous, entries=entries)


def unreachable_releases(
    releases: list[str], entries: list[Entry], current: str
) -> list[str]:
    listed = [Version(entry.version) for entry in entries]
    if not listed:
        return []
    floor = min(listed)
    unreachable = []
    for release in sorted(releases, key=Version):
        version = Version(release)
        if not floor <= version <= Version(current):
            continue
        if not any(
            (entry.major, entry.minor) == (version.major, version.minor)
            and entry <= version
            for entry in listed
        ):
            unreachable.append(release)
    return unreachable


def is_micro(version: str) -> bool:
    return Version(version).micro > INITIAL_MICRO


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

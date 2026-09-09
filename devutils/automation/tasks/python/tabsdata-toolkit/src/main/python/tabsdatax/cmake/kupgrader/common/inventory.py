#
# Copyright 2026 Tabsdata Inc.
#

from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from tabsdatax.cmake.kupgrader.common.naming import FAMILIES, parsable, parse_name
from tabsdatax.cmake.kupgrader.common.normalization import equivalent


class State(Enum):
    UNCHANGED = "Unchanged"
    CHANGED = "Changed"
    NEW = "New"
    DELETED = "Deleted"


@dataclass
class Variant:
    family: str
    bundle: str
    name: str
    state: State
    parsable: bool
    previous: Path | None
    current: Path | None


@dataclass
class Bundle:
    family: str
    name: str
    variants: list[Variant]

    @property
    def altered(self) -> bool:
        return any(variant.state is not State.UNCHANGED for variant in self.variants)

    @property
    def suffixed(self) -> bool:
        return any(parse_name(variant.name).tokens for variant in self.variants)

    def counted(self, state: State) -> int:
        return sum(1 for variant in self.variants if variant.state is state)


def read_family(kprofile_folder: Path, family: str) -> dict[str, Path]:
    folder = kprofile_folder / family
    if not folder.is_dir():
        return {}
    return {path.name: path for path in sorted(folder.iterdir()) if path.is_file()}


def state_of(family: str, previous: Path | None, current: Path | None) -> State:
    if previous is None:
        return State.NEW
    if current is None:
        return State.DELETED
    if previous.read_bytes() == current.read_bytes():
        return State.UNCHANGED
    if equivalent(family, previous, current):
        return State.UNCHANGED
    return State.CHANGED


def collect(previous_folder: Path, current_folder: Path) -> list[Bundle]:
    bundles: list[Bundle] = []

    for family in FAMILIES:
        previous_files = read_family(previous_folder, family)
        current_files = read_family(current_folder, family)

        grouped: dict[str, list[str]] = {}
        for name in sorted(set(previous_files) | set(current_files)):
            grouped.setdefault(parse_name(name).stripped, []).append(name)

        for bundle_name, names in sorted(grouped.items()):
            variants = [
                Variant(
                    family=family,
                    bundle=bundle_name,
                    name=name,
                    state=state_of(
                        family, previous_files.get(name), current_files.get(name)
                    ),
                    parsable=parsable(family, name),
                    previous=previous_files.get(name),
                    current=current_files.get(name),
                )
                for name in names
            ]
            bundles.append(Bundle(family=family, name=bundle_name, variants=variants))

    return bundles

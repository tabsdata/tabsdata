#
# Copyright 2026 Tabsdata Inc.
#

import re
from dataclasses import dataclass

from packaging.requirements import InvalidRequirement, Requirement

from tabsdatax.cmake.kupgrader.common.operations import Gap, Operation, Synthesis

REQUIREMENT_EXTENSIONS = (".txt",)


NAME_PATTERN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)\s*(\[[^]]*\])?\s*(.*)$")


@dataclass
class Entry:
    line: str
    name: str | None
    specifier: str

    @property
    def parsed(self) -> bool:
        return self.name is not None


def is_requirements(filename: str) -> bool:
    return filename.endswith(REQUIREMENT_EXTENSIONS)


def entry_of(line: str) -> Entry:
    try:
        requirement = Requirement(line)
        return Entry(
            line=line,
            name=requirement.name.lower(),
            specifier=str(requirement.specifier),
        )
    except InvalidRequirement:
        pass

    matched = NAME_PATTERN.match(line)
    if matched is None:
        return Entry(line=line, name=None, specifier="")
    return Entry(
        line=line,
        name=matched.group(1).lower(),
        specifier=matched.group(3).strip(),
    )


def entries_of(text: str) -> list[Entry]:
    entries = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        entries.append(entry_of(line))
    return entries


def indexed(entries: list[Entry]) -> dict[str, Entry]:
    return {entry.name: entry for entry in entries if entry.parsed}


def unparsed(entries: list[Entry]) -> list[Entry]:
    return [entry for entry in entries if not entry.parsed]


def duplicated(entries: list[Entry]) -> list[Entry]:
    seen: dict[str, int] = {}
    for entry in entries:
        if entry.parsed:
            seen[entry.name] = seen.get(entry.name, 0) + 1
    return [entry for entry in entries if entry.parsed and seen[entry.name] > 1]


def synthesize_requirements(previous: str, current: str) -> Synthesis:
    synthesis = Synthesis()

    previous_entries = entries_of(previous)
    current_entries = entries_of(current)

    for entry in unparsed(previous_entries) + unparsed(current_entries):
        synthesis.gaps.append(
            Gap(
                leaf=entry.line,
                shape="Requirement holds no readable package name",
                detail=(
                    "The line does not start with a package name, so its change "
                    "cannot be derived."
                ),
            )
        )
    for entry in duplicated(previous_entries) + duplicated(current_entries):
        synthesis.gaps.append(
            Gap(
                leaf=entry.line,
                shape="Requirement declared more than once",
                detail=(
                    f"'{entry.name}' is declared by several lines, so the change to "
                    "each of them cannot be told apart."
                ),
            )
        )

    if synthesis.gaps:
        return synthesis

    previous_index = indexed(previous_entries)
    current_index = indexed(current_entries)

    for name, entry in previous_index.items():
        if name not in current_index:
            synthesis.gaps.append(
                Gap(
                    leaf=entry.line,
                    shape="Requirement removed",
                    detail=(
                        "No operation removes a line from a text file. An operation "
                        "able to drop a matched line is required."
                    ),
                )
            )

    for name, entry in current_index.items():
        if name not in previous_index:
            synthesis.gaps.append(
                Gap(
                    leaf=entry.line,
                    shape="Requirement added",
                    detail=(
                        "No operation appends a line to a text file. An operation "
                        "able to add a line when it is absent is required."
                    ),
                )
            )
            continue
        before = previous_index[name]
        if before.line == entry.line:
            continue
        synthesis.operations.append(
            Operation(
                op="modify-text-if",
                fields={
                    "pattern": re.escape(before.line),
                    "replacement": entry.line,
                },
            )
        )

    reconcile(synthesis, previous_index, current_index)
    return synthesis


def reconcile(
    synthesis: Synthesis,
    previous_index: dict[str, Entry],
    current_index: dict[str, Entry],
) -> None:
    if synthesis.gaps:
        return
    accounted = sum(
        1
        for name, entry in current_index.items()
        if previous_index[name].line == entry.line
    )
    if accounted + len(synthesis.operations) != len(current_index):
        synthesis.gaps.append(
            Gap(
                leaf="<file>",
                shape="Requirements could not be reconciled",
                detail=(
                    f"{len(current_index)} requirement(s) were expected, but "
                    f"{accounted} unchanged and {len(synthesis.operations)} rewritten "
                    "were derived. The change cannot be trusted."
                ),
            )
        )

#
# Copyright 2026 Tabsdata Inc.
#

import difflib
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from tabsdatax.cmake.kupgrader.common.emission import Emission


@dataclass
class Difference:
    path: str
    reason: str
    diff: list[str] = field(default_factory=list)


@dataclass
class Comparison:
    differences: list[Difference] = field(default_factory=list)

    @property
    def matched(self) -> bool:
        return not self.differences


def payload_names(folder: Path) -> set[str]:
    if not folder.is_dir():
        return set()
    return {
        str(path.relative_to(folder)) for path in folder.rglob("*") if path.is_file()
    }


def compare_spec(emission: Emission, shipped_spec_file: Path) -> list[Difference]:
    generated = emission.spec_file.read_text()
    if not shipped_spec_file.is_file():
        return [
            Difference(
                path=shipped_spec_file.name,
                reason="The repository holds no specification for this version",
            )
        ]
    shipped = shipped_spec_file.read_text()
    if shipped == generated:
        return []
    return [
        Difference(
            path=shipped_spec_file.name,
            reason="The shipped specification differs from the generated one",
            diff=list(
                difflib.unified_diff(
                    shipped.splitlines(),
                    generated.splitlines(),
                    fromfile="shipped",
                    tofile="generated",
                    lineterm="",
                )
            ),
        )
    ]


def compare_payloads(emission: Emission, shipped_folder: Path) -> list[Difference]:
    differences = []
    generated = payload_names(emission.content_folder)
    shipped = payload_names(shipped_folder)

    for name in sorted(generated - shipped):
        differences.append(
            Difference(path=name, reason="Missing from the shipped upgrader")
        )
    for name in sorted(shipped - generated):
        differences.append(
            Difference(path=name, reason="Left over in the shipped upgrader")
        )
    for name in sorted(generated & shipped):
        if (emission.content_folder / name).read_bytes() != (
            shipped_folder / name
        ).read_bytes():
            differences.append(
                Difference(path=name, reason="Content differs from the shipped payload")
            )

    return differences


def compare(
    emission: Emission, shipped_spec_file: Path, shipped_folder: Path
) -> Comparison:
    return Comparison(
        differences=compare_spec(emission, shipped_spec_file)
        + compare_payloads(emission, shipped_folder)
    )


def commit(emission: Emission, shipped_spec_file: Path, shipped_folder: Path) -> None:
    shipped_spec_file.parent.mkdir(parents=True, exist_ok=True)
    if shipped_spec_file.exists():
        shipped_spec_file.unlink()
    if shipped_folder.exists():
        shutil.rmtree(shipped_folder)

    shutil.copyfile(emission.spec_file, shipped_spec_file)
    if emission.content_folder.is_dir():
        shutil.copytree(emission.content_folder, shipped_folder)

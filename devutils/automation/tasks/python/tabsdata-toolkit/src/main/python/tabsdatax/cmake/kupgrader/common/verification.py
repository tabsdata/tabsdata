#
# Copyright 2026 Tabsdata Inc.
#

import json
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from tabsdatax.cmake.kupgrader.common.emission import Emission
from tabsdatax.cmake.kupgrader.common.errors import KupgraderError
from tabsdatax.cmake.kupgrader.common.naming import FAMILIES
from tabsdatax.cmake.kupgrader.common.normalization import (
    NormalizationError,
    normalize,
)

UPGRADER_BINARY = "tdk_kupgrader"
UPGRADER_PROFILE = "debug"

INVENTORY = (
    "#\n# Copyright 2026 Tabsdata Inc.\n#\n\nschema: 1\nversions:\n"
    "  - version: {current}\n    upgrader: true\n"
    "  - version: {previous}\n    upgrader: false\n"
)


class VerificationError(KupgraderError):
    pass


@dataclass
class Mismatch:
    family: str
    name: str
    reason: str


@dataclass
class Verification:
    binary: Path
    steps: int
    changed: int
    identical: int
    mismatches: list[Mismatch] = field(default_factory=list)

    @property
    def matched(self) -> bool:
        return not self.mismatches


def locate_binary(root_folder: Path) -> Path:
    binary = root_folder / "target" / UPGRADER_PROFILE / UPGRADER_BINARY
    if binary.is_file():
        return binary
    raise VerificationError(
        f"The '{UPGRADER_BINARY}' binary was not found at '{binary}'. It must be "
        f"compiled with the '{UPGRADER_PROFILE}' profile. Build it with "
        f"'cargo build -p td-server --bin {UPGRADER_BINARY}' and run the task again."
    )


def stage(
    emission: Emission,
    previous_kprofile_folder: Path,
    work_folder: Path,
    current: str,
    previous: str,
) -> tuple[Path, Path]:
    if work_folder.exists():
        shutil.rmtree(work_folder)
    work_folder.mkdir(parents=True)

    kupgrade_folder = work_folder / "kupgrade"
    (kupgrade_folder / "upgrades").mkdir(parents=True)
    shutil.copyfile(
        emission.spec_file, kupgrade_folder / "upgrades" / emission.spec_file.name
    )
    if emission.content_folder.is_dir():
        shutil.copytree(
            emission.content_folder,
            kupgrade_folder / "upgrades" / emission.content_folder.name,
        )
    (kupgrade_folder / "upgrades.yaml").write_text(
        INVENTORY.format(current=current, previous=previous)
    )

    kprofile_folder = work_folder / "kprofile"
    shutil.copytree(previous_kprofile_folder, kprofile_folder)

    return kupgrade_folder, kprofile_folder


def apply_upgrader(
    binary: Path, kupgrade_folder: Path, kprofile_folder: Path, version: str
) -> dict:
    completed = subprocess.run(
        [
            str(binary),
            "--kupgrade",
            str(kupgrade_folder),
            "--kprofile",
            str(kprofile_folder),
            "--version",
            version,
        ],
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise VerificationError(
            f"The upgrader could not be applied: {completed.stderr.strip()[:400]}"
        )
    return json.loads(completed.stdout)


def compare(upgraded_folder: Path, current_folder: Path) -> tuple[int, list[Mismatch]]:
    identical = 0
    mismatches = []

    for family in FAMILIES:
        upgraded = files_of(upgraded_folder / family)
        current = files_of(current_folder / family)

        for name in sorted(current - upgraded):
            mismatches.append(
                Mismatch(family, name, "Missing from the upgraded kprofile")
            )
        for name in sorted(upgraded - current):
            mismatches.append(
                Mismatch(family, name, "Left over in the upgraded kprofile")
            )
        for name in sorted(upgraded & current):
            try:
                same = normalize(family, upgraded_folder / family / name) == normalize(
                    family, current_folder / family / name
                )
            except NormalizationError as error:
                mismatches.append(Mismatch(family, name, str(error)))
                continue
            if same:
                identical += 1
            else:
                mismatches.append(
                    Mismatch(family, name, "Content differs from the current kprofile")
                )

    return identical, mismatches


def files_of(folder: Path) -> set[str]:
    if not folder.is_dir():
        return set()
    return {path.name for path in folder.iterdir() if path.is_file()}


def verify(
    root_folder: Path,
    emission: Emission,
    previous_kprofile_folder: Path,
    current_kprofile_folder: Path,
    work_folder: Path,
    current: str,
    previous: str,
) -> Verification:
    binary = locate_binary(root_folder)
    kupgrade_folder, kprofile_folder = stage(
        emission, previous_kprofile_folder, work_folder, current, previous
    )
    report = apply_upgrader(binary, kupgrade_folder, kprofile_folder, current)
    identical, mismatches = compare(kprofile_folder, current_kprofile_folder)
    steps = report.get("steps", [])
    return Verification(
        binary=binary,
        steps=len(steps),
        changed=sum(1 for step in steps if step.get("changed")),
        identical=identical,
        mismatches=mismatches,
    )

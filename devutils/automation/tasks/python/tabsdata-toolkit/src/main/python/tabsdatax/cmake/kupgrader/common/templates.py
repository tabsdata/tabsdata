#
# Copyright 2026 Tabsdata Inc.
#

import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from tabsdatax.cmake.kupgrader.common.emission import Emission
from tabsdatax.cmake.kupgrader.common.errors import KupgraderError
from tabsdatax.cmake.kupgrader.common.versions import (
    Resolution,
    with_upgrader,
    write_inventory,
)
from tabsdatax.cmake.kupgrader.common.workflow import Context


class TemplatesError(KupgraderError):
    pass


@dataclass
class Mismatch:
    provider: str
    version: str
    file: str
    reason: str


def run_upgrader(binary: Path, arguments: list[str]) -> dict:
    completed = subprocess.run(
        [str(binary), *arguments],
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise TemplatesError(
            f"The upgrader binary failed running '{arguments[0]}': "
            f"{completed.stderr.strip()[:400]}"
        )
    return json.loads(completed.stdout)


def mismatches_of(report: dict) -> list[Mismatch]:
    return [Mismatch(**mismatch) for mismatch in report.get("mismatches", [])]


def repository_arguments(context: Context, kupgrade_folder: Path) -> list[str]:
    return [
        "--repository",
        str(context.root_folder),
        "--kupgrade",
        str(kupgrade_folder),
    ]


def replay(binary: Path, context: Context, kupgrade_folder: Path) -> list[Mismatch]:
    return mismatches_of(
        run_upgrader(
            binary,
            [
                "replay",
                *repository_arguments(context, kupgrade_folder),
                "--diverged",
                str(context.diverged_folder),
            ],
        )
    )


def digests(
    binary: Path, context: Context, kupgrade_folder: Path, record: bool
) -> tuple[int, list[Mismatch]]:
    arguments = ["digests", *repository_arguments(context, kupgrade_folder)]
    if record:
        arguments.append("--record")
    report = run_upgrader(binary, arguments)
    return report.get("recorded", 0), mismatches_of(report)


def materialize(binary: Path, context: Context, output_folder: Path) -> Path:
    if output_folder.exists():
        shutil.rmtree(output_folder)
    report = run_upgrader(
        binary,
        [
            "templates",
            *repository_arguments(context, context.kupgrade_folder),
            "--output",
            str(output_folder),
        ],
    )
    return Path(report["folder"])


def stage_kupgrade(
    context: Context, emission: Emission, resolution: Resolution
) -> Path:
    """The shipped upgrade tree with the generated upgrader in place, so the
    replay walks every earlier hop plus the one about to be committed."""
    staged = context.work_folder / "replay" / "kupgrade"
    if staged.exists():
        shutil.rmtree(staged)
    shutil.copytree(context.kupgrade_folder, staged)
    upgrades = staged / context.upgrades_folder.name
    shutil.copyfile(emission.spec_file, upgrades / emission.spec_file.name)
    content = upgrades / emission.content_folder.name
    if content.exists():
        shutil.rmtree(content)
    if emission.content_folder.is_dir():
        shutil.copytree(emission.content_folder, content)
    write_inventory(staged / context.upgrades_file.name, with_upgrader(resolution))
    if context.diverged_folder.exists():
        shutil.rmtree(context.diverged_folder)
    return staged

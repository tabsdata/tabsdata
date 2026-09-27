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

PATCHES_FOLDER = "patches"


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


def patch_file(context: Context, version: str) -> Path:
    return context.templates_folder / PATCHES_FOLDER / f"{version}.patch"


def apply_patch(kprofile_folder: Path, patch: Path) -> None:
    """A release whose package differs from its tag gets that difference as a
    patch, so its templates hold what was really published."""
    completed = subprocess.run(
        ["git", "apply", "--unsafe-paths", "-p1", str(patch)],
        cwd=kprofile_folder,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise TemplatesError(
            f"The patch '{patch}' does not apply to '{kprofile_folder}': "
            f"{completed.stderr.strip()[:400]}"
        )


def mismatches_of(report: dict) -> list[Mismatch]:
    return [Mismatch(**mismatch) for mismatch in report.get("mismatches", [])]


def version_templates(
    binary: Path, kprofile_folder: Path, context: Context, version: str, write: bool
) -> list[Mismatch]:
    arguments = [
        "templates",
        "--kprofile",
        str(kprofile_folder),
        "--templates",
        str(context.templates_folder),
        "--version",
        version,
    ]
    if write:
        arguments.append("--write")
    return mismatches_of(run_upgrader(binary, arguments))


def replay(binary: Path, kupgrade_folder: Path, context: Context) -> list[Mismatch]:
    return mismatches_of(
        run_upgrader(
            binary,
            [
                "replay",
                "--kupgrade",
                str(kupgrade_folder),
                "--templates",
                str(context.templates_folder),
                "--diverged",
                str(context.diverged_folder),
            ],
        )
    )


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

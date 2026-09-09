#
# Copyright 2026 Tabsdata Inc.
#

import logging
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.table import Table

from tabsdatax.cmake.kupgrader.common.sources import (
    copy_kprofile,
    extract_kprofile,
    reset_folder,
    resolve_tag,
)
from tabsdatax.cmake.kupgrader.common.versions import (
    Resolution,
    read_current,
    read_inventory,
    resolve,
)
from tabsdatax.cmake.kupgrader.common.workflow import RENDER_WIDTH, Context

log = logging.getLogger(__name__)


@dataclass
class Preparation:
    resolution: Resolution
    commit: str
    previous_kprofile_folder: Path
    current_kprofile_folder: Path


def count_files(folder: Path) -> int:
    if not folder.is_dir():
        return 0
    return sum(1 for path in folder.rglob("*") if path.is_file())


def display_resolution(context: Context, resolution: Resolution) -> None:
    console = Console(width=RENDER_WIDTH)

    table = Table(
        title="Resolved Versions",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Version", style="cyan", no_wrap=True)
    table.add_column("Upgrader", style="green", justify="center")
    table.add_column("Role", style="yellow")

    for entry in resolution.entries:
        if entry.version == resolution.current:
            role = "Current"
        elif entry.version == resolution.previous:
            role = "Previous"
        else:
            role = ""
        table.add_row(entry.version, "Yes" if entry.upgrader else "No", role)

    console.print()
    console.print(table)
    console.print()


def display_preparation(preparation: Preparation) -> None:
    console = Console(width=RENDER_WIDTH)

    table = Table(
        title="Prepared Sources",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Item", style="cyan", no_wrap=True)
    table.add_column("Value", style="yellow")
    table.add_column("Files", style="green", justify="right")

    table.add_row(
        f"Previous kprofile ({preparation.resolution.previous})",
        str(preparation.previous_kprofile_folder),
        str(count_files(preparation.previous_kprofile_folder)),
    )
    table.add_row(
        f"Current kprofile ({preparation.resolution.current})",
        str(preparation.current_kprofile_folder),
        str(count_files(preparation.current_kprofile_folder)),
    )
    table.add_row("Previous commit", preparation.commit, "")

    console.print()
    console.print(table)
    console.print()


def prepare(context: Context) -> Preparation | None:
    current = read_current(context.version_file)
    entries = read_inventory(context.upgrades_file)
    resolution = resolve(current, entries)
    display_resolution(context, resolution)

    previous = resolution.previous
    if previous is None:
        log.warning(
            f"Version '{resolution.current}' is the oldest entry of the inventory. "
            "There is no previous version to upgrade from."
        )
        return None

    commit = resolve_tag(context.root_folder, previous)

    reset_folder(context.work_folder)
    previous_kprofile_folder = extract_kprofile(
        context.root_folder, previous, context.previous_folder
    )
    current_kprofile_folder = copy_kprofile(context.root_folder, context.current_folder)

    preparation = Preparation(
        resolution=resolution,
        commit=commit,
        previous_kprofile_folder=previous_kprofile_folder,
        current_kprofile_folder=current_kprofile_folder,
    )
    display_preparation(preparation)
    return preparation

#
# Copyright 2026 Tabsdata Inc.
#

from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.table import Table

RENDER_WIDTH = 1024

WORK_FOLDER = Path("target") / "kupgrader"

RESOURCES_FOLDER = Path("variant") / "resources"
KPROFILE_FOLDER = RESOURCES_FOLDER / "kprofile"
KUPGRADE_FOLDER = RESOURCES_FOLDER / "kupgrade"

UPGRADES_FILE = KUPGRADE_FOLDER / "upgrades.yaml"
UPGRADES_FOLDER = KUPGRADE_FOLDER / "upgrades"

VERSION_FILE = (
    Path("connectors")
    / "python"
    / "tabsdata-api"
    / "src"
    / "main"
    / "python"
    / "tabsdatak"
    / "assets"
    / "manifest"
    / "VERSION"
)


@dataclass
class Context:
    mode: str
    root_folder: Path

    @property
    def work_folder(self) -> Path:
        return self.root_folder / WORK_FOLDER

    @property
    def kprofile_folder(self) -> Path:
        return self.root_folder / KPROFILE_FOLDER

    @property
    def kupgrade_folder(self) -> Path:
        return self.root_folder / KUPGRADE_FOLDER

    @property
    def upgrades_file(self) -> Path:
        return self.root_folder / UPGRADES_FILE

    @property
    def upgrades_folder(self) -> Path:
        return self.root_folder / UPGRADES_FOLDER

    @property
    def version_file(self) -> Path:
        return self.root_folder / VERSION_FILE

    @property
    def previous_folder(self) -> Path:
        return self.work_folder / "previous"

    @property
    def current_folder(self) -> Path:
        return self.work_folder / "current"

    @property
    def generated_folder(self) -> Path:
        return self.work_folder / "upgrades"

    @property
    def upgraded_folder(self) -> Path:
        return self.work_folder / "upgraded"

    @property
    def resolved_folder(self) -> Path:
        return self.work_folder / "resolved"

    @property
    def diverged_folder(self) -> Path:
        return self.work_folder / "replay" / "diverged"


def context_of(mode: str, root_folder: Path) -> Context:
    return Context(mode=mode, root_folder=root_folder.resolve())


def display_context(context: Context) -> None:
    console = Console(width=RENDER_WIDTH)

    table = Table(
        title="Kupgrader Context",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Item", style="cyan", no_wrap=True)
    table.add_column("Value", style="yellow")

    table.add_row("Mode", context.mode.capitalize())
    table.add_row("Repository", str(context.root_folder))
    table.add_row("Work folder", str(context.work_folder))
    table.add_row("Kprofile folder", str(context.kprofile_folder))
    table.add_row("Kupgrade folder", str(context.kupgrade_folder))
    table.add_row("Upgrades file", str(context.upgrades_file))
    table.add_row("Upgrades folder", str(context.upgrades_folder))
    table.add_row("Version file", str(context.version_file))

    console.print()
    console.print(table)
    console.print()

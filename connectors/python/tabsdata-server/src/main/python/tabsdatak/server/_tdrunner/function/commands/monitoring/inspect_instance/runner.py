#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

from tabsdatak.server._tdrunner.function._commons.monitor.sysinfo import SystemReport
from tabsdatak.server._tdrunner.function.commands.monitoring.inspect_instance.options import (  # noqa: E501
    InspectInstanceArguments,
)

MOUNTED_FOLDERS = (
    Path("tabsdata", "pods"),
    Path("tabsdata", "configs"),
)


class InspectInstance:
    arguments: InspectInstanceArguments
    folders: tuple[Path, ...]

    def __init__(self, arguments: InspectInstanceArguments) -> None:
        self.arguments = arguments
        self.folders = MOUNTED_FOLDERS

    def run(self) -> int:
        for folder in self.folders:
            self.show(folder)
        SystemReport().show()
        print("Done!", flush=True)
        return 0

    def show(self, folder: Path) -> None:
        print(f"Reading mounted '{folder}' folder", flush=True)
        try:
            paths = sorted(folder.iterdir())
        except OSError as error:
            print(f"[inspector]   {folder} = <unreadable: {error}>", flush=True)
            return
        for path in paths:
            if path.is_file():
                print(f"[inspector]   {path} = {self.contents(path)}", flush=True)

    @staticmethod
    def contents(path: Path) -> str:
        try:
            return path.read_text(encoding="utf-8", errors="replace")
        except OSError as error:
            return f"<unreadable: {error}>"

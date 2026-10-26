#
# Copyright 2026 Tabsdata Inc.
#

from pathlib import Path

from tabsdatax.cmake.kupgrader.common.templates import run_upgrader

OWNERSHIP_COMMAND = "ownership"
KUPGRADE_OPTION = "--kupgrade"
VERSION_OPTION = "--version"
PREVIOUS_OPTION = "--previous"
CURRENT_OPTION = "--current"
PROBLEMS_KEY = "problems"


def ownership_problems(
    binary: Path,
    kupgrade_folder: Path,
    version: str,
    previous_kprofile_folder: Path,
    current_kprofile_folder: Path,
) -> list[str]:
    report = run_upgrader(
        binary,
        [
            OWNERSHIP_COMMAND,
            KUPGRADE_OPTION,
            str(kupgrade_folder),
            VERSION_OPTION,
            version,
            PREVIOUS_OPTION,
            str(previous_kprofile_folder),
            CURRENT_OPTION,
            str(current_kprofile_folder),
        ],
    )
    return list(report.get(PROBLEMS_KEY, []))

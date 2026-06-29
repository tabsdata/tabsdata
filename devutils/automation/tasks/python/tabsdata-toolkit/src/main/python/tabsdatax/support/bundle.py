#
# Copyright 2026 Tabs Data Inc.
#

import os
import shutil
import sys
import tempfile
import time
import zipfile
from pathlib import Path

from rich.console import Console

TABSDATA_REPOSITORIES_HOME = "TABSDATA_REPOSITORIES_HOME"
PROJECT_TABSDATA_BASE_FOLDER = "PROJECT_TABSDATA_BASE_FOLDER"
TMPDIR = "TMPDIR"
HOME = "HOME"

REPOSITORIES = ("tabsdata-os", "tabsdata-ee", "tabsdata")

REPORTS = "reports"
TDLOCAL = "tdlocal"
TD_TEMP = "td-temp"
HARVESTER = "harvester"
INSTANCES = "instances"

TARGET = "target"
PYTEST_REPORT = ("reports", "pytest", "tabsdata")
TD_TEMP_FOLDER = "td-temp"
HARVESTER_FOLDER = (".tabsdatax", "pytest", "harvester")
INSTANCES_FOLDER = (".tabsdata", "instances")

LOG_PATTERN = "*.log*"

BUNDLE_PREFIX = "td-support"
BUNDLE_TIMESTAMP = "%Y%m%d-%H%M%S"
BUNDLE_EXTENSION = "zip"
STAGING_PREFIX = "td-support-"

console = Console()


def workspace_folder() -> Path:
    repositories_home = os.getenv(TABSDATA_REPOSITORIES_HOME)
    if repositories_home:
        return Path(repositories_home).resolve()
    base = os.getenv(PROJECT_TABSDATA_BASE_FOLDER)
    if not base:
        console.print(
            "⚠️ Neither the environment variable TABSDATA_REPOSITORIES_HOME nor "
            "PROJECT_TABSDATA_BASE_FOLDER is set, so the parent of the current "
            "folder will be used instead."
        )
        return Path.cwd().parent.resolve()
    return Path(base).resolve().parent


def bundle_sources() -> dict[str, tuple[Path, str | None]]:
    workspace = workspace_folder()
    sources: dict[str, tuple[Path, str | None]] = {}
    for repository in REPOSITORIES:
        repository_folder = workspace / repository
        sources[f"{repository}/{REPORTS}"] = (
            repository_folder.joinpath(TARGET, *PYTEST_REPORT),
            None,
        )
        sources[f"{repository}/{TDLOCAL}"] = (
            repository_folder / TARGET / TDLOCAL,
            None,
        )
    temp_folder = os.getenv(TMPDIR) or tempfile.gettempdir()
    sources[TD_TEMP] = (Path(temp_folder) / TD_TEMP_FOLDER, None)
    home_folder = os.getenv(HOME) or str(Path.home())
    sources[HARVESTER] = (Path(home_folder).joinpath(*HARVESTER_FOLDER), None)
    sources[INSTANCES] = (Path(home_folder).joinpath(*INSTANCES_FOLDER), LOG_PATTERN)
    return sources


def stage_source(source: Path, target: Path, pattern: str | None = None) -> int:
    target.mkdir(parents=True, exist_ok=True)
    if not source.exists():
        return 0
    if source.is_file():
        shutil.copy2(source, target / source.name)
        return 1
    staged = 0
    items = source.rglob(pattern) if pattern else source.rglob("*")
    for item in items:
        if item.is_dir():
            continue
        destination = target / item.relative_to(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(item, destination)
            staged = staged + 1
        except Exception as error:  # noqa: BLE001
            console.print(f"⚠️ The file '{item}' could not be staged: {error}.")
    return staged


def build_bundle() -> Path:
    timestamp = time.strftime(BUNDLE_TIMESTAMP)
    bundle_path = (
        Path(tempfile.gettempdir()) / f"{BUNDLE_PREFIX}-{timestamp}.{BUNDLE_EXTENSION}"
    )
    sources = bundle_sources()
    with tempfile.TemporaryDirectory(prefix=STAGING_PREFIX) as staging_root:
        staging_folder = Path(staging_root)
        for subfolder, (source, pattern) in sources.items():
            target = staging_folder / subfolder
            staged = stage_source(source, target, pattern)
            if staged > 0:
                console.print(
                    f"🧺 Staged {staged} file(s) for '{subfolder}' from {source}."
                )
            elif not source.exists():
                console.print(
                    f"🫙 No files were staged for '{subfolder}' because the source "
                    f"does not exist: {source}."
                )
            else:
                console.print(
                    f"🫙 No files were staged for '{subfolder}' because the source "
                    f"contains no matching files: {source}."
                )
        with zipfile.ZipFile(
            bundle_path, "w", compression=zipfile.ZIP_DEFLATED
        ) as bundle:
            for path in sorted(staging_folder.rglob("*")):
                archive_name = path.relative_to(staging_folder)
                if path.is_dir():
                    if not any(path.iterdir()):
                        bundle.write(path, f"{archive_name}/")
                else:
                    bundle.write(path, str(archive_name))
    return bundle_path


def main() -> int:
    bundle_path = build_bundle()
    console.print("")
    console.print(f"🛟 The support bundle was created at 👉 {bundle_path}.")
    console.print("")
    return 0


if __name__ == "__main__":
    sys.exit(main())

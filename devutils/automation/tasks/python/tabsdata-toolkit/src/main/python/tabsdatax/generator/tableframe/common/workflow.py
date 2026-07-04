#
# Copyright 2026 Tabs Data Inc.
#

import logging
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import rich_click as click

log = logging.getLogger(__name__)

TABSDATA_OS_ENV = "TABSDATA_OS"
MANIFEST_FOLDER_RELATIVE = Path(
    "devutils/automation/tasks/python/tabsdata-toolkit"
    "/src/main/resources/tabsdatak/generator/tableframe"
)
MODULE = "tabsdatak.tableframe"
PACKAGE_RELATIVE = Path("connectors/python/tabsdata-api")
MAIN_RELATIVE = Path("src/main/python")
UTEST_RELATIVE = Path("src/utest/python")

POLARS_KIND = "pl"
TABSDATA_KIND = "td"
MANIFEST_NAME_PATTERN = re.compile(r"manifest_(?P<version>.+)_(?P<kind>pl|td)\.json")


@dataclass(frozen=True)
class ManifestName:
    version: str
    kind: str

    @classmethod
    def of(cls, name: str) -> "ManifestName | None":
        match = MANIFEST_NAME_PATTERN.fullmatch(name)
        if match is None:
            return None
        return cls(version=match.group("version"), kind=match.group("kind"))

    def filename(self) -> str:
        return f"manifest_{self.version}_{self.kind}.json"


def default_manifest_folder() -> Path | None:
    root = os.environ.get(TABSDATA_OS_ENV)
    return Path(root) / MANIFEST_FOLDER_RELATIVE if root else None


def resolve_manifest_file(given: Path) -> Path:
    if given.is_absolute() or given.exists() or len(given.parts) > 1:
        return given
    folder = default_manifest_folder()
    return folder / given if folder is not None else given


def greatest_tabsdata_manifest(folder: Path) -> Path | None:
    from packaging.version import InvalidVersion, Version

    candidates: list[tuple[Version, Path]] = []
    for path in folder.glob(f"manifest_*_{TABSDATA_KIND}.json"):
        name = ManifestName.of(path.name)
        if name is None:
            continue
        try:
            candidates.append((Version(name.version), path))
        except InvalidVersion:
            continue
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: candidate[0])[1]


def default_package() -> Path | None:
    root = os.environ.get(TABSDATA_OS_ENV)
    return Path(root) / PACKAGE_RELATIVE if root else None


def main_root_of(package: Path) -> Path:
    return package / MAIN_RELATIVE


def main_folder_of(package: Path, module: str) -> Path:
    return package.joinpath(MAIN_RELATIVE, *module.split("."))


def utest_folder_of(package: Path, module: str) -> Path:
    first, *rest = module.split(".")
    return package.joinpath(UTEST_RELATIVE, f"test_{first}", *rest, "generator")


def require_manifest_folder(manifest: Path | None) -> Path:
    if manifest is not None:
        return manifest
    folder = default_manifest_folder()
    if folder is None:
        log.error(
            f"No manifest folder given and the {TABSDATA_OS_ENV} environment "
            f"variable is not set. Pass --manifest with the manifest folder."
        )
        sys.exit(1)
    return folder


def preview(title: str, entries: dict[str, str | Path | bool]) -> None:
    log.info(title)
    width = max(len(key) for key in entries)
    for key, value in entries.items():
        log.info(f"  {key}{'.' * (width - len(key))}: {value}")


def confirm(auto_accept: bool) -> None:
    if auto_accept:
        return
    if not click.confirm("Proceed?", default=None):
        log.info("Aborted.")
        sys.exit(0)


def confirm_overwrite(targets: list[Path], auto_accept: bool) -> None:
    existing = [target for target in targets if target.exists()]
    if existing and not auto_accept:
        for target in existing:
            log.warning(f"Will overwrite existing file: {target}")
        if not click.confirm("Overwrite?", default=None):
            log.info("Aborted.")
            sys.exit(0)

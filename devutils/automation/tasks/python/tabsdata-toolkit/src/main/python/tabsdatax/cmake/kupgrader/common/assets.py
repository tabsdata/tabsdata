#
# Copyright 2026 Tabsdata Inc.
#

import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

from tabsdatax.cmake.kupgrader.common.errors import KupgraderError
from tabsdatax.cmake.kupgrader.common.workflow import Context

log = logging.getLogger(__name__)

ASSETS_FAMILY = "assets"

STORAGE_SOURCE_FOLDER = (
    Path("connectors")
    / "python"
    / "tabsdata-common"
    / "src"
    / "main"
    / "python"
    / "tabsdatak"
    / "common"
    / "_storage"
)

COPIED_ASSETS = {
    "mounts.py": STORAGE_SOURCE_FOLDER / "mounts.py",
    "transfer.py": STORAGE_SOURCE_FOLDER / "transfer.py",
}


@dataclass(frozen=True)
class CopiedAsset:
    name: str
    source: Path
    target: Path

    @property
    def current(self) -> bool:
        return (
            self.target.is_file()
            and self.target.read_bytes() == self.source.read_bytes()
        )


def copied_assets(context: Context) -> list[CopiedAsset]:
    assets = []
    for name, source in COPIED_ASSETS.items():
        path = context.root_folder / source
        if not path.is_file():
            raise KupgraderError(
                f"The source {path} of the kprofile asset '{name}' does not exist."
            )
        assets.append(
            CopiedAsset(
                name=name,
                source=path,
                target=context.kprofile_folder / ASSETS_FAMILY / name,
            )
        )
    return assets


def stale_assets(context: Context) -> list[CopiedAsset]:
    return [asset for asset in copied_assets(context) if not asset.current]


def verify_assets(context: Context) -> None:
    stale = stale_assets(context)
    for asset in stale:
        log.error(f"The kprofile asset {asset.target} is not a copy of {asset.source}.")
    if stale:
        log.error("Run 'cargo make upgrader build' to copy them.")
        raise SystemExit(1)


def copy_assets(context: Context) -> list[CopiedAsset]:
    stale = stale_assets(context)
    for asset in stale:
        asset.target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(asset.source, asset.target)
    return stale

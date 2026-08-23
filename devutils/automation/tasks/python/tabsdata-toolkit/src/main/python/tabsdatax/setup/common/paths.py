#
#  Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

TABSDATA_TO_ROOT = "."
CONNECTOR_TO_ROOT = os.path.join("..", "..", "..")

ROOT_FILE = ".root"

PACKAGE_FOLDER_ARGUMENT = "--package-folder"

TABSDATA_BASE_FOLDER_VARIABLE = "PROJECT_TABSDATA_BASE_FOLDER"
TABSDATA_CORE_REPOSITORIES = ("tabsdata", "tabsdata-os", "tabsdata-ee")


def package_folder(setup_file: str) -> str:
    if PACKAGE_FOLDER_ARGUMENT in sys.argv:
        position = sys.argv.index(PACKAGE_FOLDER_ARGUMENT)
        folder = sys.argv[position + 1]
        del sys.argv[position : position + 2]
        return folder
    return str(Path(setup_file).parent.absolute())


def as_tabsdata_core_repository_root(found_root: Path) -> str | None:
    declared = os.environ.get(TABSDATA_BASE_FOLDER_VARIABLE, "").strip()
    if not declared:
        return None
    if found_root.name not in TABSDATA_CORE_REPOSITORIES:
        return None
    return declared


def root_folder(setup_file: str, fallback: str = TABSDATA_TO_ROOT) -> str:
    current_folder = Path(package_folder(setup_file))
    logger.debug(f"📁 Current setup folder is: {current_folder}")
    while True:
        root_file = Path(os.path.join(current_folder, ROOT_FILE))
        root_file_exists = root_file.exists() and root_file.is_file()

        if root_file_exists:
            tabsdata_core_repository_root = as_tabsdata_core_repository_root(
                current_folder
            )
            if tabsdata_core_repository_root:
                logger.debug(
                    "🗂️ Root project folder for setup is: "
                    f"{tabsdata_core_repository_root} "
                    f"(declared by {TABSDATA_BASE_FOLDER_VARIABLE})"
                )
                return tabsdata_core_repository_root
            logger.debug(f"🗂️ Root project folder for setup is: {current_folder}")
            return str(current_folder)
        else:
            parent_folder = current_folder.parent
            if current_folder == parent_folder:
                logger.error(
                    "☢️️ "
                    "Root folder is unreachable from current setup folder! "
                    f"Defaulting to '{fallback}'"
                )
                return fallback
            current_folder = parent_folder


def read_file(*paths: str, base_path: str | None = None, encoding: str = "utf8") -> str:
    if base_path is None:
        base_path = "."

    full_path = os.path.join(base_path, *paths)

    with io.open(full_path, encoding=encoding) as open_file:
        content = open_file.read().strip()

    return content


def get_variant_manifest_folder(root: str) -> str:
    return os.path.join(
        root,
        "variant",
        "assets",
        "manifest",
    )


def get_variant_assets_folder(root: str) -> str:
    return os.path.join(
        root,
        "variant",
        "assets",
    )

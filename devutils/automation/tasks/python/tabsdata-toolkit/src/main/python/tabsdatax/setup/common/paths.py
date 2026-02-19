#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import io
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

TABSDATA_TO_ROOT = "."
CONNECTOR_TO_ROOT = os.path.join("..", "..", "..")


def root_folder(setup_file: str, fallback: str = TABSDATA_TO_ROOT) -> str:
    current_folder = Path(setup_file).parent.absolute()
    logger.debug(f"📁 Current setup folder is: {current_folder}")
    while True:
        root_file = Path(os.path.join(current_folder, ".root"))
        root_file_exists = root_file.exists() and root_file.is_file()

        if root_file_exists:
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

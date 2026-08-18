#
#  Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import os
import tomllib
from collections.abc import Mapping, Sequence

logger = logging.getLogger(__name__)

SETUP_TOML_FILE = "setup.toml"
KEY_BINARY_GROUPS = "binary_groups"
KEY_NAMES = "names"
KEY_PACKAGE = "package"
KEY_INSTALL_DEST = "install_dest"


def load_binary_groups(folder: str) -> list[Mapping]:
    setup_toml = os.path.join(folder, SETUP_TOML_FILE)
    if not os.path.isfile(setup_toml):
        logger.debug(f"No {SETUP_TOML_FILE} in {folder}; no binaries to ship.")
        return []
    with open(setup_toml, "rb") as file:
        return tomllib.load(file).get(KEY_BINARY_GROUPS, [])


def binary_names(groups: Sequence[Mapping]) -> list[str]:
    return [name for group in groups for name in group.get(KEY_NAMES, [])]


def binary_names_by_destination(
    groups: Sequence[Mapping],
    default_destination: str,
) -> dict[str, list[str]]:
    destinations: dict[str, list[str]] = {}
    for group in groups:
        destination = group.get(KEY_INSTALL_DEST, default_destination)
        destinations.setdefault(destination, []).extend(group.get(KEY_NAMES, []))
    return destinations

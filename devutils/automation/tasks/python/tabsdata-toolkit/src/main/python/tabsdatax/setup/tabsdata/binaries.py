#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
from collections.abc import Mapping, Sequence

from tabsdatax.setup.common.platform import (
    get_python_binaries_folder,
    resolve_km_project_relative,
)

logger = logging.getLogger(__name__)


def detect_binaries(
    base_binaries: Sequence[str],
    target_release_folder: str,
    require_server_binaries: bool = False,
) -> tuple[list[str], list[str]]:
    existing_binaries = [
        binary
        for base in base_binaries
        for binary in (base, f"{base}.exe")
        if os.path.exists(os.path.join(target_release_folder, binary))
    ]

    missing_binaries = [
        base
        for base in base_binaries
        if not any(
            os.path.exists(os.path.join(target_release_folder, binary))
            for binary in (base, f"{base}.exe")
        )
    ]

    if missing_binaries and require_server_binaries:
        raise FileNotFoundError(
            "The following binaries are missing in "
            f"{target_release_folder}: {', '.join(missing_binaries)}"
        )

    return existing_binaries, missing_binaries


def collect_group_datafiles(
    binary_groups: Sequence[Mapping],
    default_target_folder: str,
    require_server_binaries: bool = False,
) -> list[tuple[str, list[str]]]:
    destination = get_python_binaries_folder()
    aggregated: list[str] = []

    for group in binary_groups:
        group_name = group.get("name", "")
        names = group.get("names", [])
        source = group.get("source")
        km_subpath = group.get("km_subpath")

        if km_subpath is not None:
            km_root = resolve_km_project_relative()
            if km_root is None:
                logger.debug(
                    f"Binary group '{group_name}' km project not found; skipping."
                )
                continue
            folder = os.path.join(km_root, km_subpath)
        elif source is not None:
            folder = source
        else:
            folder = default_target_folder

        if (km_subpath is not None or source is not None) and not os.path.isdir(folder):
            logger.debug(
                f"Binary group '{group_name}' source folder '{folder}' does"
                " not exist; skipping."
            )
            continue

        existing, _missing = detect_binaries(
            names,
            folder,
            require_server_binaries,
        )
        aggregated.extend(os.path.join(folder, binary) for binary in existing)

    if not aggregated:
        return []

    datafiles = [(destination, aggregated)]
    logger.debug(f"Including tabsdata binaries: {datafiles}")
    return datafiles

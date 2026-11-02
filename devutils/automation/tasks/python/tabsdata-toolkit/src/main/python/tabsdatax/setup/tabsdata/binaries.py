#
#  Copyright 2026 Tabsdata Inc.
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
    root: str = "",
) -> list[tuple[str, list[str]]]:
    default_destination = get_python_binaries_folder()
    datafiles: list[tuple[str, list[str]]] = []

    for group in binary_groups:
        group_name = group.get("name", "")
        names = group.get("names", [])
        source = group.get("source")
        km_subpath = group.get("km_subpath")
        install_dest = group.get("install_dest", default_destination)

        if km_subpath is not None:
            km_root = resolve_km_project_relative(root)
            if km_root is None:
                if require_server_binaries:
                    raise FileNotFoundError(
                        f"Binary group '{group_name}' km project not found."
                    )
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
            if require_server_binaries:
                raise FileNotFoundError(
                    f"Binary group '{group_name}' source folder '{folder}' does"
                    " not exist."
                )
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
        if not existing:
            continue
        files = [
            os.path.relpath(os.path.join(folder, binary)).replace(os.sep, "/")
            for binary in existing
        ]
        datafiles.append((install_dest, files))

    if not datafiles:
        return []

    logger.debug(f"Including tabsdata binaries: {datafiles}")
    return datafiles


def collect_resource_datafiles(
    resource_groups: Sequence[Mapping],
    root: str,
    require_server_binaries: bool = False,
) -> list[tuple[str, list[str]]]:
    """The data files of `[[resource_groups]]`. A group missing any file is
    skipped, or fails a build that requires the server binaries."""
    datafiles: list[tuple[str, list[str]]] = []
    for group in resource_groups:
        group_name = group.get("name", "")
        folder = os.path.join(root, group["source"])
        names = group.get("names", [])
        missing = [
            name for name in names if not os.path.isfile(os.path.join(folder, name))
        ]
        if missing:
            message = (
                f"Resource group '{group_name}' lacks {', '.join(missing)} in"
                f" '{folder}'"
            )
            if require_server_binaries:
                raise FileNotFoundError(message)
            logger.debug(f"{message}; skipping.")
            continue
        files = [
            os.path.relpath(os.path.join(folder, name)).replace(os.sep, "/")
            for name in names
        ]
        datafiles.append((group["install_dest"], files))
    return datafiles

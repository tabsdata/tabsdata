#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from tabsdatax.setup.common.platform import get_python_binaries_folder

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

EDITABLE_EXTRA_PATHS = [
    "client/libraries/ta_features/src/main/python",
    "client/libraries/ta_tableframe/src/main/python",
    "extensions/python/libraries/te_examples/src/main/python",
    "extensions/python/libraries/te_tableframe/src/main/python",
    "extensions/python/libraries/te_variant/src/main/python",
    "expansions/python/modules/to-foundation/python/src/main/python",
    "expansions/polars/modules/ty-tableframe/python/src/main/python",
]


def is_editable_install() -> bool:
    return "editable_wheel" in sys.argv or "develop" in sys.argv


def create_editable_extra_pth_file(
    setup_file: str,
    extra_paths: Sequence[str] | None = None,
) -> None:
    if not is_editable_install():
        return

    if extra_paths is None:
        extra_paths = EDITABLE_EXTRA_PATHS

    try:
        import site

        site_packages_list = site.getsitepackages()
        if not site_packages_list:
            return
        site_packages = site_packages_list[0]
    except (AttributeError, IndexError):
        return

    project_root = os.path.dirname(os.path.abspath(setup_file))
    pth_file = os.path.join(site_packages, "tabsdata_extra_paths.pth",)

    pth_lines = []
    for relative_extra_path in extra_paths:
        absolute_extra_path = os.path.join(project_root, relative_extra_path,)
        if os.path.isdir(absolute_extra_path):
            pth_lines.append(absolute_extra_path)

    if pth_lines:
        try:
            with open(pth_file, "w") as f:
                f.write("\n".join(pth_lines) + "\n")
        except (OSError, IOError):
            pass


def create_editable_binary_symlinks(
    datafiles: list[tuple[str, list[str]]],
) -> None:
    if not is_editable_install():
        return

    try:
        import site

        site_packages_list = site.getsitepackages()
        if not site_packages_list:
            return
        site_packages = Path(site_packages_list[0])
        venv_bin_dir = site_packages.parent.parent.parent / get_python_binaries_folder()
    except (AttributeError, IndexError):
        return

    if not venv_bin_dir.exists():
        return

    for dest_dir, binary_paths in datafiles:
        for binary_path in binary_paths:
            binary_path = Path(binary_path)
            if not binary_path.exists():
                continue

            binary_name = binary_path.name
            symlink_path = venv_bin_dir / binary_name

            if symlink_path.is_symlink() or symlink_path.exists():
                try:
                    symlink_path.unlink()
                except OSError:
                    continue

            try:
                symlink_path.symlink_to(binary_path.resolve())
                logger.info(
                    f"Created symlink: {symlink_path} -> {binary_path.resolve()}"
                )
            except OSError as e:
                logger.warning(f"Failed to create symlink for {binary_name}: {e}")

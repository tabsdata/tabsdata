#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import sys
from collections.abc import Sequence
from pathlib import Path

logger = logging.getLogger(__name__)

EDITABLE_EXTRA_PATHS = [
    "client/libraries/ta_features/src/main/python",
    "client/libraries/ta_tableframe/src/main/python",
    "extensions/python/libraries/te_examples/src/main/python",
    "extensions/python/libraries/te_tableframe/src/main/python",
    "extensions/python/libraries/te_variant/src/main/python",
    "expansions/python/modules/to-foundation/python/src/main/python",
    "expansions/polars/modules/ty-tableframe/python/src/main/python",
    "expansions/polars/extensions/tz-tableframe/python/src/main/python",
]


def is_editable_install() -> bool:
    return "editable_wheel" in sys.argv or "develop" in sys.argv


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
        venv_root = site_packages.parent.parent.parent
    except (AttributeError, IndexError):
        return

    for dest_dir, binary_paths in datafiles:
        venv_dest_dir = venv_root / dest_dir
        try:
            venv_dest_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.warning(f"Failed to create destination {venv_dest_dir}: {e}")
            continue
        for binary_path in binary_paths:
            binary_path = Path(binary_path)
            if not binary_path.exists():
                continue

            binary_name = binary_path.name
            symlink_path = venv_dest_dir / binary_name

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


def create_editable_libexec_links(
    install_subfolder: str,
    paths: Sequence[str],
) -> None:
    if not is_editable_install():
        return
    if not paths:
        return

    try:
        import site

        site_packages_list = site.getsitepackages()
        if not site_packages_list:
            return
        venv_root = Path(site_packages_list[0]).parent.parent.parent
    except (AttributeError, IndexError):
        return

    target_dir = venv_root / install_subfolder
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.warning(f"Failed to create {target_dir}: {e}")
        return

    for source in paths:
        source_path = Path(source)
        if not source_path.exists():
            continue

        link_path = target_dir / source_path.name

        if link_path.is_symlink() or link_path.exists():
            try:
                link_path.unlink()
            except OSError:
                continue

        try:
            link_path.symlink_to(source_path.resolve())
            logger.info(f"Created symlink: {link_path} -> {source_path.resolve()}")
        except OSError as e:
            logger.warning(f"Failed to create symlink for {source_path.name}: {e}")

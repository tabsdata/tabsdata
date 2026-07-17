#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import shutil
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

    venv_root = Path(sys.prefix)

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
            except OSError:
                try:
                    shutil.copy2(binary_path.resolve(), symlink_path)
                    logger.info(
                        f"Copied binary: {binary_path.resolve()} -> {symlink_path}"
                    )
                except OSError as copy_err:
                    logger.error(
                        f"Failed to stage {binary_name} into "
                        f"{venv_dest_dir}: {copy_err}"
                    )


def create_editable_libexec_links(
    install_subfolder: str,
    paths: Sequence[str],
) -> None:
    if not is_editable_install():
        return
    if not paths:
        return

    venv_root = Path(sys.prefix)
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
        except OSError:
            try:
                shutil.copy2(source_path.resolve(), link_path)
                logger.info(f"Copied binary: {source_path.resolve()} -> {link_path}")
            except OSError as copy_err:
                logger.error(
                    f"Failed to stage {source_path.name} into {target_dir}: {copy_err}"
                )

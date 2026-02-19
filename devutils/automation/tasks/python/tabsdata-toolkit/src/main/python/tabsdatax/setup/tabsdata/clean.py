#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import shutil
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING

from setuptools import Command

from tabsdatax.setup.common.platform import get_python_binaries_folder

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def create_custom_clean_class(root: str, setup_toml_path: str) -> type:
    class CustomClean(Command):
        def initialize_options(self) -> None:
            self.all = None

        def finalize_options(self) -> None:
            pass

        def run(self) -> None:
            directories_to_clean = [
                "build",
                os.path.join(
                    "target",
                    "python",
                    "build",
                ),
                os.path.join(
                    "target",
                    "python",
                    "dist",
                ),
                os.path.join(
                    "target",
                    "python",
                    "sdist",
                ),
                os.path.join(
                    "target",
                    "python",
                    "egg",
                ),
            ]

            if self.all:
                for pattern in [
                    "*.egg-info",
                    ".eggs",
                ]:
                    for path in Path(".").glob(pattern):
                        if path.is_dir():
                            directories_to_clean.append(str(path))

            for directory in directories_to_clean:
                if os.path.exists(directory):
                    logger.info(f"Removing {directory}")
                    shutil.rmtree(directory)

            if self.all:
                self._clean_editable_artifacts()

        # noinspection PyMethodMayBeStatic
        def _clean_editable_artifacts(self) -> None:
            try:
                import site

                site_packages_list = site.getsitepackages()
                if not site_packages_list:
                    return
                site_packages = Path(site_packages_list[0])
            except (AttributeError, IndexError):
                return

            pth_file = site_packages / "tabsdata_extra_paths.pth"
            if pth_file.exists():
                try:
                    pth_file.unlink()
                    logger.info(f"Removed {pth_file}")
                except OSError as e:
                    logger.warning(f"Failed to remove {pth_file}: {e}")

            venv_bin_dir = (
                site_packages.parent.parent.parent / get_python_binaries_folder()
            )
            if not venv_bin_dir.exists():
                return

            full_setup_toml_path = os.path.join(root, setup_toml_path)
            if not os.path.exists(full_setup_toml_path):
                logger.warning(f"Setup toml not found: {full_setup_toml_path}")
                return

            with open(full_setup_toml_path, "rb") as setup_toml:
                setup_config = tomllib.load(setup_toml)
                binary_names = setup_config.get("setup", {}).get("binaries", [])

            for binary_name in binary_names:
                for name in (binary_name, f"{binary_name}.exe"):
                    symlink_path = venv_bin_dir / name
                    if symlink_path.is_symlink():
                        try:
                            symlink_path.unlink()
                            logger.info(f"Removed symlink {symlink_path}")
                        except OSError as e:
                            logger.warning(
                                f"Failed to remove symlink {symlink_path}: {e}"
                            )

    return CustomClean

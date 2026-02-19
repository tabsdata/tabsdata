#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

from setuptools.command.build_py import build_py as _build_py


logger = logging.getLogger(__name__)


def create_main_custom_build_hook(root: str):

    def run_hook(build_cmd) -> None:
        foundation_libs_path = os.path.join(
            "expansions",
            "python",
            "modules",
            "to-foundation",
            "python",
            "src",
            "main",
            "python",
            "tabsdata.libs",
        )
        if os.path.exists(foundation_libs_path):
            tabsdata_libs_folder = os.path.join(build_cmd.build_lib, "tabsdata.libs")
            logger.info(f"Copying {foundation_libs_path} to {tabsdata_libs_folder}")
            if os.path.exists(tabsdata_libs_folder):
                shutil.rmtree(tabsdata_libs_folder)
            shutil.copytree(foundation_libs_path, tabsdata_libs_folder)

        tableframe_libs_path = os.path.join(
            "expansions",
            "polars",
            "modules",
            "ty-tableframe",
            "python",
            "src",
            "main",
            "python",
            "tabsdata.libs",
        )
        if os.path.exists(tableframe_libs_path):
            tabsdata_libs_folder = os.path.join(build_cmd.build_lib, "tabsdata.libs")
            logger.info(f"Copying {tableframe_libs_path} to {tabsdata_libs_folder}")
            if os.path.exists(tabsdata_libs_folder):
                shutil.rmtree(tabsdata_libs_folder)
            shutil.copytree(tableframe_libs_path, tabsdata_libs_folder)

    return run_hook


def create_custom_build_py_class(
    package_resources: dict[Path, Path] | None = None
) -> type:
    if package_resources is None:
        package_resources = {}

    class CustomBuildPy(_build_py):
        def run(self) -> None:
            package_root = Path(self.build_lib)
            for source, target in package_resources.items():
                if not source.exists():
                    logger.warning(
                        f"External resource {source} does not exist. Skipping."
                    )
                    continue
                destination = package_root / target
                if source.is_dir():
                    shutil.copytree(
                        source,
                        destination,
                        dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns(".gitkeep"),
                    )
                elif source.is_file():
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, destination)
                else:
                    logger.warning(
                        f"External resource {source} has unsupported type. Skipping."
                    )
            super().run()

    return CustomBuildPy

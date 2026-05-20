#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Callable, Sequence

from setuptools import Distribution
from setuptools.command.bdist_egg import bdist_egg as _bdist_egg
from setuptools.command.build import build as _build
from setuptools.command.editable_wheel import editable_wheel as _editable_wheel
from setuptools.command.sdist import sdist as _sdist

try:
    from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel
except ImportError:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel


logger = logging.getLogger(__name__)


def create_custom_bdist_egg_class() -> type[_bdist_egg]:
    class CustomBDistEgg(_bdist_egg):
        def __init__(self, dist: Distribution) -> None:
            super().__init__(dist)
            self.build_base: str | None = None

        def initialize_options(self) -> None:
            super().initialize_options()
            self.dist_dir = os.path.join(
                "target",
                "python",
                "dist",
            )
            self.build_base = os.path.join(
                "target",
                "python",
                "build",
            )
            os.makedirs(self.build_base, exist_ok=True)

    return CustomBDistEgg


def create_custom_bdist_wheel_class(
    python_version_tag: str,
    python_version_abi: str,
    get_platname_func: Callable[[], str],
) -> type[_bdist_wheel]:
    class CustomBDistWheel(_bdist_wheel):
        def __init__(self, dist: Distribution) -> None:
            super().__init__(dist)
            self.dist_dir: str | None = None
            self.root_is_pure: bool | None = None

        def initialize_options(self) -> None:
            super().initialize_options()
            self.dist_dir = os.path.join(
                "target",
                "python",
                "dist",
            )

        def finalize_options(self) -> None:
            super().finalize_options()
            self.root_is_pure = False

        def get_tag(self) -> tuple[str, str, str]:
            _, _, plat = super().get_tag()
            return python_version_tag, python_version_abi, get_platname_func()

        def run(self) -> None:
            build_dir = os.path.join(
                "target",
                "python",
                "build",
            )
            if os.path.exists(build_dir):
                shutil.rmtree(build_dir)
            super().run()

    return CustomBDistWheel


def create_custom_build_class(
    run_hook: Callable[[_build], None] | None = None,
) -> type[_build]:
    class CustomBuild(_build):
        def initialize_options(self) -> None:
            super().initialize_options()
            self.build_base = os.path.join(
                "target",
                "python",
                "build",
            )
            os.makedirs(self.build_base, exist_ok=True)

        def run(self) -> None:
            super().run()
            if run_hook is not None:
                run_hook(self)

    return CustomBuild


def create_custom_sdist_class(skip_readme_check: bool = False) -> type[_sdist]:
    class CustomSDist(_sdist):
        def __init__(self, dist: Distribution) -> None:
            super().__init__(dist)
            self.temp_dir: str | None = None
            self.dist_dir: str | None = None

        def initialize_options(self) -> None:
            super().initialize_options()
            self.dist_dir = os.path.join(
                "target",
                "python",
                "dist",
            )
            self.temp_dir = os.path.join(
                "target",
                "python",
                "sdist",
            )
            os.makedirs(self.temp_dir, exist_ok=True)

        if skip_readme_check:

            def check_readme(self) -> None:
                # README lives in shared assets; skip setuptools validation warning.
                logger.debug("Skipping README validation for sdist build")
                return

    return CustomSDist


def create_custom_editable_wheel_class(
    extra_paths: Sequence[str],
    project_root: str,
) -> type[_editable_wheel]:
    root = Path(project_root).resolve()
    resolved_extras = tuple(
        str((root / relative).resolve())
        for relative in extra_paths
        if (root / relative).is_dir()
    )

    class CustomEditableWheel(_editable_wheel):
        def _select_strategy(self, name, tag, build_lib):
            strategy = super()._select_strategy(name, tag, build_lib)
            if not resolved_extras:
                return strategy
            path_entries = getattr(strategy, "path_entries", None)
            if path_entries is None:
                logger.warning(
                    f"Editable strategy {type(strategy).__name__} does not "
                    f"expose path_entries; extra editable paths will not be "
                    f"injected."
                )
                return strategy
            path_entries.extend(Path(entry) for entry in resolved_extras)
            return strategy

    return CustomEditableWheel


def ensure_egg_folder() -> None:
    os.makedirs(os.path.join("target", "python", "egg"), exist_ok=True)

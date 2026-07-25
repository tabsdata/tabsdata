#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import importlib.metadata
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import yaml
from packaging.utils import canonicalize_name

from tabsdatak.common._interpreter.python.binary import Binary, binaries_from_setup_toml
from tabsdatak.common._interpreter.python.error import (
    PackageNotInstalledError,
    UnsupportedInstallMethodError,
)
from tabsdatak.common._interpreter.python.recipe import (
    SUPPORTED_INSTALL_RECIPES,
    InstallRecipe,
    PackageRecipe,
)

FILE_PROTOCOL = "file://"
WINDOWS_URL_PREFIX = "/"
BACK_SLASH = "\\"
WHEEL_EXTENSION = ".whl"
WINDOWS_OS_NAME = "nt"

DIRECT_URL_METADATA_FILE = "direct_url.json"
DIRECT_URL_KEY_URL = "url"
DIRECT_URL_KEY_DIR_INFO = "dir_info"
DIRECT_URL_KEY_EDITABLE = "editable"

BUILD_MANIFEST_RELATIVE_PATH = "assets/manifest/BUILD"


@dataclass(frozen=True)
class Package:
    name: str
    mandatory: bool = False

    def recipe(self) -> PackageRecipe | None:
        distribution = self.distribution()
        if distribution is None:
            if self.mandatory:
                raise PackageNotInstalledError(package=self.name)
            return None
        version = distribution.version
        direct_url_text = distribution.read_text(DIRECT_URL_METADATA_FILE)
        if direct_url_text is None:
            return self.recipe_from_registry(version)
        direct_url = json.loads(direct_url_text)
        url = direct_url.get(DIRECT_URL_KEY_URL, "")
        if not url.startswith(FILE_PROTOCOL):
            return self.recipe_from_registry(version)
        path = self.path_from_url(url)
        if path.suffix == WHEEL_EXTENSION:
            return self.recipe_from_wheel(version, path)
        if direct_url.get(DIRECT_URL_KEY_DIR_INFO, {}).get(
            DIRECT_URL_KEY_EDITABLE, False
        ):
            return self.recipe_from_editable(version, path)
        return self.recipe_from_folder_frozen(version, path)

    def recipe_from_registry(self, version: str | None) -> PackageRecipe:
        return self.build_recipe(InstallRecipe.REGISTRY, version, None)

    def recipe_from_wheel(self, version: str | None, location: Path) -> PackageRecipe:
        return self.build_recipe(InstallRecipe.WHEEL, version, location)

    def recipe_from_folder_frozen(
        self, version: str | None, location: Path
    ) -> PackageRecipe:
        return self.build_recipe(InstallRecipe.FROZEN, version, location)

    def recipe_from_editable(
        self, version: str | None, location: Path
    ) -> PackageRecipe:
        return self.build_recipe(
            InstallRecipe.EDITABLE,
            version,
            location,
            binaries=binaries_from_setup_toml(location),
        )

    def build_recipe(
        self,
        recipe: InstallRecipe,
        version: str | None,
        location: Path | None,
        binaries: tuple[Binary, ...] = (),
    ) -> PackageRecipe:
        if recipe not in SUPPORTED_INSTALL_RECIPES:
            supported = ", ".join(
                sorted(item.value for item in SUPPORTED_INSTALL_RECIPES)
            )
            raise UnsupportedInstallMethodError(
                recipe=recipe.value,
                package=self.name,
                supported=supported,
            )
        return PackageRecipe(
            name=self.name,
            recipe=recipe,
            version=version,
            location=location,
            binaries=binaries,
        )

    @staticmethod
    def path_from_url(url: str) -> Path:
        stripped = url[len(FILE_PROTOCOL) :]
        if (
            os.name == WINDOWS_OS_NAME
            and stripped.startswith(WINDOWS_URL_PREFIX)
            and len(stripped) >= 3
            and stripped[1].isalpha()
            and stripped[2] == ":"
        ):
            stripped = stripped[len(WINDOWS_URL_PREFIX) :]
        if stripped.startswith(BACK_SLASH):
            stripped = stripped[len(BACK_SLASH) :]
        return Path(stripped)

    def distribution(self) -> importlib.metadata.Distribution | None:
        try:
            return importlib.metadata.distribution(canonicalize_name(self.name))
        except importlib.metadata.PackageNotFoundError:
            return None

    def manifest(self) -> dict | None:
        distribution = cast(importlib.metadata.PathDistribution, self.distribution())
        if not isinstance(distribution, importlib.metadata.PathDistribution):
            return None
        try:
            manifest_path = (
                distribution._path.parent / self.name / BUILD_MANIFEST_RELATIVE_PATH
            )
            content = yaml.safe_load(manifest_path.read_bytes().decode("utf-8"))
        except (
            AttributeError,
            FileNotFoundError,
            OSError,
            UnicodeDecodeError,
            yaml.YAMLError,
        ):
            return None
        return content if isinstance(content, dict) else None

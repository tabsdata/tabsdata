#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
from importlib.resources import as_file, files
from pathlib import Path, PurePosixPath

from tabsdatak.common._constants import (
    BACK_SLASH,
    RESOURCES,
    SLASH,
    TABSDATA_MODULE_NAME,
    TABSDATAK_MODULE_NAME,
    VARIANT,
)
from tabsdatak.server._tdruntime.python.package import Package
from tabsdatak.server._tdruntime.python.recipe import InstallRecipe

logger = logging.getLogger(__name__)


RESOURCES_LOCATION: dict[str, tuple[str, tuple[str, ...]]] = {
    TABSDATAK_MODULE_NAME: (TABSDATAK_MODULE_NAME, (VARIANT,)),
    TABSDATA_MODULE_NAME: (TABSDATA_MODULE_NAME, (VARIANT,)),
}

FALLBACK_ORDER: dict[str, tuple[str, ...]] = {
    TABSDATAK_MODULE_NAME: (TABSDATA_MODULE_NAME,),
    TABSDATA_MODULE_NAME: (),
}


def td_resource(
    resource: str,
    *,
    package: str = TABSDATAK_MODULE_NAME,
) -> Path:
    for candidate in (package, *FALLBACK_ORDER.get(package, ())):
        path = lookup_resource(resource, package=candidate)
        if path is not None:
            return path
    raise FileNotFoundError(
        f"Resource '{resource}' not found in package '{package}'"
        + (
            f" or fallbacks {FALLBACK_ORDER[package]}"
            if FALLBACK_ORDER.get(package)
            else ""
        )
    )


def lookup_resource(resource: str, *, package: str) -> Path | None:
    if package not in RESOURCES_LOCATION:
        return None
    module, editable_root_parts = RESOURCES_LOCATION[package]
    normalized_resource = resource.replace(BACK_SLASH, SLASH)
    resource_parts = PurePosixPath(normalized_resource).parts
    if resource_parts and resource_parts[0] == RESOURCES:
        source_root = editable_source_root(package)
        if source_root is not None:
            try:
                variant_path = source_root.joinpath(
                    *editable_root_parts, *resource_parts
                )
                if variant_path.exists():
                    return variant_path
            except OSError:
                pass
    try:
        traversable = files(module).joinpath(*resource_parts)
        with as_file(traversable) as path:
            if path.exists():
                return path
    except (ModuleNotFoundError, FileNotFoundError):
        return None
    return None


def editable_source_root(package: str) -> Path | None:
    recipe = Package(package).recipe()
    if recipe is None or recipe.recipe != InstallRecipe.EDITABLE:
        return None
    return recipe.location

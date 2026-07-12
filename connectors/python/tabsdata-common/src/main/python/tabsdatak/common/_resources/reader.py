#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import sys
from importlib.resources import as_file, files
from pathlib import Path, PurePosixPath

from tabsdatak.common._constants import (
    BACK_SLASH,
    RESOURCES,
    SLASH,
    SRC_MAIN,
    TABSDATA_AG_MODULE_NAME,
    TABSDATA_AGENT_PACKAGE_NAME,
    TABSDATA_AGENT_REGION,
    TABSDATA_AGENT_TDRESOURCES_SOURCE_PARTS,
    TABSDATA_MODULE_NAME,
    TABSDATA_PACKAGE_NAME,
    TABSDATA_REGION,
    TABSDATA_TDRESOURCES_SOURCE_PARTS,
    TABSDATAK_MODULE_NAME,
    TABSDATAK_REGION,
    TDRESOURCES,
    VARIANT,
)
from tabsdatak.common._interpreter.python.error import UnsupportedInstallMethodError
from tabsdatak.common._interpreter.python.package import Package
from tabsdatak.common._interpreter.python.recipe import InstallRecipe

logger = logging.getLogger(__name__)


RESOURCES_LOCATION: dict[str, tuple[str, str, dict[str, tuple[str, ...]]]] = {
    TABSDATAK_REGION: (
        TABSDATAK_MODULE_NAME,
        TABSDATA_PACKAGE_NAME,
        {
            RESOURCES: (VARIANT,),
            TDRESOURCES: TABSDATA_TDRESOURCES_SOURCE_PARTS,
        },
    ),
    TABSDATA_REGION: (
        TABSDATA_MODULE_NAME,
        TABSDATA_PACKAGE_NAME,
        {
            RESOURCES: (VARIANT,),
            TDRESOURCES: TABSDATA_TDRESOURCES_SOURCE_PARTS,
        },
    ),
    TABSDATA_AGENT_REGION: (
        TABSDATA_AG_MODULE_NAME,
        TABSDATA_AGENT_PACKAGE_NAME,
        {
            RESOURCES: SRC_MAIN,
            TDRESOURCES: TABSDATA_AGENT_TDRESOURCES_SOURCE_PARTS,
        },
    ),
}

FALLBACK_ORDER: dict[str, tuple[str, ...]] = {
    TABSDATAK_REGION: (TABSDATA_REGION,),
    TABSDATA_REGION: (),
}


def td_resource(
    resource: str,
    *,
    region: str = TABSDATAK_REGION,
) -> Path:
    for candidate in (region, *FALLBACK_ORDER.get(region, ())):
        path = lookup_resource(resource, region=candidate)
        if path is not None:
            return path
    raise FileNotFoundError(
        f"Resource '{resource}' not found in region '{region}'"
        + (
            f" or fallbacks {FALLBACK_ORDER[region]}"
            if FALLBACK_ORDER.get(region)
            else ""
        )
    )


def lookup_resource(resource: str, *, region: str) -> Path | None:
    if region not in RESOURCES_LOCATION:
        return None
    module, distribution, editable_root_parts = RESOURCES_LOCATION[region]
    normalized_resource = resource.replace(BACK_SLASH, SLASH)
    resource_parts = PurePosixPath(normalized_resource).parts
    source_parts = (
        editable_root_parts.get(resource_parts[0]) if resource_parts else None
    )
    if source_parts is not None:
        source_root = editable_source_root(distribution)
        if source_root is not None:
            try:
                source_path = source_root.joinpath(*source_parts, *resource_parts)
                if source_path.exists():
                    return source_path
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


def editable_source_root(distribution: str) -> Path | None:
    try:
        recipe = Package(distribution).recipe()
    except UnsupportedInstallMethodError:
        return None
    if recipe is None or recipe.recipe != InstallRecipe.EDITABLE:
        return None
    return recipe.location


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: td_resource <resource-path>", file=sys.stderr)
        sys.exit(2)
    try:
        print(td_resource(sys.argv[1]))
    except FileNotFoundError as error:
        print(f"{error}", file=sys.stderr)
        sys.exit(1)

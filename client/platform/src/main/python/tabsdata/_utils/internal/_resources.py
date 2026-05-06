#
# Copyright 2025 Tabs Data Inc.
#

import json
import logging
import os
from importlib.metadata import distribution
from importlib.resources import as_file, files
from pathlib import Path, PurePosixPath

from tabsdata._utils.constants import (
    TABSDATA_AGENT_MODULE_NAME as TABSDATA_AGENT_PACKAGE_NAME,
    TABSDATA_MODULE_NAME as TABSDATA_PACKAGE_NAME,
)

TABSDATA_MODULE_NAME = "tabsdata"
TABSDATA_AGENT_MODULE_NAME = "tabsdata.ag"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


DIRECT_URL_JSON = "direct_url.json"
DIR_INFO = "dir_info"
EDITABLE = "editable"

FILE_PROTOCOL = "file://"
WINDOWS_OS_NAME = "nt"
WINDOWS_URL_PREFIX = "/"
SLASH = "/"
BACK_SLASH = "\\"
WHEEL_EXTENSION = ".whl"

VARIANT = "variant"
RESOURCES = "resources"

SRC_MAIN = ("src", "main")

RESOURCES_LOCATION: dict[str, tuple[str, tuple[str, ...]]] = {
    TABSDATA_PACKAGE_NAME: (TABSDATA_MODULE_NAME, (VARIANT,)),
    TABSDATA_AGENT_PACKAGE_NAME: (TABSDATA_AGENT_MODULE_NAME, SRC_MAIN),
}


# noinspection PyBroadException
def is_editable_package(package: str) -> tuple[bool, Path | None]:
    editable: bool = False
    root: Path | None = None
    try:
        package_distribution = distribution(package)
        if package_distribution.read_text(DIRECT_URL_JSON):
            direct_url_data = json.loads(
                package_distribution.read_text(DIRECT_URL_JSON)
            )
            if direct_url_data.get(DIR_INFO, {}).get(EDITABLE, False):
                editable = True
                url = direct_url_data.get("url", "")
                if url.startswith(FILE_PROTOCOL):
                    url = url[len(FILE_PROTOCOL) :]
                    if os.name == WINDOWS_OS_NAME and url.startswith(
                        WINDOWS_URL_PREFIX
                    ):
                        url = url[len(WINDOWS_URL_PREFIX) :]
                    url = Path(url)
                    if not url.suffix == WHEEL_EXTENSION:
                        root = url
    except Exception:
        pass
    return editable, root


# noinspection PyBroadException
def td_resource(
    resource: str,
    *,
    package: str = TABSDATA_PACKAGE_NAME,
) -> Path:
    module, editable_root_parts = RESOURCES_LOCATION[package]
    normalized_resource = resource.replace(BACK_SLASH, SLASH)
    resource_parts = PurePosixPath(normalized_resource).parts
    if resource_parts and resource_parts[0] == RESOURCES:
        editable, root = is_editable_package(package)
        if editable and root:
            try:
                variant_path = root.joinpath(*editable_root_parts, *resource_parts)
                if variant_path.exists():
                    return variant_path
            except Exception:
                pass
    traversable = files(module).joinpath(*resource_parts)
    with as_file(traversable) as path:
        if not path.exists():
            raise FileNotFoundError(
                f"Resource '{resource}' not found in package '{package}'"
            )
        return path

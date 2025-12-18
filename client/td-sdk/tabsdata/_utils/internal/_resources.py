#
# Copyright 2025 Tabs Data Inc.
#

import json
import logging
import os
from importlib.metadata import distribution
from importlib.resources import as_file, files
from pathlib import Path

import tabsdata
from tabsdata._utils.constants import TABSDATA_MODULE_NAME

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


DIRECT_URL_JSON = "direct_url.json"
DIR_INFO = "dir_info"
EDITABLE = "editable"

VARIANT = "variant"
RESOURCES = "resources"


# noinspection PyBroadException
def is_editable_package(package: str) -> bool:
    try:
        package_distribution = distribution(package)
        if package_distribution.read_text(DIRECT_URL_JSON):
            direct_url = json.loads(package_distribution.read_text(DIRECT_URL_JSON))
            if direct_url.get(DIR_INFO, {}).get(EDITABLE, False):
                return True
    except Exception:
        pass
    return False


# noinspection PyBroadException
def td_resource(resource: str) -> Path:
    if resource.startswith(f"{RESOURCES}{os.sep}"):
        if is_editable_package(TABSDATA_MODULE_NAME):
            try:
                module_path = Path(tabsdata.__file__).parent
                project_root = module_path.parent.parent.parent
                variant_path = project_root / VARIANT / resource
                if variant_path.exists():
                    return variant_path
            except Exception:
                pass
    traversable = files(TABSDATA_MODULE_NAME).joinpath(resource)
    with as_file(traversable) as path:
        if not path.exists():
            raise FileNotFoundError(
                f"Resource '{resource}' not found in package '{TABSDATA_MODULE_NAME}'"
            )
        return path

#
# Copyright 2025 Tabs Data Inc.
#

import logging
from importlib.resources import as_file, files
from pathlib import Path

from tabsdata._utils.constants import TABSDATA_MODULE_NAME

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def td_resource(resource: str) -> Path:
    traversable = files(TABSDATA_MODULE_NAME).joinpath(resource)
    with as_file(traversable) as path:
        if not path.exists():
            raise FileNotFoundError(
                f"Resource '{resource}' not found in package '{TABSDATA_MODULE_NAME}'"
            )
        return path

#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
from importlib.metadata import version
from pathlib import Path

from packaging.version import Version

from tabsdata._utils.constants import TABSDATA_MODULE_NAME

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DEFAULT_TABSDATA_FOLDER = os.path.join(os.path.expanduser("~"), ".tabsdata")
DEFAULT_ENVIRONMENT_FOLDER = os.path.join(DEFAULT_TABSDATA_FOLDER, "environments")
DEFAULT_INSTANCES_FOLDER = os.path.join(DEFAULT_TABSDATA_FOLDER, "instances")
DEFAULT_INSTANCE = os.path.join(DEFAULT_INSTANCES_FOLDER, "tabsdata")
VERSION_FILE = ".version"
WORKSPACE_FOLDER = "workspace"
WORK_FOLDER = "work"
LOCK_FOLDER = "lock"
LOG_FOLDER = "log"


def get_version() -> Version:
    return Version(version(TABSDATA_MODULE_NAME))


def get_instance_path(instance: str) -> Path:
    instance = instance or DEFAULT_INSTANCE
    instance_path = Path(instance)
    if instance_path.is_absolute():
        pass
    elif os.sep not in instance and (os.altsep is None or os.altsep not in instance):
        instance_path = Path(os.path.join(DEFAULT_INSTANCES_FOLDER, instance))
    else:
        message = (
            f"Invalid instance: '{instance_path}'. "
            "It is neither an absolute path nor a single name."
        )
        logger.error(message)
        raise ValueError(message)
    if not instance_path.exists():
        message = f"Invalid instance: '{instance_path}'. Instance path does not exist."
        logger.error(message)
        raise ValueError(message)
    if not instance_path.is_dir():
        message = f"Invalid instance: '{instance_path}'. Instance path is not a folder."
        logger.error(message)
        raise ValueError(message)
    return instance_path

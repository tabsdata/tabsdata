#
# Copyright 2025 Tabs Data Inc.
#
import os
import tempfile
from pathlib import Path

TABSDATA_MODULE_NAME = "tabsdata"
TD_TABSDATA_DEV_PKG = "TD_TABSDATA_DEV_PKG"

TABSDATA_CONNECTORS_NAMES = [
    "databricks",
    "mongodb",
    "mssql",
    "salesforce",
    "snowflake",
]

TABSDATA_CONNECTORS = {
    f"tabsdata_{name}": {"is_dev_env": f"TD_TABSDATA_{name.upper()}_DEV_PKG"}
    for name in TABSDATA_CONNECTORS_NAMES
}

TABSDATA_PACKAGES = [TABSDATA_MODULE_NAME] + list(TABSDATA_CONNECTORS.keys())

TRUE_VALUES = {"1", "true", "yes", "y", "on"}

NO_VERSION = "♾️"

TD_TEMP = "td-temp"

TABSDATA_TEMP_ROOT = os.path.join(tempfile.gettempdir(), TD_TEMP)
TABSDATA_TEMP_ROOT_PATH = Path(TABSDATA_TEMP_ROOT)


def tabsdata_temp_folder() -> str:
    if not TABSDATA_TEMP_ROOT_PATH.exists():
        os.makedirs(TABSDATA_TEMP_ROOT, exist_ok=True)
    return TABSDATA_TEMP_ROOT


def env_enabled(env: str) -> bool:
    return os.getenv(env, "False").lower() in TRUE_VALUES

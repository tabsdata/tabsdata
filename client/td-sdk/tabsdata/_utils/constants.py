#
# Copyright 2025 Tabs Data Inc.
#

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

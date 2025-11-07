#
# Copyright 2025 Tabs Data Inc.
#

TABSDATA_MODULE_NAME = "tabsdata"
POLARS_MODULE_NAME = "polars"

TD_TABSDATA_DEV_PKG = "TD_TABSDATA_DEV_PKG"

TABSDATA_CONNECTORS_NAMES = [
    "bigquery",
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

TABSDATA_AGENT_MODULE_NAME = "tabsdata_agent"
TD_TABSDATA_AGENT_DEV_PKG = "TD_TABSDATA_AGENT_DEV_PKG"

TABSDATA_PACKAGES = (
    [TABSDATA_MODULE_NAME]
    + list(TABSDATA_CONNECTORS.keys())
    + [TABSDATA_AGENT_MODULE_NAME]
)

TRUE_VALUES = {"1", "true", "yes", "y", "on"}

NO_VERSION = "♾️"

COPYRIGHT_HEADER = "#\n# Copyright {} Tabs Data Inc.\n#\n\n"

INTERNAL_ERROR_MESSAGE = (
    "Internal error: {}. Contact Tabsdata for help and to report it."
)

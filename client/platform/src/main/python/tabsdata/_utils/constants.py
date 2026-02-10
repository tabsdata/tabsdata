#
# Copyright 2025 Tabs Data Inc.
#

TABSDATA_MODULE_NAME = "tabsdata"
POLARS_MODULE_NAME = "polars"

TD_TABSDATA_DEV_PKG = "TD_TABSDATA_DEV_PKG"

PYTHON_EDITABLE_PACKAGE_PREFIX = "__editable___"
PYTHON_EDITABLE_PACKAGE_SUFFIX = "_finder"

TABSDATA_CONNECTORS_NAMES = [
    "bigquery",
    "databricks",
    "kafka",
    "mongodb",
    "mssql",
    "salesforce",
    "snowflake",
]

TABSDATA_CONNECTORS = {
    f"tabsdata-{name}": {"is_dev_env": f"TD_TABSDATA_{name.upper()}_DEV_PKG"}
    for name in TABSDATA_CONNECTORS_NAMES
}

TABSDATA_AGENT_MODULE_NAME = "tabsdata-agent"
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

BUILD_DEPENDENCIES = [
    "archspec==0.2.5",
    "colorama==0.4.6",
    "psutil==7.2.2",
    "py-cpuinfo==9.0.0",
    "PyYAML==6.0.3",
    "setuptools==78.1.1",
    "tqdm==4.67.3",
    "tzlocal==5.3.1",
    "wheel",
]

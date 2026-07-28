#
# Copyright 2025 Tabsdata Inc.
#

TABSDATA_MODULE_NAME = "tabsdata"
POLARS_MODULE_NAME = "polars"

TD_TABSDATA_DEV_PKG = "TD_TABSDATA_DEV_PKG"

PYTHON_EDITABLE_PACKAGE_PREFIX = "__editable___"
PYTHON_EDITABLE_PACKAGE_SUFFIX = "_finder"

TABSDATA_CONNECTORS_NAMES = [
    "api",
    "cli",
    "common",
    "conn-azureblob",
    "conn-bigquery",
    "conn-clickhouse",
    "conn-common",
    "conn-common-file",
    "conn-common-sql",
    "conn-databricks",
    "conn-db2",
    "conn-dev",
    "conn-druid",
    "conn-gcs",
    "conn-kafka",
    "conn-localfile",
    "conn-mongodb",
    "conn-mssql",
    "conn-mysql",
    "conn-oracle",
    "conn-postgres",
    "conn-s3",
    "conn-salesforce",
    "conn-snowflake",
    "conn-starrocks",
    "conn-stream-stage",
    "medallion",
    "sdk",
    "server",
]

TABSDATA_CONNECTORS = {
    f"tabsdata-{name}": {
        "is_dev_env": f"TD_TABSDATA_{name.upper().replace('-', '_')}_DEV_PKG"
    }
    for name in TABSDATA_CONNECTORS_NAMES
}

TABSDATA_TOOLKIT_PACKAGE_NAME = "tabsdata-toolkit"
TD_TABSDATA_TOOLKIT_DEV_PKG = "TD_TABSDATA_TOOLKIT_DEV_PKG"

TABSDATA_AGENT_PACKAGE_NAME = "tabsdata-agent"
TD_TABSDATA_AGENT_DEV_PKG = "TD_TABSDATA_AGENT_DEV_PKG"

TABSDATA_PACKAGES = (
    [TABSDATA_MODULE_NAME]
    + [TABSDATA_TOOLKIT_PACKAGE_NAME]
    + list(TABSDATA_CONNECTORS.keys())
    + [TABSDATA_AGENT_PACKAGE_NAME]
)

TRUE_1 = "1"
TRUE_TRUE = "true"
TRUE_YES = "yes"
TRUE_Y = "y"
TRUE_ON = "on"

FALSE_0 = "0"
FALSE_FALSE = "false"
FALSE_NO = "no"
FALSE_N = "n"
FALSE_OFF = "off"

TRUE_VALUES = {TRUE_1, TRUE_TRUE, TRUE_YES, TRUE_Y, TRUE_ON}
FALSE_VALUES = {FALSE_0, FALSE_FALSE, FALSE_NO, FALSE_N, FALSE_OFF}

NO_VERSION = "♾️"

COPYRIGHT_HEADER = "#\n# Copyright {} Tabsdata Inc.\n#\n\n"

INTERNAL_ERROR_MESSAGE = (
    "Internal error: {}. Contact Tabsdata for help and to report it."
)

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

# --- Filesystem / URL helpers ---------------------------------------------

FILE_PROTOCOL = "file://"
WINDOWS_OS_NAME = "nt"
WINDOWS_URL_PREFIX = "/"
SLASH = "/"
BACK_SLASH = "\\"
WHEEL_EXTENSION = ".whl"

VARIANT = "variant"
RESOURCES = "resources"
TDRESOURCES = "tdresources"

SRC_MAIN = ("src", "main")
PLATFORM_SRC_MAIN = ("client", "platform", "src", "main")

TABSDATA_TDRESOURCES_SOURCE_PARTS = PLATFORM_SRC_MAIN + (RESOURCES,)
TABSDATA_AGENT_TDRESOURCES_SOURCE_PARTS = SRC_MAIN + (RESOURCES,)

# --- Package / module names -----------------------------------------------

TABSDATA_MODULE_NAME = "tabsdata"
TABSDATAK_MODULE_NAME = "tabsdatak"
TABSDATA_AG_MODULE_NAME = "tabsdata.ag"
POLARS_MODULE_NAME = "polars"

TABSDATA_REGION = "tabsdata"
TABSDATAK_REGION = "tabsdatak"
TABSDATA_AGENT_REGION = "tabsdata-agent"

TD_TABSDATA_DEV_PKG = "TD_TABSDATA_DEV_PKG"

PYTHON_EDITABLE_PACKAGE_PREFIX = "__editable___"
PYTHON_EDITABLE_PACKAGE_SUFFIX = "_finder"

TABSDATA_PACKAGE_NAME = "tabsdata"

TABSDATA_TOOLKIT_PACKAGE_NAME = "tabsdata-toolkit"
TD_TABSDATA_TOOLKIT_DEV_PKG = "TD_TABSDATA_TOOLKIT_DEV_PKG"

TABSDATA_AGENT_PACKAGE_NAME = "tabsdata-agent"
TD_TABSDATA_AGENT_DEV_PKG = "TD_TABSDATA_AGENT_DEV_PKG"

# --- Truthy / falsy env-var values ----------------------------------------

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

# --- Misc -----------------------------------------------------------------

NO_VERSION = "♾️"

COPYRIGHT_HEADER = "#\n# Copyright {} Tabsdata Inc.\n#\n\n"

INTERNAL_ERROR_MESSAGE = (
    "Internal error: {}. Contact Tabsdata for help and to report it."
)

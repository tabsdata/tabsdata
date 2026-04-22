#
# Copyright 2025 Tabs Data Inc.
#

import importlib.metadata

from tabsdata.connector.salesforce._connection import SalesforceConn
from tabsdata.connector.salesforce._connector import SalesforceSource
from tabsdata.connector.salesforce.input.salesforce_input import SalesforceSrc
from tabsdata.connector.salesforce.input.salesforce_report_input import (
    SalesforceReportSrc,
)

__all__ = [
    "SalesforceConn",
    "SalesforceReportSrc",
    "SalesforceSrc",
    "SalesforceSource",
]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-salesforce")
except Exception:
    __version__ = "unknown"

version = __version__

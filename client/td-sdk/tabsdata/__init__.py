#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import logging

from polars import (
    Boolean,
    Date,
    Datetime,
    Duration,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Null,
    String,
    Time,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
)

from tabsdata.api.tabsdata_server import (
    Collection,
    Commit,
    DataVersion,
    ExecutionPlan,
    TabsdataServer,
    User,
)
from tabsdata.credentials import (
    AzureAccountKeyCredentials,
    S3AccessKeyCredentials,
    UserPasswordCredentials,
)
from tabsdata.decorators import ALL_DEPS, publisher, subscriber, transformer
from tabsdata.format import CSVFormat, LogFormat, NDJSONFormat, ParquetFormat
from tabsdata.plugin import DestinationPlugin, SourcePlugin
from tabsdata.secret import DirectSecret, EnvironmentSecret, HashiCorpSecret
from tabsdata.tableframe.expr.expr import Expr as Expr
from tabsdata.tableframe.functions.col import col as col
from tabsdata.tableframe.functions.eager import concat
from tabsdata.tableframe.functions.lit import lit
from tabsdata.tableframe.lazyframe.frame import TableFrame
from tabsdata.tableuri import TableURI
from tabsdata.tabsdatafunction import (
    AzureDestination,
    AzureSource,
    LocalFileDestination,
    LocalFileSource,
    MariaDBDestination,
    MariaDBSource,
    MySQLDestination,
    MySQLSource,
    OracleDestination,
    OracleSource,
    PostgresDestination,
    PostgresSource,
    S3Destination,
    S3Source,
    TableInput,
    TableOutput,
    TabsdataFunction,
)

logging.basicConfig(
    level=logging.getLevelName(logging.WARNING),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

__all__ = [
    # from tabsdatafunction.py
    "TabsdataFunction",
    # from tabsdatafunction.py Inputs
    "AzureSource",
    "LocalFileSource",
    "MariaDBSource",
    "MySQLSource",
    "OracleSource",
    "PostgresSource",
    "S3Source",
    "TableInput",
    # from tabsdatafunction.py Outputs
    "AzureDestination",
    "LocalFileDestination",
    "MariaDBDestination",
    "MySQLDestination",
    "OracleDestination",
    "PostgresDestination",
    "S3Destination",
    "TableOutput",
    # from plugin.py
    "SourcePlugin",
    "DestinationPlugin",
    # from decorators.py
    "ALL_DEPS",
    "publisher",
    "subscriber",
    "transformer",
    # from format.py
    "CSVFormat",
    "LogFormat",
    "NDJSONFormat",
    "ParquetFormat",
    # from credentials.py
    "AzureAccountKeyCredentials",
    "S3AccessKeyCredentials",
    "UserPasswordCredentials",
    # from secret.py
    "DirectSecret",
    "EnvironmentSecret",
    "HashiCorpSecret",
    # from uri.py
    "TableURI",
    # from tabsdata_server.py
    "Collection",
    "Commit",
    "DataVersion",
    "ExecutionPlan",
    "TabsdataServer",
    "User",
    # from tableframe....
    "col",
    "concat",
    "lit",
    "Expr",
    "TableFrame",
    # from polars"...
    Boolean,
    Date,
    Datetime,
    Duration,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Null,
    String,
    Time,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
]

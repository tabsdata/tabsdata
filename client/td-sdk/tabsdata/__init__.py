#
# Copyright 2024 Tabs Data Inc.
#

import pkgutil

# noinspection PyUnboundLocalVariable
__path__ = pkgutil.extend_path(__path__, __name__)

# from __future__ import annotations

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

from tabsdata.credentials import (
    AzureAccountKeyCredentials,
    S3AccessKeyCredentials,
    UserPasswordCredentials,
)
from tabsdata.decorators import ALL_DEPS, publisher, subscriber, transformer
from tabsdata.format import CSVFormat, LogFormat, NDJSONFormat, ParquetFormat
from tabsdata.io.input import (
    AzureSource,
    LocalFileSource,
    MariaDBSource,
    MySQLSource,
    OracleSource,
    PostgresSource,
    S3Source,
    TableInput,
)
from tabsdata.io.output import (
    AWSGlue,
    AzureDestination,
    LocalFileDestination,
    MariaDBDestination,
    MySQLDestination,
    OracleDestination,
    PostgresDestination,
    S3Destination,
    TableOutput,
)
from tabsdata.io.plugin import DestinationPlugin, SourcePlugin
from tabsdata.secret import EnvironmentSecret, HashiCorpSecret
from tabsdata.tableframe.functions.col import col as col
from tabsdata.tableframe.functions.eager import concat
from tabsdata.tableframe.functions.lit import lit
from tabsdata.tableframe.lazyframe.frame import TableFrame
from tabsdata.tabsserver.function.execution_exceptions import CustomException
from tabsdata_databricks.connector import DatabricksDestination
from tabsdata_mongodb.connector import MongoDBDestination
from tabsdata_salesforce.connector import SalesforceSource
from tabsdata_snowflake.connector import SnowflakeDestination

logging.basicConfig(
    level=logging.getLevelName(logging.WARNING),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

__all__ = [
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
    "AWSGlue",
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
    "EnvironmentSecret",
    "HashiCorpSecret",
    # from tabsserver.function.execution_exceptions.py
    "CustomException",
    # from tableframe....
    "col",
    "concat",
    "lit",
    "TableFrame",
    # from polars...
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
    # From tabsdata_databricks.connector
    "DatabricksDestination",
    # From tabsdata_mongodb.connector
    "MongoDBDestination",
    # From tabsdata_salesforce.connector
    "SalesforceSource",
    # From tabsdata_snowflake.connector
    "SnowflakeDestination",
]

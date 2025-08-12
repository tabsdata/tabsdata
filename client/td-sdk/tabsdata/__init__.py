import pkgutil

# noinspection PyUnboundLocalVariable
__path__ = pkgutil.extend_path(__path__, __name__)

# The lines above must appear at the top of this file to ensure
# PyCharm correctly recognizes namespace packages.

#
# Copyright 2025 Tabs Data Inc.
#

import importlib.metadata
import logging

from polars import (
    Boolean,
    Categorical,
    Date,
    Datetime,
    Decimal,
    Duration,
    Enum,
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
from polars.datatypes.classes import (
    FloatType,
    IntegerType,
    NumericType,
    SignedIntegerType,
    TemporalType,
    UnsignedIntegerType,
)
from polars.datatypes.group import (
    FLOAT_DTYPES,
    INTEGER_DTYPES,
    NUMERIC_DTYPES,
    SIGNED_INTEGER_DTYPES,
    TEMPORAL_DTYPES,
    UNSIGNED_INTEGER_DTYPES,
)

from tabsdata._credentials import (
    AzureAccountKeyCredentials,
    S3AccessKeyCredentials,
    UserPasswordCredentials,
)
from tabsdata._decorators import ALL_DEPS, publisher, subscriber, transformer
from tabsdata._format import CSVFormat, LogFormat, NDJSONFormat, ParquetFormat
from tabsdata._io.inputs.file_inputs import AzureSource, LocalFileSource, S3Source
from tabsdata._io.inputs.sql_inputs import (
    MariaDBSource,
    MySQLSource,
    OracleSource,
    PostgresSource,
)
from tabsdata._io.inputs.table_inputs import TableInput
from tabsdata._io.outputs.file_outputs import (
    AWSGlue,
    AzureDestination,
    LocalFileDestination,
    S3Destination,
)
from tabsdata._io.outputs.sql_outputs import (
    MariaDBDestination,
    MySQLDestination,
    OracleDestination,
    PostgresDestination,
)
from tabsdata._io.outputs.table_outputs import TableOutput
from tabsdata._io.plugin import DestinationPlugin, SourcePlugin
from tabsdata._secret import EnvironmentSecret, HashiCorpSecret
from tabsdata._tabsdatafunction import TabsdataFunction
from tabsdata._tabsserver.function.execution_exceptions import CustomException
from tabsdata.tableframe.functions.col import col as col
from tabsdata.tableframe.functions.eager import concat
from tabsdata.tableframe.functions.lit import lit
from tabsdata.tableframe.lazyframe.frame import TableFrame
from tabsdata_databricks._connector import DatabricksDestination
from tabsdata_mongodb._connector import MongoDBDestination
from tabsdata_mssql._connector import MSSQLDestination, MSSQLSource
from tabsdata_salesforce._connector import SalesforceSource
from tabsdata_snowflake._connector import SnowflakeDestination

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
    # from polars (basic)...
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
    # from polars (advanced)...
    FLOAT_DTYPES,
    INTEGER_DTYPES,
    NUMERIC_DTYPES,
    SIGNED_INTEGER_DTYPES,
    TEMPORAL_DTYPES,
    UNSIGNED_INTEGER_DTYPES,
    NumericType,
    IntegerType,
    SignedIntegerType,
    UnsignedIntegerType,
    FloatType,
    TemporalType,
    # NestedType,
    # ObjectType,
    Decimal,
    # Binary,
    Categorical,
    Enum,
    # Object,
    # Unknown,
    # List,
    # Array,
    # Field,
    # Struct,
    # From tabsdata_databricks.connector
    "DatabricksDestination",
    # From tabsdata_mongodb.connector
    "MongoDBDestination",
    # From tabsdata_mssql.connector
    "MSSQLDestination",
    "MSSQLSource",
    # From tabsdata_salesforce.connector
    "SalesforceSource",
    # From tabsdata_snowflake.connector
    "SnowflakeDestination",
    # From tabsdata._tabsdatafunction
    "TabsdataFunction",
]


# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata")
except Exception:
    __version__ = "unknown"

version = __version__

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

from tabsdata import typing
from tabsdata._credentials import (
    AzureAccountKeyCredentials,
    GCPServiceAccountKeyCredentials,
    S3AccessKeyCredentials,
    UserPasswordCredentials,
)
from tabsdata._cron_trigger import CronTrigger
from tabsdata._decorators import ALL_DEPS, publisher, subscriber, transformer
from tabsdata._format import (
    AvroFormat,
    CSVFormat,
    LogFormat,
    NDJSONFormat,
    ParquetFormat,
)
from tabsdata._io.connections.connections import Conn
from tabsdata._io.connections.mariadb_connection import MariaDBConn
from tabsdata._io.connections.mysql_connection import MySQLConn
from tabsdata._io.connections.oracle_connection import OracleConn
from tabsdata._io.connections.postgres_connection import PostgresConn
from tabsdata._io.inputs.file_inputs import (
    AzureSource,
    GCSSource,
    LocalFileSource,
    S3Source,
)
from tabsdata._io.inputs.sql.mariadb_input import MariaDBSrc
from tabsdata._io.inputs.sql.mysql_input import (
    MySQLSrc,
)
from tabsdata._io.inputs.sql.oracle_input import OracleSrc
from tabsdata._io.inputs.sql.postgres_input import PostgresSrc
from tabsdata._io.inputs.sql_inputs import (
    MariaDBSource,
    MySQLSource,
    OracleSource,
    PostgresSource,
)
from tabsdata._io.inputs.stage_input import Stage

# noinspection PyProtectedMember
from tabsdata._io.inputs.table_inputs import TableInput

# noinspection PyProtectedMember
from tabsdata._io.outputs.file_outputs import (
    AWSGlue,
    AzureDestination,
    GCSDestination,
    LocalFileDestination,
    S3Destination,
)
from tabsdata._io.outputs.sql.mariadb_output import MariaDBDest
from tabsdata._io.outputs.sql.mysql_output import MySQLDest
from tabsdata._io.outputs.sql.postgres_output import PostgresDest

# noinspection PyProtectedMember
from tabsdata._io.outputs.sql_outputs import (
    MariaDBDestination,
    MySQLDestination,
    OracleDestination,
    PostgresDestination,
)

# noinspection PyProtectedMember
from tabsdata._io.outputs.table_outputs import TableOutput

# noinspection PyProtectedMember
from tabsdata._io.plugin import DestinationPlugin, SourcePlugin
from tabsdata._io.stage_trigger import StageTrigger
from tabsdata._secret import EnvironmentSecret, HashiCorpSecret

# noinspection PyProtectedMember
from tabsdata._tabsserver.function.execution_exceptions import CustomException
from tabsdata._tdfunction import TabsdataFunction
from tabsdata.tableframe.dtypes import (
    FLOAT_DTYPES,
    INTEGER_DTYPES,
    NUMERIC_DTYPES,
    SIGNED_INTEGER_DTYPES,
    TEMPORAL_DTYPES,
    UNSIGNED_INTEGER_DTYPES,
    Boolean,
    Categorical,
    Date,
    Datetime,
    Decimal,
    Duration,
    Enum,
    Float32,
    Float64,
    FloatType,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    IntegerType,
    Null,
    NumericType,
    SignedIntegerType,
    String,
    TemporalType,
    Time,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UnsignedIntegerType,
    Utf8,
)
from tabsdata.tableframe.functions.col import col as col
from tabsdata.tableframe.functions.eager import concat
from tabsdata.tableframe.functions.lit import lit
from tabsdata.tableframe.lazyframe.frame import TableFrame

# noinspection PyProtectedMember
from tabsdata_bigquery._connector import BigQueryConn, BigQueryDest

# noinspection PyProtectedMember
from tabsdata_databricks._connector import DatabricksDestination
from tabsdata_kafka._connector import (
    AwsGlueSchemaRegistry,
    ConfluentSchemaRegistry,
    KafkaConn,
    KafkaTrigger,
)
from tabsdata_kafka.shared_types import (
    AwsGlueSchemaRegistryCredentialsSpec,
    ConfluentSchemaRegistryCredentialsSpec,
    KafkaCredentialsSpec,
)

# noinspection PyProtectedMember
from tabsdata_mongodb._connector import MongoDBDestination

# noinspection PyProtectedMember
from tabsdata_mssql._connector import MSSQLDestination, MSSQLSource
from tabsdata_mssql.connection.mssql_connection import MSSQLConn
from tabsdata_mssql.input.mssql_input import MSSQLSrc
from tabsdata_salesforce._connector import (
    SalesforceReportSource,
    SalesforceSource,
    SalesforceTokenCredentials,
)
from tabsdata_snowflake._connector import SnowflakeDestination

logging.basicConfig(
    level=logging.getLevelName(logging.WARNING),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

__all__ = [
    # Generics
    "SourcePlugin",
    "DestinationPlugin",
    "Stage",
    "Conn",
    "StageTrigger",
    # Decorators
    "publisher",
    "transformer",
    "subscriber",
    "TabsdataFunction",
    # Inputs
    "TableInput",
    # Sources
    "AzureSource",
    "GCSSource",
    "LocalFileSource",
    "MariaDBSource",
    "MariaDBSrc",
    "MSSQLSource",
    "MSSQLSrc",
    "MySQLSource",
    "MySQLSrc",
    "OracleSource",
    "OracleSrc",
    "PostgresSource",
    "PostgresSrc",
    "S3Source",
    "SalesforceReportSource",
    "SalesforceSource",
    # Outputs
    "TableOutput",
    # Destinations
    "AWSGlue",
    "AzureDestination",
    "BigQueryDest",
    "DatabricksDestination",
    "GCSDestination",
    "LocalFileDestination",
    "MariaDBDest",
    "MariaDBDestination",
    "MongoDBDestination",
    "MSSQLDestination",
    "MySQLDest",
    "MySQLDestination",
    "OracleDestination",
    "PostgresDest",
    "PostgresDestination",
    "S3Destination",
    "SnowflakeDestination",
    # Triggers
    "CronTrigger",
    "KafkaTrigger",
    # Connections
    "BigQueryConn",
    "KafkaConn",
    "MariaDBConn",
    "MSSQLConn",
    "MySQLConn",
    "OracleConn",
    "PostgresConn",
    # Credentials
    "AwsGlueSchemaRegistryCredentialsSpec",
    "KafkaCredentialsSpec",
    "ConfluentSchemaRegistryCredentialsSpec",
    "AzureAccountKeyCredentials",
    "GCPServiceAccountKeyCredentials",
    "S3AccessKeyCredentials",
    "SalesforceTokenCredentials",
    "UserPasswordCredentials",
    # Secrets
    "EnvironmentSecret",
    "HashiCorpSecret",
    # Formats
    "AvroFormat",
    "CSVFormat",
    "LogFormat",
    "NDJSONFormat",
    "ParquetFormat",
    # Support
    "AwsGlueSchemaRegistry",
    "ConfluentSchemaRegistry",
    # Keywords
    "ALL_DEPS",
    # TableFrame
    "TableFrame",
    "col",
    "concat",
    "lit",
    # Types
    "typing",
    "Boolean",
    "Date",
    "Datetime",
    "Duration",
    "Float32",
    "Float64",
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Int128",
    "Null",
    "String",
    "Time",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Utf8",
    "FLOAT_DTYPES",
    "INTEGER_DTYPES",
    "NUMERIC_DTYPES",
    "SIGNED_INTEGER_DTYPES",
    "TEMPORAL_DTYPES",
    "UNSIGNED_INTEGER_DTYPES",
    "NumericType",
    "IntegerType",
    "SignedIntegerType",
    "UnsignedIntegerType",
    "FloatType",
    "TemporalType",
    "Decimal",
    "Categorical",
    "Enum",
    # Exceptions
    "CustomException",
]


# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata")
except Exception:
    __version__ = "unknown"

version = __version__

import pkgutil

# noinspection PyUnboundLocalVariable
__path__ = pkgutil.extend_path(__path__, __name__)

# The lines above must appear at the top of this file to ensure
# PyCharm correctly recognizes namespace packages.

#
# Copyright 2025 Tabs Data Inc.
#

# This must be done early, as other imports might be importing packages that require
# having some system libraries already loaded.
import tabsdata._utils.internal._libraries  # noqa: F401, E402  # isort: skip

import importlib.metadata
import logging

from tabsdata import typing
from tabsdata._credentials import (
    AzureAccountKeyCredentials,
    GCPServiceAccountKeyCredentials,
    HMACCredentials,
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
from tabsdata._io.cloud_staging.common.storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)
from tabsdata._io.connections.clickhouse_connection import ClickHouseConn
from tabsdata._io.connections.connections import Conn
from tabsdata._io.connections.db2_connection import Db2Conn
from tabsdata._io.connections.druid_connection import DruidConn
from tabsdata._io.connections.mariadb_connection import MariaDBConn
from tabsdata._io.connections.mysql_connection import MySQLConn
from tabsdata._io.connections.oracle_connection import OracleConn
from tabsdata._io.connections.postgres_connection import PostgresConn
from tabsdata._io.connections.starrocks_connection import StarRocksConn
from tabsdata._io.inputs.file_inputs import (
    AzureSource,
    GCSSource,
    LocalFileSource,
    S3Source,
)
from tabsdata._io.inputs.sql.db2_input import Db2Src
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
from tabsdata._io.outputs.sql.clickhouse_output import ClickHouseDest
from tabsdata._io.outputs.sql.druid_output import DruidDest
from tabsdata._io.outputs.sql.mariadb_output import MariaDBDest
from tabsdata._io.outputs.sql.mysql_output import MySQLDest
from tabsdata._io.outputs.sql.postgres_output import PostgresDest
from tabsdata._io.outputs.sql.starrocks_output import StarRocksDest

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

# noinspection PyProtectedMember
from tabsdata.connector.bigquery._connector import BigQueryConn, BigQueryDest
from tabsdata.connector.cdc.common import CdcConn, CdcTrigger
from tabsdata.connector.cdc.db2._connection import Db2CdcConn
from tabsdata.connector.cdc.db2._connector import Db2CdcTrigger
from tabsdata.connector.cdc.mysql._connection import MySQLCdcConn
from tabsdata.connector.cdc.mysql._connector import MySQLCdcTrigger
from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn
from tabsdata.connector.cdc.postgres._connector import PostgresCdcTrigger

# noinspection PyProtectedMember
from tabsdata.connector.databricks._connector import (
    DatabricksDestination,
    DatabricksSource,
)
from tabsdata.connector.kafka._connector import (
    AwsGlueSchemaRegistry,
    ConfluentSchemaRegistry,
    KafkaConn,
    KafkaTrigger,
)
from tabsdata.connector.kafka.typing import (
    AwsGlueSchemaRegistryCredentialsSpec,
    ConfluentSchemaRegistryCredentialsSpec,
    KafkaCredentialsSpec,
)

# noinspection PyProtectedMember
from tabsdata.connector.mongodb._connector import MongoDBDestination

# noinspection PyProtectedMember
from tabsdata.connector.mssql._connector import MSSQLDestination, MSSQLSource
from tabsdata.connector.mssql.connection.mssql_connection import MSSQLConn
from tabsdata.connector.mssql.input.mssql_input import MSSQLSrc
from tabsdata.connector.salesforce._connector import (
    SalesforceReportSource,
    SalesforceSource,
    SalesforceTokenCredentials,
)
from tabsdata.connector.snowflake._connector import SnowflakeDestination
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
    Float16,
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
    "CdcConn",
    "StageTrigger",
    "CdcTrigger",
    # Decorators
    "publisher",
    "transformer",
    "subscriber",
    "TabsdataFunction",
    # Inputs
    "TableInput",
    # Sources
    "AzureSource",
    "DatabricksSource",
    "Db2Src",
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
    "ClickHouseDest",
    "DruidDest",
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
    "StarRocksDest",
    # Triggers
    "CronTrigger",
    "Db2CdcTrigger",
    "KafkaTrigger",
    "MySQLCdcTrigger",
    "PostgresCdcTrigger",
    # Connections
    "BigQueryConn",
    "ClickHouseConn",
    "Db2Conn",
    "Db2CdcConn",
    "DruidConn",
    "KafkaConn",
    "MariaDBConn",
    "MSSQLConn",
    "MySQLCdcConn",
    "MySQLConn",
    "OracleConn",
    "PostgresConn",
    "PostgresCdcConn",
    "StarRocksConn",
    # Cloud staging configs
    "AzureConfig",
    "GCSConfig",
    "S3Config",
    # Credentials
    "AwsGlueSchemaRegistryCredentialsSpec",
    "KafkaCredentialsSpec",
    "ConfluentSchemaRegistryCredentialsSpec",
    "AzureAccountKeyCredentials",
    "GCPServiceAccountKeyCredentials",
    "HMACCredentials",
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
    "Float16",
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

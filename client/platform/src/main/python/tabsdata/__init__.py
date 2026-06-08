# ruff: noqa: E402
import pkgutil

# noinspection PyUnboundLocalVariable
__path__ = pkgutil.extend_path(__path__, __name__)

# The lines above must appear at the top of this file to ensure
# PyCharm correctly recognizes namespace packages.

"""
tabsdata — Declarative data pipeline functions.

All public classes are accessible via ``import tabsdata as td``.

Overview
========

Define data pipelines using three decorators:

- ``@td.publisher``:   Ingest data from external sources into internal tables.
- ``@td.transformer``: Transform data between internal tables.
- ``@td.subscriber``:  Export data from internal tables to external destinations.

The pattern for any external system is always::

    td.<Service>Conn(uri=..., credentials=...)        # Connection config
        → td.<Service>Src(conn=..., queries=...)       # For publishers
        → td.<Service>Dest(conn=..., destination_tables=...)  # For subscribers

Examples
========

Publisher — ingest from MySQL::

    import tabsdata as td

    @td.publisher(
        source=td.MySQLSrc(
            conn=td.MySQLConn(
                uri="mysql://localhost/mydb",
                credentials=td.UserPasswordCredentials(user="admin", password="secret"),
            ),
            queries="SELECT * FROM users",
        ),
        tables="users",
    )
    def load_users(df: td.TableFrame) -> td.TableFrame:
        return df.drop_nulls()

Publisher — ingest from local CSV file::

    @td.publisher(
        source=td.LocalFileSource("/data/users.csv"),
        tables="users",
    )
    def load_csv(df: td.TableFrame) -> td.TableFrame:
        return df

Publisher — ingest from S3::

    @td.publisher(
        source=td.S3Src(
            conn=td.S3Conn(
                bucket_uri="s3://my-bucket",
                credentials=td.S3AccessKeyCredentials(
                    aws_access_key_id="AKIA...",
                    aws_secret_access_key="...",
                ),
            ),
            path="data/users.parquet",
        ),
        tables="users",
    )
    def load_s3(df: td.TableFrame) -> td.TableFrame:
        return df

Transformer — transform between internal tables::

    @td.transformer(
        input_tables="my_collection/users",
        output_tables="active_users",
    )
    def filter_active(users: td.TableFrame) -> td.TableFrame:
        return users.filter(td.col("active") == True)

Subscriber — export to PostgreSQL::

    @td.subscriber(
        tables="my_collection/active_users",
        destination=td.PostgresDest(
            conn=td.PostgresConn(
                uri="postgresql://localhost/analytics",
                credentials=td.UserPasswordCredentials(
                user="writer", password="secret"
            ),
            ),
            destination_tables="active_users_export",
        ),
    )
    def export_users(df: td.TableFrame) -> td.TableFrame:
        return df

Subscriber — export to local Parquet file::

    @td.subscriber(
        tables="my_collection/users",
        destination=td.LocalFileDestination("/output/users.parquet"),
    )
    def export_parquet(df: td.TableFrame) -> td.TableFrame:
        return df

Multiple inputs and outputs::

    @td.publisher(
        source=td.MySQLSrc(
            conn=td.MySQLConn(uri="mysql://localhost/shop"),
            queries=["SELECT * FROM orders", "SELECT * FROM items"],
        ),
        tables=["orders", "items"],
    )
    def load_shop(orders: td.TableFrame, items: td.TableFrame):
        return orders, items

Subscriber — split into multiple output tables::

    @td.subscriber(
        tables="my_collection/data",
        destination=td.MySQLDest(
            conn=td.MySQLConn(uri="mysql://localhost/warehouse"),
            destination_tables=["active_users", "inactive_users"],
        ),
    )
    def split_users(df: td.TableFrame):
        active = df.filter(td.col("active") == True)
        inactive = df.filter(td.col("active") == False)
        return active, inactive

Available Connectors
====================

SQL Databases:

==============  ==================  ============================
Service         Conn                Src / Dest
==============  ==================  ============================
MySQL           ``MySQLConn``       ``MySQLSrc``, ``MySQLDest``
PostgreSQL      ``PostgresConn``    ``PostgresSrc``, ``PostgresDest``
MariaDB         ``MariaDBConn``     ``MariaDBSrc``, ``MariaDBDest``
Oracle          ``OracleConn``      ``OracleSrc``, ``OracleDest``
DB2             ``Db2Conn``         ``Db2Src``
MSSQL           ``MSSQLConn``       ``MSSQLSrc``, ``MSSQLDest``
ClickHouse      ``ClickHouseConn``  ``ClickHouseDest``
StarRocks       ``StarRocksConn``   ``StarRocksDest``
Druid           ``DruidConn``       ``DruidDest``
==============  ==================  ============================

Cloud Storage:

==============  ==================  ============================
Service         Conn                Src / Dest
==============  ==================  ============================
AWS S3          ``S3Conn``          ``S3Src``, ``S3Dest``
Google Cloud    ``GCSConn``         ``GCSSrc``, ``GCSDest``
Azure Blob      ``AzureConn``       ``AzureSrc``, ``AzureDest``
==============  ==================  ============================

CDC (Change Data Capture, require separate pip packages):

==============  ====================  ======================
Service         Conn                  Trigger
==============  ====================  ======================
Postgres CDC    ``PostgresCdcConn``   ``PostgresCdcTrigger``
MySQL CDC       ``MySQLCdcConn``      ``MySQLCdcTrigger``
DB2 CDC         ``Db2CdcConn``        ``Db2CdcTrigger``
==============  ====================  ======================

Specialized (require separate pip packages ``tabsdata-<name>``):

==============  ====================  ==============================
Service         Conn                  Src / Dest
==============  ====================  ==============================
BigQuery        ``BigQueryConn``      ``BigQueryDest``
Snowflake       ``SnowflakeConn``     ``SnowflakeDest``
Databricks      ``DatabricksConn``    ``DatabricksSrc``,
                                          ``DatabricksDest``
MongoDB         ``MongoDBConn``       ``MongoDBDest``
Salesforce      ``SalesforceConn``    ``SalesforceSrc``
Kafka           ``KafkaConn``         (trigger: ``KafkaTrigger``)
==============  ====================  ==============================

Local and internal (no Conn needed):

================  ============================================
Type              Class
================  ============================================
Local files       ``LocalFileSource``, ``LocalFileDestination``
Internal tables   ``TableInput``, ``TableOutput``
================  ============================================

Credentials
===========

::

    # Username/password (all SQL databases)
    td.UserPasswordCredentials(user="admin", password="secret")

    # AWS S3
    td.S3AccessKeyCredentials(
        aws_access_key_id="AKIA...",
        aws_secret_access_key="...",
    )

    # Google Cloud Platform
    td.GCPServiceAccountKeyCredentials(
        service_account_key='{"type": "service_account", ...}',
    )

    # Azure
    td.AzureAccountKeyCredentials(account_name="myaccount", account_key="...")

    # HMAC (e.g. for GCS interoperability)
    td.HMACCredentials(access_id="...", secret="...")

Secrets
=======

For credentials that reference external secret stores instead of inline values::

    td.EnvironmentSecret("MY_ENV_VAR")           # Read from environment variable
    td.HashiCorpSecret(path="secret/data/mydb", key="password")  # HashiCorp Vault

File Formats
============

Used with file-based sources and destinations. If omitted, format is inferred
from the file extension::

    td.CSVFormat(separator=",", input_has_header=True)
    td.ParquetFormat()
    td.AvroFormat()
    td.NDJSONFormat()
    td.LogFormat()

File Info
=========

Control whether file metadata columns are added when reading from file sources::

    td.FileInfo.NAME   # Add a column with the source file name
    td.FileInfo.URI    # Add a column with the full source URI
    None               # No file info columns (default)

Triggers
========

Control when functions execute::

    trigger_by=td.ALL_DEPS          # All input tables (default)
    trigger_by=None                 # Manual only (default for publisher)
    trigger_by=td.CronTrigger(mask="0 */6 * * *")  # Every 6 hours
    trigger_by=td.StageTrigger(...)                 # Trigger on file staging events
    trigger_by="collection/table_name"              # Specific table
    trigger_by=["collection/t1", "collection/t2"]   # Multiple tables

TableFrame
==========

``td.TableFrame`` wraps Polars LazyFrame. Key functions:

- ``td.col("name")`` — reference a column in expressions
- ``td.lit(42)`` — create a literal value in expressions
- ``td.concat([df1, df2])`` — concatenate multiple TableFrames

Common operations::

    def transform(df: td.TableFrame) -> td.TableFrame:
        return (
            df
            .filter(td.col("age") > 18)
            .with_columns(td.col("name").str.to_uppercase().alias("NAME"))
            .drop_nulls()
            .select("NAME", "age", "email")
        )

Data Quality
============

Attach data quality checks to publishers, transformers, or subscribers using
``td.DataQuality``::

    from tabsdata.dataquality import DataQuality

    @td.publisher(
        source=td.LocalFileSource("/data/sales.csv"),
        tables="sales",
        on_tables=DataQuality(...),
    )
    def load_sales(df: td.TableFrame) -> td.TableFrame:
        return df

Named Connections
=================

For production, use ``NamedConn`` to reference server-side connection configs
instead of embedding credentials in code::

    @td.publisher(
        source=td.MySQLSrc(
            conn=td.NamedConn(name="production_mysql"),
            queries="SELECT * FROM users",
        ),
        tables="users",
    )
    def load_users(df: td.TableFrame) -> td.TableFrame:
        return df

Registering and Executing Functions
====================================

::

    from tabsdata.api.tabsdata_server import TabsdataServer

    server = TabsdataServer("localhost:2457", "admin", "tabsdata", "sys_admin")
    server.create_collection("my_collection", description="My data")
    server.register_function(
        collection_name="my_collection",
        function_path="/path/to/my_file.py::load_users",
    )
    execution = server.trigger_function("my_collection", "load_users")
"""

#
# Copyright 2025 Tabs Data Inc.
#

# This must be done early, as other imports might be importing packages that require
# having some system libraries already loaded.
from tabsdatak.common._runtime.libraries import load as _load_libraries  # isort: skip

_load_libraries()

import importlib.metadata
import logging

from tabsdata import typing

# ── Credentials ───────────────────────────────────────────────────────────────
from tabsdata._credentials import (
    AzureAccountKeyCredentials,
    GCPServiceAccountKeyCredentials,
    HMACCredentials,
    S3AccessKeyCredentials,
    UserPasswordCredentials,
)

# ── Triggers ──────────────────────────────────────────────────────────────────
from tabsdata._cron_trigger import CronTrigger

# ── Decorators ────────────────────────────────────────────────────────────────
from tabsdata._decorators import ALL_DEPS, publisher, subscriber, transformer

# ── File formats ──────────────────────────────────────────────────────────────
from tabsdata._format import (
    AvroFormat,
    CSVFormat,
    LogFormat,
    NDJSONFormat,
    ParquetFormat,
)
from tabsdata._info import FileInfo

# ── Cloud staging configs ─────────────────────────────────────────────────────
from tabsdata._io.cloud_staging.common.storage_config import (
    AzureConfig,
    GCSConfig,
    S3Config,
)
from tabsdata._io.connections.clickhouse_connection import ClickHouseConn

# ── Connections (Conn) ────────────────────────────────────────────────────────
from tabsdata._io.connections.connections import Conn
from tabsdata._io.connections.db2_connection import Db2Conn
from tabsdata._io.connections.druid_connection import DruidConn
from tabsdata._io.connections.file_connections.azure_connection import AzureConn
from tabsdata._io.connections.file_connections.gcp_connection import GCSConn
from tabsdata._io.connections.file_connections.s3_connection import S3Conn
from tabsdata._io.connections.mariadb_connection import MariaDBConn
from tabsdata._io.connections.mysql_connection import MySQLConn
from tabsdata._io.connections.named_connection.named_connection import NamedConn
from tabsdata._io.connections.oracle_connection import OracleConn
from tabsdata._io.connections.postgres_connection import PostgresConn
from tabsdata._io.connections.starrocks_connection import StarRocksConn

# ── Sources (Src) — for @td.publisher ─────────────────────────────────────────
from tabsdata._io.inputs.file.azure_input import AzureSrc
from tabsdata._io.inputs.file.gcs_input import GCSSrc
from tabsdata._io.inputs.file.s3_input import S3Src

# ── Legacy classes (deprecated — use the Src/Dest classes above) ──────────────
from tabsdata._io.inputs.file_inputs import (
    AzureSource,
    GCSSource,
    LocalFileSource,  # no Conn needed
    S3Source,
)
from tabsdata._io.inputs.sql.db2_input import Db2Src
from tabsdata._io.inputs.sql.mariadb_input import MariaDBSrc
from tabsdata._io.inputs.sql.mysql_input import MySQLSrc
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

# ── Destinations (Dest) — for @td.subscriber ──────────────────────────────────
from tabsdata._io.outputs.file.azure_output import AzureDest
from tabsdata._io.outputs.file.gcs_output import GCSDest
from tabsdata._io.outputs.file.s3_output import S3Dest

# noinspection PyProtectedMember
# noinspection PyProtectedMember
from tabsdata._io.outputs.file_outputs import (  # no Conn needed
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
from tabsdata._io.outputs.sql.oracle_output import OracleDest
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

# ── Plugin base classes ───────────────────────────────────────────────────────
# noinspection PyProtectedMember
from tabsdata._io.plugin import DestinationPlugin, SourcePlugin
from tabsdata._io.stage_trigger import StageTrigger

# ── Secrets ───────────────────────────────────────────────────────────────────
from tabsdata._secret import EnvironmentSecret, HashiCorpSecret

# ── Exceptions ────────────────────────────────────────────────────────────────
# noinspection PyProtectedMember
from tabsdata._tabsserver.function.execution_exceptions import CustomException
from tabsdata._tdfunction import TabsdataFunction

# ── External connectors (require separate pip packages) ───────────────────────
# BigQuery (pip install tabsdata-bigquery)
# noinspection PyProtectedMember
from tabsdata.connector.bigquery._connector import BigQueryConn, BigQueryDest

# CDC — Change Data Capture (pip install tabsdata-cdc-*)
from tabsdata.connector.cdc.common import CdcConn, CdcTrigger
from tabsdata.connector.cdc.db2._connection import Db2CdcConn
from tabsdata.connector.cdc.db2._connector import Db2CdcTrigger
from tabsdata.connector.cdc.mysql._connection import MySQLCdcConn
from tabsdata.connector.cdc.mysql._connector import MySQLCdcTrigger
from tabsdata.connector.cdc.postgres._connection import PostgresCdcConn
from tabsdata.connector.cdc.postgres._connector import PostgresCdcTrigger

# Databricks (pip install tabsdata-databricks)
# noinspection PyProtectedMember
from tabsdata.connector.databricks._connection import DatabricksConn
from tabsdata.connector.databricks._connector import (
    DatabricksDestination,
    DatabricksSource,
)
from tabsdata.connector.databricks.input.databricks_input import DatabricksSrc
from tabsdata.connector.databricks.output.databricks_output import DatabricksDest

# Kafka (pip install tabsdata-kafka)
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

# MongoDB (pip install tabsdata-mongodb)
# noinspection PyProtectedMember
from tabsdata.connector.mongodb._connection import MongoDBConn
from tabsdata.connector.mongodb._connector import MongoDBDestination
from tabsdata.connector.mongodb.output.mongodb_output import MongoDBDest

# noinspection PyProtectedMember
from tabsdata.connector.mssql._connector import MSSQLDestination, MSSQLSource

# MSSQL (pip install tabsdata-mssql)
from tabsdata.connector.mssql.connection.mssql_connection import MSSQLConn
from tabsdata.connector.mssql.input.mssql_input import MSSQLSrc
from tabsdata.connector.mssql.output.mssql_output import MSSQLDest

# Salesforce (pip install tabsdata-salesforce)
from tabsdata.connector.salesforce._connection import SalesforceConn
from tabsdata.connector.salesforce._connector import (
    SalesforceReportSource,
    SalesforceSource,
    SalesforceTokenCredentials,
)
from tabsdata.connector.salesforce.input.salesforce_input import SalesforceSrc
from tabsdata.connector.salesforce.input.salesforce_report_input import (
    SalesforceReportSrc,
)

# Snowflake (pip install tabsdata-snowflake)
from tabsdata.connector.snowflake._connection import SnowflakeConn
from tabsdata.connector.snowflake._connector import SnowflakeDestination
from tabsdata.connector.snowflake.output.snowflake_output import SnowflakeDest
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

# ── TableFrame (data processing) ─────────────────────────────────────────────
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
    "AzureSrc",
    "DatabricksSource",
    "DatabricksSrc",
    "Db2Src",
    "GCSSource",
    "GCSSrc",
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
    "S3Src",
    "SalesforceSrc",
    "SalesforceReportSource",
    "SalesforceReportSrc",
    "SalesforceSource",
    # Outputs
    "TableOutput",
    # Destinations
    "AWSGlue",
    "AzureDest",
    "AzureDestination",
    "BigQueryDest",
    "ClickHouseDest",
    "DruidDest",
    "DatabricksDest",
    "DatabricksDestination",
    "GCSDestination",
    "LocalFileDestination",
    "MariaDBDest",
    "MariaDBDestination",
    "MongoDBDest",
    "MongoDBDestination",
    "MSSQLDest",
    "MSSQLDestination",
    "MySQLDest",
    "MySQLDestination",
    "OracleDest",
    "OracleDestination",
    "PostgresDest",
    "PostgresDestination",
    "S3Dest",
    "GCSDest",
    "S3Destination",
    "SnowflakeDest",
    "SnowflakeDestination",
    "StarRocksDest",
    # Triggers
    "CronTrigger",
    "Db2CdcTrigger",
    "KafkaTrigger",
    "MySQLCdcTrigger",
    "PostgresCdcTrigger",
    # Connections
    "AzureConn",
    "BigQueryConn",
    "ClickHouseConn",
    "DatabricksConn",
    "Db2CdcConn",
    "Db2Conn",
    "DruidConn",
    "GCSConn",
    "KafkaConn",
    "MSSQLConn",
    "MariaDBConn",
    "MongoDBConn",
    "MySQLCdcConn",
    "MySQLConn",
    "NamedConn",
    "OracleConn",
    "PostgresCdcConn",
    "PostgresConn",
    "S3Conn",
    "SalesforceConn",
    "SnowflakeConn",
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
    # Literals
    "FileInfo",
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

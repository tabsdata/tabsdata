# tabsdata — LLM Guide

## What is tabsdata?

tabsdata is a declarative data pipeline framework. Users define Python functions
decorated with `@td.publisher`, `@td.transformer`, or `@td.subscriber` to move
and transform data between external systems and internal tables.

All public classes are accessible via `import tabsdata as td`.

## The Three Decorators

### @td.publisher — Ingest data from external sources
Reads from databases, files, or APIs into internal tables.

```python
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
```

### @td.transformer — Transform data between internal tables
Reads from internal tables, transforms, and writes to internal tables.

```python
@td.transformer(
    input_tables="collection/users",
    output_tables="active_users",
)
def filter_active(users: td.TableFrame) -> td.TableFrame:
    return users.filter(td.col("active") == True)
```

### @td.subscriber — Export data to external destinations
Reads from internal tables and writes to databases, files, or APIs.

```python
@td.subscriber(
    tables="collection/active_users",
    destination=td.PostgresDest(
        conn=td.PostgresConn(
            uri="postgresql://localhost/analytics",
            credentials=td.UserPasswordCredentials(user="writer", password="secret"),
        ),
        destination_tables="active_users_export",
    ),
)
def export_users(df: td.TableFrame) -> td.TableFrame:
    return df
```

## Key Pattern: Conn + Src/Dest

Every external system follows the same pattern:
1. Create a **Conn** (connection config with URI + credentials)
2. Pass it to a **Src** (source) or **Dest** (destination)
3. Pass the Src/Dest to the decorator

```
td.<Service>Conn(uri=..., credentials=...)
    ↓
td.<Service>Src(conn=..., queries=...)     ← for publishers
td.<Service>Dest(conn=..., destination_tables=...)  ← for subscribers
    ↓
@td.publisher(source=...)  or  @td.subscriber(destination=...)
```

## Available Connectors

### SQL Databases
| Service | Conn | Src | Dest |
|---------|------|-----|------|
| MySQL | `MySQLConn` | `MySQLSrc` | `MySQLDest` |
| PostgreSQL | `PostgresConn` | `PostgresSrc` | `PostgresDest` |
| MariaDB | `MariaDBConn` | `MariaDBSrc` | `MariaDBDest` |
| Oracle | `OracleConn` | `OracleSrc` | `OracleDest` |
| DB2 | `Db2Conn` | `Db2Src` | — |
| MSSQL | `MSSQLConn` | `MSSQLSrc` | `MSSQLDest` |
| ClickHouse | `ClickHouseConn` | — | `ClickHouseDest` |
| StarRocks | `StarRocksConn` | — | `StarRocksDest` |
| Druid | `DruidConn` | — | `DruidDest` |

### Cloud Storage
| Service | Conn | Src | Dest |
|---------|------|-----|------|
| AWS S3 | `S3Conn` | `S3Src` | `S3Dest` |
| Google Cloud Storage | `GCSConn` | `GCSSrc` | `GCSDest` |
| Azure Blob | `AzureConn` | `AzureSrc` | `AzureDest` |

### CDC (Change Data Capture)
| Service | Conn | Trigger |
|---------|------|---------|
| PostgreSQL CDC | `PostgresCdcConn` | `PostgresCdcTrigger` |
| MySQL CDC | `MySQLCdcConn` | `MySQLCdcTrigger` |
| DB2 CDC | `Db2CdcConn` | `Db2CdcTrigger` |

### Specialized
| Service | Conn | Src | Dest |
|---------|------|-----|------|
| BigQuery | `BigQueryConn` | — | `BigQueryDest` |
| Snowflake | `SnowflakeConn` | — | `SnowflakeDest` |
| Databricks | `DatabricksConn` | `DatabricksSrc` | `DatabricksDest` |
| MongoDB | `MongoDBConn` | — | `MongoDBDest` |
| Salesforce | `SalesforceConn` | `SalesforceSrc` | — |
| Kafka | `KafkaConn` | — | — (trigger only: `KafkaTrigger`) |

### Local / Internal
| Type | Class |
|------|-------|
| Local files | `LocalFileSource` (source), `LocalFileDestination` (dest) |
| Internal tables | `TableInput` (source), `TableOutput` (dest) |

## Credentials

```python
# Username/password (SQL databases)
td.UserPasswordCredentials(user="admin", password="secret")

# AWS S3
td.S3AccessKeyCredentials(
    aws_access_key_id="AKIA...",
    aws_secret_access_key="...",
)

# GCP
td.GCPServiceAccountKeyCredentials(service_account_key='{"type": "service_account", ...}')

# Azure
td.AzureAccountKeyCredentials(account_name="myaccount", account_key="...")

# HMAC (e.g. for GCS interoperability)
td.HMACCredentials(access_id="...", secret="...")
```

## Secrets

For credentials that reference external secret stores:
```python
td.EnvironmentSecret("MY_ENV_VAR")           # Read from environment variable
td.HashiCorpSecret(path="secret/data/mydb", key="password")  # HashiCorp Vault
```

## File Formats

Used with file-based sources and destinations:
```python
td.CSVFormat(separator=",", input_has_header=True)
td.ParquetFormat()
td.AvroFormat()
td.NDJSONFormat()
td.LogFormat()
```

If no format is specified, it is inferred from the file extension.

## File Source Metadata

Every table published by a file source (`LocalFileSrc`, `S3Src`, `GCSSrc`,
`AzureBlobSrc`) automatically carries one metadata column — there is nothing to
configure:

| Column | Contents |
|--------|----------|
| `@td.file.path` | Location the row was read from: a local path for `LocalFileSrc`, the object URI for a cloud source |

```python
td.S3Src(paths=["in/*.csv"])
# Rows read from in/a.csv carry:
#   @td.file.path  "s3://bucket/in/a.csv"
# Rows read from in/b.csv carry:
#   @td.file.path  "s3://bucket/in/b.csv"
```

A path that globs several files reports the specific file each row came from.

## SQL Source Metadata

Every table published by a SQL source (`MySQLSrc`, `PostgresSrc`, `OracleSrc`,
`Db2Src`, `MSSQLSrc`, `DatabricksSrc`) automatically carries two metadata
columns — there is nothing to configure:

| Column | Contents |
|--------|----------|
| `@td.sql.query` | Text of the query that produced the row |
| `@td.sql.initial_values` | JSON serialization of the bind values the query ran with (`{}` when the query has no binds) |

```python
td.MySQLSrc(
    queries=["SELECT * FROM city WHERE id > :wm"],
    initial_values={"wm": 0},
)
# Published rows carry:
#   @td.sql.query           "SELECT * FROM city WHERE id > :wm"
#   @td.sql.initial_values  '{"wm": 0}'
```

With several queries, each output table records its own query; the bind values
are the same for every query of a run.

## Salesforce Source Metadata

Every table published by a Salesforce source automatically carries two metadata
columns — there is nothing to configure:

| Source | Column | Contents |
|--------|--------|----------|
| `SalesforceSrc` | `@td.salesforce.soql` | The SOQL as sent to Salesforce (any `$lastModified` token already substituted) |
| `SalesforceSrc` | `@td.salesforce.last_modified` | The `initial_last_modified` cutoff it ran with (empty when unset) |
| `SalesforceReportSrc` | `@td.salesforce.reportId` | Resolved report id, even when the report was named by name |
| `SalesforceReportSrc` | `@td.salesforce.last_modified` | The `initial_last_modified` cutoff (empty when unset) |

```python
td.SalesforceReportSrc(reports=["Quarterly Pipeline"], find_report_by="name")
# Published rows carry:
#   @td.salesforce.reportId       "00O000000000001"
#   @td.salesforce.last_modified  ""
```

## Triggers

Control when functions execute:
```python
# Trigger on all input table changes (default for transformer/subscriber)
trigger_by=td.ALL_DEPS

# Manual trigger only (default for publisher)
trigger_by=None

# Cron schedule
trigger_by=td.CronTrigger(mask="0 */6 * * *")  # every 6 hours

# Trigger on file staging events
trigger_by=td.StageTrigger(...)

# Specific table changes
trigger_by="collection/table_name"
trigger_by=["collection/table1", "collection/table2"]
```

## TableFrame

`td.TableFrame` wraps Polars LazyFrame. Key functions:
- `td.col("name")` — reference a column in expressions
- `td.lit(42)` — create a literal value in expressions
- `td.concat([df1, df2])` — concatenate multiple TableFrames

Common operations:
```python
def transform(df: td.TableFrame) -> td.TableFrame:
    return (
        df
        .filter(td.col("age") > 18)
        .with_columns(td.col("name").str.to_uppercase().alias("NAME"))
        .drop_nulls()
        .select("NAME", "age", "email")
    )
```

## Data Quality

Attach data quality checks using `td.DataQuality`:
```python
from tabsdata.dataquality import DataQuality

@td.publisher(
    source=td.LocalFileSource("/data/sales.csv"),
    tables="sales",
    on_tables=DataQuality(...),
)
def load_sales(df: td.TableFrame) -> td.TableFrame:
    return df
```

## Multiple Inputs and Outputs

```python
# Multiple queries → multiple input TableFrames
@td.publisher(
    source=td.MySQLSrc(
        conn=conn,
        queries=["SELECT * FROM orders", "SELECT * FROM items"],
    ),
    tables=["orders", "items"],
)
def load_data(orders: td.TableFrame, items: td.TableFrame) -> (td.TableFrame, td.TableFrame):
    return orders, items

# Multiple output tables
@td.subscriber(
    tables="collection/data",
    destination=td.MySQLDest(
        conn=conn,
        destination_tables=["table_a", "table_b"],
    ),
)
def export(df: td.TableFrame) -> (td.TableFrame, td.TableFrame):
    active = df.filter(td.col("active") == True)
    inactive = df.filter(td.col("active") == False)
    return active, inactive
```

## Subscriber with Multiple Output Tables

```python
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
```

## Registering and Executing Functions

```python
from tabsdata.api.tabsdata_server import TabsdataServer

server = TabsdataServer("localhost:2457", "admin", "tabsdata", "sys_admin")
server.create_collection("my_collection", description="My data")
server.register_function(
    collection_name="my_collection",
    function_path="/path/to/my_file.py::load_users",
)
execution = server.trigger_function("my_collection", "load_users")
```

## Named Connections

For production deployments, use `NamedConn` to reference server-side connection configs:
```python
@td.publisher(
    source=td.MySQLSrc(
        conn=td.NamedConn(name="production_mysql"),
        queries="SELECT * FROM users",
    ),
    tables="users",
)
def load_users(df: td.TableFrame) -> td.TableFrame:
    return df
```

<!--
Copyright 2026 Tabsdata Inc.
-->

# tabsdata Package Structure

## User-facing API

Everything is accessible via `import tabsdata as td`.
Do NOT import from internal paths (anything with `_` prefix).

See `LLM_GUIDE.md` for usage examples.

## Where to find things

### Decorators
`_decorators.py` → `td.publisher`, `td.transformer`, `td.subscriber`

### Connections (Conn classes)
SQL databases:
- `_io/connections/mysql_connection.py` → `MySQLConn`
- `_io/connections/postgres_connection.py` → `PostgresConn`
- `_io/connections/mariadb_connection.py` → `MariaDBConn`
- `_io/connections/oracle_connection.py` → `OracleConn`
- `_io/connections/db2_connection.py` → `Db2Conn`
- `_io/connections/clickhouse_connection.py` → `ClickHouseConn`
- `_io/connections/starrocks_connection.py` → `StarRocksConn`
- `_io/connections/druid_connection.py` → `DruidConn`

Cloud storage:
- `_io/connections/file_connections/s3_connection.py` → `S3Conn`
- `_io/connections/file_connections/gcp_connection.py` → `GCSConn`
- `_io/connections/file_connections/azure_connection.py` → `AzureConn`

Named connections:
- `_io/connections/named_connection/named_connection.py` → `NamedConn`

### Sources (Src classes) — for @td.publisher
SQL (new style):
- `_io/inputs/sql/mysql_input.py` → `MySQLSrc`
- `_io/inputs/sql/postgres_input.py` → `PostgresSrc`
- `_io/inputs/sql/mariadb_input.py` → `MariaDBSrc`
- `_io/inputs/sql/oracle_input.py` → `OracleSrc`
- `_io/inputs/sql/db2_input.py` → `Db2Src`

Cloud files (new style):
- `_io/inputs/file/s3_input.py` → `S3Src`
- `_io/inputs/file/gcs_input.py` → `GCSSrc`
- `_io/inputs/file/azure_input.py` → `AzureSrc`

Local and internal:
- `_io/inputs/file_inputs.py` → `LocalFileSource`
- `_io/inputs/table_inputs.py` → `TableInput`
- `_io/inputs/stage_input.py` → `Stage`

Legacy (deprecated — use new-style classes above):
- `_io/inputs/file_inputs.py` → `S3Source`, `AzureSource`, `GCSSource`
- `_io/inputs/sql_inputs.py` → `MySQLSource`, `PostgresSource`, `MariaDBSource`, `OracleSource`

### Destinations (Dest classes) — for @td.subscriber
SQL (new style):
- `_io/outputs/sql/mysql_output.py` → `MySQLDest`
- `_io/outputs/sql/postgres_output.py` → `PostgresDest`
- `_io/outputs/sql/mariadb_output.py` → `MariaDBDest`
- `_io/outputs/sql/oracle_output.py` → `OracleDest`
- `_io/outputs/sql/clickhouse_output.py` → `ClickHouseDest`
- `_io/outputs/sql/starrocks_output.py` → `StarRocksDest`
- `_io/outputs/sql/druid_output.py` → `DruidDest`

Cloud files (new style):
- `_io/outputs/file/s3_output.py` → `S3Dest`
- `_io/outputs/file/gcs_output.py` → `GCSDest`
- `_io/outputs/file/azure_output.py` → `AzureDest`

Local and internal:
- `_io/outputs/file_outputs.py` → `LocalFileDestination`
- `_io/outputs/table_outputs.py` → `TableOutput`

Legacy (deprecated — use new-style classes above):
- `_io/outputs/file_outputs.py` → `S3Destination`, `AzureDestination`, `GCSDestination`
- `_io/outputs/sql_outputs.py` → `MySQLDestination`, `PostgresDestination`, `MariaDBDestination`, `OracleDestination`

### External connectors (separate pip packages)
These live under `connectors/python/tabsdata-<name>/` in the repo
and install into the `tabsdata.connector.<name>` namespace:
- `tabsdata-bigquery` → `BigQueryConn`, `BigQueryDest`
- `tabsdata-databricks` → `DatabricksConn`, `DatabricksSrc`, `DatabricksDest`
- `tabsdata-kafka` → `KafkaConn`, `KafkaTrigger`
- `tabsdata-mongodb` → `MongoDBConn`, `MongoDBDest`
- `tabsdata-mssql` → `MSSQLConn`, `MSSQLSrc`, `MSSQLDest`
- `tabsdata-salesforce` → `SalesforceConn`, `SalesforceSrc`, `SalesforceReportSrc`
- `tabsdata-snowflake` → `SnowflakeConn`, `SnowflakeDest`

### Credentials
`_credentials.py` → `UserPasswordCredentials`, `S3AccessKeyCredentials`, `GCPServiceAccountKeyCredentials`, `AzureAccountKeyCredentials`, `HMACCredentials`

### Secrets
`_secret.py` → `EnvironmentSecret`, `HashiCorpSecret`

### File formats
`_format.py` → `CSVFormat`, `ParquetFormat`, `AvroFormat`, `NDJSONFormat`, `LogFormat`

### Triggers
`_cron_trigger.py` → `CronTrigger`
`_io/stage_trigger.py` → `StageTrigger`

### TableFrame (data processing)
`tableframe/lazyframe/frame.py` → `TableFrame`
`tableframe/functions/col.py` → `col()`
`tableframe/functions/lit.py` → `lit()`
`tableframe/functions/eager.py` → `concat()`
`tableframe/dtypes.py` → type classes (Int32, String, etc.)

### Data quality
`dataquality/` → `DataQuality`, classifiers, operators

### Exceptions
`exceptions.py` → all exception and error code definitions

### Backend runtime (internal — not user-facing)
`_tabsserver/` → function execution, YAML parsing, environment creation
`_tabsserver/tools/` → janitor, config resolver, server info

### CLI (internal)
`_cli/` → command-line interface implementation

### API client
`api/apiserver.py` → `APIServer` (HTTP client)
`api/tabsdata_server.py` → `TabsdataServer` (high-level SDK)

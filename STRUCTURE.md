<!--
Copyright 2026 Tabsdata Inc.
-->

# tabsdata Package Structure

## User-facing API

The public surface lives in the `tabsdatak` namespace package: `tabsdatak.api`,
`tabsdatak.tableframe`, `tabsdatak.dataquality`, `tabsdatak.error`, and one
`tabsdatak.conn.<name>` package per connector. Do NOT import from internal
paths (anything with a `_` prefix).

See `LLM_GUIDE.md` for usage examples and `books/guides/LLM.md` for the full
authoring reference.

`tabsdatak` is an implicit namespace package: every distribution under
`connectors/python/tabsdata-*` contributes its own subpackages to it. The one
anchor `tabsdatak/__init__.py` lives in `client/platform/src/main/python/`.
Each distribution keeps its code in `src/main/python/tabsdatak/`.

## Where to find things

### Core API — `connectors/python/tabsdata-api`
- `api.py` → `tabsdatak.api`: `publisher`, `transformer`, `subscriber`,
  `stream_publisher`, `CronTrigger`, `TablesTrigger`, `AllTablesTrigger`,
  `Secret`, the `Src` / `Dest` / `StreamSrc` bases, the `*Spec` type aliases
- `tableframe/` → `TableFrame`; `tableframe/functions.py` → `col()`, `lit()`,
  `concat()`
- `dataquality/` → `DataQuality`, classifiers, operators
- `error.py` → `TabsdataException`, `ApiException`, `ApiErrorCode`
- `_connectors.py` → connector discovery: `avail_srcs()`,
  `avail_stream_srcs()`, `avail_dests()`
- `spi.py` → the connector implementer surface (not for flow code)

### Connector shared code
- `tabsdata-conn-common` → `tabsdatak.conn.common`: credentials and value
  types (`types.py`: `UserPassword`, `AwsAccessSecretKey`, `GCPJson`,
  `GCPHmac`, `AzureAccountKey`), `ConnException` (`error.py`)
- `tabsdata-conn-common-sql` → `tabsdatak.conn.common.sql`: shared SQL source
  and destination machinery, `@td.sql.*` metadata
- `tabsdata-conn-common-file` → `tabsdatak.conn.common.file`: shared file
  formats and validators for the file connectors, `@td.file.*` metadata

### Connectors (Src, Dest and Conn classes)
One distribution per connector, `connectors/python/tabsdata-conn-<name>`,
imported as `tabsdatak.conn.<name>`. Each exposes `<Vendor>Src` /
`<Vendor>Dest` for flow code and `<Vendor>SrcConn` / `<Vendor>DestConn` for the
connection registered on the collection.

SQL databases:
- `mysql` → `MySQLSrc`, `MySQLDest`, `MySQLCdcStreamSrc`
- `postgres` → `PostgresSrc`, `PostgresDest`, `PostgresCdcStreamSrc`
- `oracle` → `OracleSrc`, `OracleDest`
- `mssql` → `MSSQLSrc`, `MSSQLDest`
- `db2` → `Db2Src`, `Db2Dest`, `Db2CdcStreamSrc`

Warehouses and analytical stores:
- `databricks` → `DatabricksSrc`, `DatabricksDest`
- `bigquery` → `BigQueryDest`
- `snowflake` → `SnowflakeDest`
- `awsglue` → `AwsGlueDest` (Iceberg tables in the AWS Glue Data Catalog, queryable from Amazon Athena)
- `clickhouse` → `ClickHouseDest`
- `druid` → `DruidDest`
- `starrocks` → `StarRocksDest`
- `mongodb` → `MongoDBDest`

Files and object storage:
- `localfile` → `LocalFileSrc`, `LocalFileDest`
- `s3` → `S3Src`, `S3Dest`
- `gcs` → `GCSSrc`, `GCSDest`
- `azureblob` → `AzureBlobSrc`, `AzureBlobDest`

Applications and streams:
- `salesforce` → `SalesforceSrc`, `SalesforceReportSrc`
- `kafka` → `KafkaStreamSrc`

Development:
- `dev` → `DataGenSrc`, `TrashDest`

Stream staging:
- `tabsdata-conn-stream-stage` → `tabsdatak.conn.stream`: stages the batches
  of the stream sources (internal)

Scaffold a new connector from `templates/connector/`; `books/guides/CONN_DEV.md`
covers the implementer side.

### Tools and clients
- `tabsdata-cli` → `tabsdatak.cli`: the `tdk` command (`tdk-reference.md`)
- `tabsdata-sdk` → `tabsdatak.sdk`: `TabsdataServer`
  (`sdk/tabsdata_server.py`), the Python client of the API server
- `tabsdata-common` → `tabsdatak.common`: shared helpers and logging

### Backend runtime (internal — not user-facing)
- `tabsdata-server` → `tabsdatak.server`: function and data quality execution
  (`_tdrunner`), workers and rendering (`_tdserver`)
- `tabsdata-backend`, `tabsdata-control-plane`, `tabsdata-execution-plane` →
  `tabsdatak._subsystem`: the binary components of the backend tier and of the
  control and execution planes
- `tabsdata-medallion` → `tabsdatak._medallion` (private; skills and examples
  import it, so its paths are frozen): bronze, silver and gold pipeline support

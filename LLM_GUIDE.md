<!--
Copyright 2026 Tabsdata Inc.
-->

# tabsdata — LLM Guide

## What is tabsdata?

tabsdata is a declarative data pipeline framework. Users write Python functions
decorated with `@publisher`, `@transformer`, `@subscriber`, or
`@stream_publisher` to move and transform data between external systems and
internal tables.

The public surface lives in the `tabsdatak` namespace:

| Module | Contents |
|--------|----------|
| `tabsdatak.api` | Decorators, triggers, `Secret`, `TableFrameSpec` / `TableFramesSpec` |
| `tabsdatak.tableframe` | `TableFrame` |
| `tabsdatak.tableframe.functions` | `col`, `lit`, `concat` |
| `tabsdatak.conn.<name>` | One package per connector: `<Vendor>Src`, `<Vendor>Dest`, `<Vendor>SrcConn`, `<Vendor>DestConn` |
| `tabsdatak.conn.common.types` | Credentials (`UserPassword`, `AwsAccessSecretKey`, `GCPJson`, `AzureAccountKey`, ...) |
| `tabsdatak.dataquality` | `DataQuality`, classifiers, operators |
| `tabsdatak.error`, `tabsdatak.conn.common.error` | `ApiException`, `ConnException` |

Never import from `_`-prefixed modules. The full authoring reference, with the
common mistakes LLMs make, is `books/guides/LLM.md`; the official documentation
is at https://docs.tabsdata.com/guide/.

## The Decorators

### @publisher — Ingest data from external sources
Reads from databases, files, or APIs into internal tables.

```python
from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrc


@publisher(
    source=LocalFileSrc(paths=["persons.csv"]),
    output_tables=["persons"],
)
def qs_pub(inputs: TableFramesSpec) -> TableFrameSpec:
    if inputs is None or len(inputs) == 0:
        return None
    return inputs[0]
```

### @transformer — Transform data between internal tables
Reads from internal tables, transforms, and writes to internal tables.

```python
from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.tableframe.functions import col, lit


@transformer(
    input_tables=["input/persons"],
    output_tables=["people"],
)
def qs_tfr(persons: TableFrameSpec) -> TableFrameSpec:
    return persons.with_columns(
        (col("first_name") + lit(" ") + col("last_name")).alias("full_name")
    ).drop("age")
```

### @subscriber — Export data to external destinations
Reads from internal tables and writes to databases, files, or APIs.

```python
from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.localfile import LocalFileDest


@subscriber(
    destination=LocalFileDest(paths=["people.jsonl"]),
    input_tables=["dataprod/people"],
)
def qs_sub(people: TableFrameSpec) -> TableFrameSpec:
    return people
```

### @stream_publisher — Ingest a continuous stream in batches
Stages a stream (Kafka, or database change data capture) and publishes it in
batches bounded by the `batch_*` thresholds. Every input slot is a list of
`TableFrame`, one per staged file of the batch.

```python
from tabsdatak.api import TableFrameSpec, TableFramesSpec, stream_publisher
from tabsdatak.conn.kafka import KafkaStreamSrc


@stream_publisher(
    source=KafkaStreamSrc(topic="users", data_format="avro", schema=AVRO_SCHEMA),
    output_tables=["users"],
    batch_max_secs=60,
)
def load_users(batches: TableFramesSpec) -> TableFrameSpec:
    ...
```

## Function Signatures

* One positional parameter per input slot. A publisher has one slot per source
  query or path; a transformer or subscriber has one per entry of
  `input_tables`.
* SQL sources (`MySQLSrc`, `PostgresSrc`, `OracleSrc`, `MSSQLSrc`, `Db2Src`,
  `DatabricksSrc`, `SalesforceSrc`) hand one `TableFrame` per slot. File
  sources (`LocalFileSrc`, `S3Src`, `GCSSrc`, `AzureBlobSrc`) hand a
  `list[TableFrame]` per slot, because a path can glob several files.
* The return is a fixed-length tuple with one `TableFrame` per output table; a
  single output may be returned bare. `None` means "no new data".
* Decorate functions at module top level.

## Key Pattern: the connection lives on the collection

Flow code never declares a connection. A `Src` or `Dest` carries only its own
configuration (`queries`, `paths`, `tables`, ...); the connection
(`<Vendor>SrcConn` / `<Vendor>DestConn`, with URI and credentials) is
registered on the function's **collection** and resolved by the runtime.

```
connection file (connectionDef YAML)  →  tdk collection create --conn-file
<Vendor>Src(...) / <Vendor>Dest(...)  →  @publisher(source=...) / @subscriber(destination=...)
```

Generate a connection file with `tdk connection template --type <type>`
(`tdk connection types` lists them):

```yaml
kind: connectionDef
apiVersion: '1.0'
type: tabsdatak.conn.mysql:MySQLSrcConn
spec:
  uri: 'str:mysql://db.example.com:3306/sales'
  credentials:
    kind: userPasswordCredentials
    apiVersion: '1.0'
    type: tabsdatak.conn.common.types:UserPassword
    spec:
      user: 'secret:etl'
      password: '$secret:MYSQL_PASSWORD'
```

A string-or-secret field takes `str:<value>` for a plain value, `secret:<value>`
for a secret, or `$secret:<ENV_VAR>` to read the secret from an environment
variable on the client.

## Available Connectors

| Service | Module | Src | Dest | Src field | Dest field |
|---------|--------|-----|------|-----------|------------|
| MySQL / MariaDB | `tabsdatak.conn.mysql` | `MySQLSrc` | `MySQLDest` | `queries` | `tables` |
| PostgreSQL | `tabsdatak.conn.postgres` | `PostgresSrc` | `PostgresDest` | `queries` | `tables` |
| Oracle | `tabsdatak.conn.oracle` | `OracleSrc` | `OracleDest` | `queries` | `tables` |
| MSSQL | `tabsdatak.conn.mssql` | `MSSQLSrc` | `MSSQLDest` | `queries` | `tables` |
| IBM Db2 | `tabsdatak.conn.db2` | `Db2Src` | `Db2Dest` | `queries` | `tables` |
| Databricks | `tabsdatak.conn.databricks` | `DatabricksSrc` | `DatabricksDest` | `queries` | `tables` |
| Salesforce | `tabsdatak.conn.salesforce` | `SalesforceSrc`, `SalesforceReportSrc` | — | `queries` / `reports` | — |
| BigQuery | `tabsdatak.conn.bigquery` | — | `BigQueryDest` | — | `tables` |
| Snowflake | `tabsdatak.conn.snowflake` | — | `SnowflakeDest` | — | `tables` |
| ClickHouse | `tabsdatak.conn.clickhouse` | — | `ClickHouseDest` | — | `tables` |
| Druid | `tabsdatak.conn.druid` | — | `DruidDest` | — | `tables` |
| StarRocks | `tabsdatak.conn.starrocks` | — | `StarRocksDest` | — | `tables` |
| MongoDB | `tabsdatak.conn.mongodb` | — | `MongoDBDest` | — | `collections_with_ids` |
| Local files | `tabsdatak.conn.localfile` | `LocalFileSrc` | `LocalFileDest` | `paths` | `paths` |
| AWS S3 | `tabsdatak.conn.s3` | `S3Src` | `S3Dest` | `paths` | `paths` |
| Google Cloud Storage | `tabsdatak.conn.gcs` | `GCSSrc` | `GCSDest` | `paths` | `paths` |
| Azure Blob | `tabsdatak.conn.azureblob` | `AzureBlobSrc` | `AzureBlobDest` | `paths` | `paths` |
| Development | `tabsdatak.conn.dev` | `DataGenSrc` | `TrashDest` | `data` | — |

Every one of those fields is a list, even with a single entry:
`MySQLSrc(queries=["SELECT * FROM users"])`, `MySQLDest(tables=["users"])`.

### Stream sources (for `@stream_publisher`)
| Service | Module | Class |
|---------|--------|-------|
| Kafka | `tabsdatak.conn.kafka` | `KafkaStreamSrc` |
| PostgreSQL CDC | `tabsdatak.conn.postgres` | `PostgresCdcStreamSrc` |
| MySQL CDC | `tabsdatak.conn.mysql` | `MySQLCdcStreamSrc` |
| Db2 CDC | `tabsdatak.conn.db2` | `Db2CdcStreamSrc` |

## Credentials

Credentials go in the connection, never in flow code. The types live in
`tabsdatak.conn.common.types`:

| Type | For |
|------|-----|
| `UserPassword` | SQL databases, MongoDB, Salesforce |
| `AwsAccessSecretKey` | S3, and S3 staging locations |
| `GCPJson`, `GCPHmac` | GCS, BigQuery |
| `AzureAccountKey` | Azure Blob |

A credential field holds a `Secret` (`tabsdatak.api.Secret`). Its value never
lands in a serialized descriptor: it always serializes to the redacted marker
`:secret:`.

## File Formats

File sources and destinations take `format` and an optional `format_cfg`:

```python
from tabsdatak.conn.s3 import DestFileFormat, S3Dest, S3Src, SrcFileFormat

S3Src(
    paths=["in/*.csv"],
    format=SrcFileFormat.CSV,
    format_cfg={"CSV": {"has_header": True}},
)
S3Dest(paths=["out/users.parquet"], format=DestFileFormat.PARQUET)
```

Sources read `CSV`, `JSON`, `AVRO`, `PARQUET`, `LOG` and `EXCEL`; destinations
write `CSV`, `JSON`, `AVRO` and `PARQUET`. `AUTO` (the default) infers the
format from the file extension. `format_cfg` is keyed by format, and the
allowed keys differ between the source and the destination side.

## File Source Metadata

Every table published by a file source (`LocalFileSrc`, `S3Src`, `GCSSrc`,
`AzureBlobSrc`) automatically carries one metadata column:

| Column | Contents |
|--------|----------|
| `@td.file.path` | Location the row was read from: a local path for `LocalFileSrc`, the object URI for a cloud source |

```python
S3Src(paths=["in/*.csv"])
# Rows read from in/a.csv carry:
#   @td.file.path  "s3://bucket/in/a.csv"
# Rows read from in/b.csv carry:
#   @td.file.path  "s3://bucket/in/b.csv"
```

A path that globs several files reports the specific file each row came from.

## SQL Source Metadata

Every table published by a SQL source (`MySQLSrc`, `PostgresSrc`, `OracleSrc`,
`Db2Src`, `MSSQLSrc`, `DatabricksSrc`) automatically carries two metadata
columns:

| Column | Contents |
|--------|----------|
| `@td.sql.query` | Text of the query that produced the row |
| `@td.sql.initial_values` | JSON serialization of the bind values the query ran with (`{}` when the query has no binds) |

```python
MySQLSrc(
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
columns:

| Source | Column | Contents |
|--------|--------|----------|
| `SalesforceSrc` | `@td.salesforce.soql` | The SOQL as sent to Salesforce (any `$lastModified` token already substituted) |
| `SalesforceSrc` | `@td.salesforce.last_modified` | The `initial_last_modified` cutoff it ran with (empty when unset) |
| `SalesforceReportSrc` | `@td.salesforce.report_id` | Resolved report id, even when the report was named by name |
| `SalesforceReportSrc` | `@td.salesforce.last_modified` | The `initial_last_modified` cutoff (empty when unset) |

```python
SalesforceReportSrc(reports=["Quarterly Pipeline"], find_report_by="name")
# Published rows carry:
#   @td.salesforce.report_id      "00O000000000001"
#   @td.salesforce.last_modified  ""
```

## Kafka Source Metadata

Every batch staged by `KafkaStreamSrc` automatically carries one metadata
column — there is nothing to configure:

| Column | Contents |
|--------|----------|
| `@td.kafka.topic` | The topic the message was consumed from |

```python
KafkaStreamSrc(topic="users", data_format="avro", schema=AVRO_SCHEMA)
# Staged rows carry:
#   @td.kafka.topic  "users"
```

A stream source stages parquet rather than reporting per-file metadata, so the
column is written into the data itself — the same way the CDC sources write
their `@td.cdc.meta.*` columns.

## Dropping Source Metadata

Every source above that reports metadata accepts the `src_cfg` key
`tabsdata.src_metadata.drop`; set it to `True` and the published table carries
the source's own columns alone:

```python
S3Src(paths=["in/*.csv"], src_cfg={"tabsdata.src_metadata.drop": True})
MySQLSrc(queries=["SELECT * FROM city"], src_cfg={"tabsdata.src_metadata.drop": True})
```

It covers the file, SQL and Salesforce sources. The Kafka and CDC columns are
written into the staged data itself, so the key does not affect them.

## Triggers

Control when functions execute with `trigger_by`, which takes a trigger object
from `tabsdatak.api`:
```python
from tabsdatak.api import AllTablesTrigger, CronTrigger, TablesTrigger

# Default: a transformer or subscriber runs on a new version of any table it
# reads (its own excluded); a publisher, which reads none, only manually
trigger_by = None
trigger_by = AllTablesTrigger()  # the same, spelled out (transformer, subscriber)

# Specific table changes
trigger_by = TablesTrigger(tables=["table1", "collection/table2"])

# Manual runs only
trigger_by = TablesTrigger(tables=[])

# Cron schedule only
trigger_by = CronTrigger(mask="0 */6 * * *")  # every 6 hours
```

A bare list or `"*"` is rejected: `trigger_by` only takes these classes.

## TableFrame

`TableFrame` wraps a Polars LazyFrame. Expression helpers live in
`tabsdatak.tableframe.functions`:
- `col("name")` — reference a column in expressions
- `lit(42)` — create a literal value in expressions
- `concat([df1, df2])` — concatenate several TableFrames

```python
from tabsdatak.api import TableFrameSpec
from tabsdatak.tableframe.functions import col


def transform(df: TableFrameSpec) -> TableFrameSpec:
    return (
        df.filter(col("age") > 18)
        .with_columns(col("name").str.to_uppercase().alias("NAME"))
        .drop_nulls()
        .select("NAME", "age", "email")
    )
```

## Data Quality

Attach data quality checks to a publisher, transformer or stream publisher with
`on_tables`:
```python
from tabsdatak.dataquality import DataQuality


@publisher(
    source=LocalFileSrc(paths=["sales.csv"]),
    output_tables=["sales"],
    on_tables=[DataQuality(table="sales", classifiers=[...], operators=[...])],
)
def load_sales(inputs: TableFramesSpec) -> TableFrameSpec:
    return inputs[0]
```

## Multiple Inputs and Outputs

```python
from tabsdatak.conn.mysql import MySQLDest, MySQLSrc


# Two queries → two input slots → two output tables
@publisher(
    source=MySQLSrc(queries=["SELECT * FROM orders", "SELECT * FROM items"]),
    output_tables=["orders", "items"],
)
def load_data(
    orders: TableFrameSpec, items: TableFrameSpec
) -> tuple[TableFrameSpec, TableFrameSpec]:
    return orders, items


# One input table → two destination tables
@subscriber(
    destination=MySQLDest(tables=["active_users", "inactive_users"]),
    input_tables=["my_collection/users"],
)
def split_users(users: TableFrameSpec) -> tuple[TableFrameSpec, TableFrameSpec]:
    active = users.filter(col("active") == True)
    inactive = users.filter(col("active") == False)
    return active, inactive
```

## Registering and Executing Functions

With the `tdk` CLI (`tdk-reference.md` lists every command):

```sh
tdk login --server http://127.0.0.1:2457 --user admin --password tabsdata
tdk project create --name quickstart_dev
tdk set --proj quickstart_dev
tdk collection create --name input --group sources --conn-file conn-input.yaml
tdk fn register --coll input --path input.py::qs_pub
tdk fn trigger --coll input --name qs_pub
```

`tdk examples` extracts complete, runnable examples, including the quickstart
these snippets come from.

From Python, `tabsdatak.sdk.tabsdata_server.TabsdataServer` covers the same
operations (`create_project`, `create_collection`, `register_function`,
`trigger_function`, ...).

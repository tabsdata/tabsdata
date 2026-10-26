<!--
Copyright 2025 Tabsdata Inc.
--> 

<div align="center">
    <img src="assets/images/tabsdata-logo-for-light-theme.svg#gh-light-mode-only" alt="Tabsdata" width="500">
    <img src="assets/images/tabsdata-logo-for-dark-theme.svg#gh-dark-mode-only" alt="Tabsdata" width="500">
</div>

<div align="center">
    <a href="https://tabsdata.com">Tabsdata</a> |
    <a href="https://github.com/tabsdata/tabsdata">GitHub Repository</a>
</div>

# Tabsdata — Agentic Data Engineering

**From data request to resolution: hours, not months.**

[Tabsdata](https://tabsdata.com/) is an **agentic data engineering platform** built on a **Pub/Sub for Tables** core. Publishers bring external data in as tables, transformers produce new tables from existing ones, and subscribers deliver tables to the systems that consume them. Every run produces a new immutable table version, and lineage is recorded by the execution itself.

An AI agent does the discovery, duplication checks, assembly, and testing of a dataflow; an engineer reviews and approves every change. Because the agent runs inside the same system that tracks every table, version, dependency, and definition, it works with full context and cannot break what it can't see. Tabsdata exposes all of its functionality over MCP, so external agents can be guided through data engineering work on it as well.

Tabsdata lands beside your stack: your warehouse stays the center of gravity, and Tabsdata delivers governed, versioned tables into it. Existing workloads and the tools that run them are untouched; new workloads run end to end on Tabsdata.

Tables are manipulated with the [TableFrame API](https://docs.tabsdata.com/latest/api_ref/index.html) (built on [Polars](https://github.com/pola-rs/polars)), which supports selection, filtering, aggregation, and joins. Numerous connectors ship with the distribution, and new ones can be added through the Connector Plugin API.

# Tabsdata Binary Distribution

Tabsdata binary distribution of the Enterprise Package (binary distribution) that is
built on this Open Source foundation and contains more valued added features is
available in [PyPi](https://pypi.org/project/tabsdata/) as a binary package for Linux,
macOS and Windows.

To install and run the binary distribution use the following command:

```
pip install "tabsdata[all]"
```

## Tabsdata Binary Distribution Documentation

* [User Guide](https://docs.tabsdata.com/2.3.0/guide)
* [API Reference](https://docs.tabsdata.com/2.3.0/api)

# Contributing

Contributions are welcome! Please refer to the [Contributing Guide](assets/docs/CONTRIBUTING.md) for more information.

# How Does Tabsdata Work?

The following snippets show how to publish and subscribe to tables in Tabsdata.

Endpoints and credentials are declared as connections, apart from function code, and attached to the
collection a function is registered in. Functions declare only what they read and write.

## Declaring the Connections

A connection to a MySQL database, `conn-mysql.yaml`:

```
kind: connectionDef
apiVersion: '1.0'
type: tabsdatak.conn.mysql:MySQLSrcConn
spec:
  uri: 'str:mysql://127.0.0.1:3306/testing'
  credentials:
    kind: userPasswordCredentials
    apiVersion: '1.0'
    type: tabsdatak.conn.common.types:UserPassword
    spec:
      user: '$secret:MYSQL_USER'
      password: '$secret:MYSQL_PASSWORD'
```

A connection to an S3 bucket, `conn-s3.yaml`:

```
kind: connectionDef
apiVersion: '1.0'
type: tabsdatak.conn.s3:S3DestConn
spec:
  bucket: 'str:my-bucket'
  region: 'str:us-east-1'
  credentials:
    kind: awsAccessSecretKeyCredentials
    apiVersion: '1.0'
    type: tabsdatak.conn.common.types:AwsAccessSecretKey
    spec:
      access_key_id: '$secret:AWS_ACCESS_KEY_ID'
      secret_access_key: '$secret:AWS_SECRET_ACCESS_KEY'
```

A `$secret:<ENV_VAR>` value is read from that environment variable when the collection is created,
and is stored as a secret. `tdk connection template --type <type>` prints a fill-in template for any
connector, and `tdk connection types` lists them.

## Publishing data from a MySQL Database

```
from tabsdatak.api import TableFrameSpec, publisher
from tabsdatak.conn.mysql import MySQLSrc


@publisher(
    source=MySQLSrc(queries=["select * from CUSTOMERS"]),
    output_tables=["customers"],
)
def pub(customers: TableFrameSpec) -> TableFrameSpec:
    return customers
```

## Subscribing, transforming and publishing data within Tabsdata

```
from tabsdatak.api import TableFrameSpec, transformer
from tabsdatak.tableframe.functions import col


@transformer(
    input_tables=["input/customers"],
    output_tables=["spanish"],
)
def tfr(customers: TableFrameSpec) -> TableFrameSpec:
    return customers.filter(col("nationality").eq("spanish")).select(
        col("identifier"), col("name"), col("surname"), col("language")
    )
```

## Subscribing to data in an S3 Bucket

```
from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.s3 import S3Dest


@subscriber(
    destination=S3Dest(paths=["spanish.parquet"]),
    input_tables=["dataprod/spanish"],
)
def sub(spanish: TableFrameSpec) -> TableFrameSpec:
    return spanish
```

## Registering and Executing the Functions

Log in, create a project, create one collection per function (the source and destination ones with
their connections), register each function in its collection, and trigger the publisher to run the
rest:

```
$ tdk login

$ tdk project create --name example
$ tdk set --proj example

$ tdk collection create --name input --group sources --conn-file conn-mysql.yaml
$ tdk collection create --name dataprod --group default
$ tdk collection create --name output --group destinations --conn-file conn-s3.yaml

$ tdk fn register --coll input --path input.py::pub
$ tdk fn register --coll dataprod --path transform.py::tfr
$ tdk fn register --coll output --path output.py::sub

$ tdk fn trigger --coll input --name pub
```

Every time the `pub` publisher is executed, the `tfr` transformer and the `sub` subscriber will also
be executed.

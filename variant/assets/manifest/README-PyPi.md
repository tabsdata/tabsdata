<!--
Copyright 2026 Tabsdata Inc.
-->

<div align="center">
    <img
        src="https://cdn.prod.website-files.com/6980eb1aa2753a5298dd0836/6adf71314186fd3b74c29f49_tabsdata-logo-tight.svg"
        alt="Tabsdata"
        width="500"
    />
</div>

<div align="center">
    <img
        src="https://cdn.prod.website-files.com/6980eb1aa2753a5298dd0836/6adf6c0c04c4902b2c12d043_tabsdata-logo-bottom-line-green-thick.svg"
        alt=""
        width="100%"
        height="3"
    />
</div>

<br>

<div align="center">
    <a href="https://tabsdata.com">Tabsdata</a> |
    <a href="https://docs.tabsdata.com/2.4.0/guide">User Guide</a> |
    <a href="https://docs.tabsdata.com/2.4.0/api">API Reference</a>
</div>

# License

Your use of this product is subject to the terms of use available at https://tabsdata.com/license.

# Tabsdata: Agentic Data Engineering

**From data request to resolution: hours, not months.**

[Tabsdata](https://tabsdata.com/) is an agentic data engineering platform built on a declarative
publish-subscribe (pub/sub) server for tables. Functions declare the tables they read and the tables
they write, and the server handles the rest: loading, processing, storing and moving. Everything the
server knows (tables, versions, dependencies, definitions and execution history) is addressable
through its API, which is what lets an AI agent drive it end to end.

The server derives the dependency graph from those declarations and triggers downstream functions
when a new version of an upstream table is committed. There is no pipeline to wire end to end and no
external scheduler, so answering a new request is a declaration against tables that already exist
rather than a dataflow assembled from scratch.

Tables are immutable and versioned. A function run does not mutate a table, it commits a new
version, and each run is recorded against the table versions it read and wrote. Lineage and
provenance are a product of execution rather than a separate metadata pass, so any table, dataflow
or run can be inspected or replayed as of any point in time. Because nothing is ever overwritten, an
agent operating on Tabsdata cannot break what it cannot see: every change is a new version an
engineer can review and approve before it propagates.

Tabsdata lands beside your existing stack. Your warehouse stays the center of gravity; Tabsdata
resolves the request lifecycle around it and delivers governed, versioned tables into it. Existing
workloads and the tools that run them are untouched. New workloads run end to end on Tabsdata, and
the adoption decision is per workload.

Three kinds of functions make up a dataflow. **Publishers** bring data in from external systems and
land it as tables. **Transformers** read tables and produce new tables. **Subscribers** deliver
tables to external destinations. Numerous connectors ship with the distribution, and new ones can be
added through the Connector Plugin API.

Tables can be manipulated using a
[TableFrame API](https://docs.tabsdata.com/2.3.0/api/category/tableframe/) (internally Tabsdata uses
[Polars](https://github.com/pola-rs/polars)) that enables selection, filtering, aggregation and
joins operations.

The server additionally provides a semantic catalog, which classifies the metadata of connected
sources and destinations on connection and keeps it current as they change, recording what a table
holds and how it is used alongside its schema; and medallion primitives, which provide raw capture,
quality observation, change data capture and slowly changing dimensions as composable functions.

Function code is the same whether the server runs as a single instance on a laptop or workstation or
on a Kubernetes cluster, where function execution scales out on the cluster and table storage is
backed by cloud object storage. Your cloud, hybrid or fully on-premises: data never leaves your
network.

## Driving Tabsdata from an AI Agent

The dependency graph, table versions, execution history and semantic catalog are first-class objects
of the server API, not metadata reconstructed after the fact. An agent queries what a table holds,
what produced it, what depends on it and what changed between two versions, and it operates on what
the server recorded rather than on inference about it. Writes follow the same rules as any other
client: a function run commits a new version instead of mutating a table, under the same access
control, and earlier versions remain readable.

The division of labor is deliberate. The agent does discovery, duplication checks, assembly and
boilerplate; the engineer decides and approves.

* **Coding agents** - agents such as Claude Code, Codex or Cursor drive Tabsdata through the `tdk`
  CLI in their own shell: under an agent it never prompts and answers each command with one JSON
  document and a fixed exit code.
* **Built-in agent** - in the Tabsdata UI, scoped to operations: what ran, what failed and why.

For more details refer to [Tabsdata Getting Started](https://docs.tabsdata.com/guide/introduction)
(latest) or the [Tabsdata User Guide](https://docs.tabsdata.com/guide) (latest).

## Installation

Supported platforms:

* Windows (x86 - latest)
* macOS (Apple silicon - latest)
* Ubuntu, Debian & RedHat - (x86 - latest)
* Kubernetes (AWS EKS, Azure AKS & Google GKE - latest)

```
pip install tabsdata
```

### Running Tabsdata Locally

`tdkserver quickstart` initializes, creates and starts a local instance:

```
$ tdkserver quickstart
```

## This version (2.3.0) Documentation

* [User Guide](https://docs.tabsdata.com/2.3.0/guide)
* [API Reference](https://docs.tabsdata.com/2.3.0/api)

## How Does Tabsdata Work?

The following snippets show how to publish and subscribe to tables in Tabsdata.

Endpoints and credentials are declared as connections, apart from function code, and attached to the
collection a function is registered in. Functions declare only what they read and write.

### Declaring the Connections

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

### Publishing data from a MySQL Database

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

### Subscribing, transforming and publishing data within Tabsdata

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

### Subscribing to data in an S3 Bucket

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

### Registering and Executing the Functions

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

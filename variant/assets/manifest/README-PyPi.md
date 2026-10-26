<!--
Copyright 2026 Tabsdata Inc.
-->

<div align="center">
    <img
        src="https://docs.tabsdata.com/img/logo-light.svg"
        alt="Tabsdata"
        width="500"
    />
</div>

<div align="center">━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</div>

<br>

<div align="center">
    <a href="https://tabsdata.com">Tabsdata</a> |
    <a href="https://docs.tabsdata.com/2.3.0/guide">User Guide</a> |
    <a href="https://docs.tabsdata.com/2.3.0/api">API Reference</a>
</div>

# License

Your use of this product is subject to the terms of use available at https://tabsdata.com/license.

# Tabsdata: An Agent-Ready Data Engineering Platform

[Tabsdata](https://tabsdata.com) is a declarative publish-subscribe (pub/sub) server for tables.
Functions declare the tables they read and the tables they write, and the server handles the rest:
loading, processing, storing and moving. Everything the server knows, tables, versions,
dependencies, definitions and execution history, is addressable through its API, which is what
lets an AI agent drive it end to end.

The server derives the dependency graph from those declarations and triggers downstream functions
when a new version of an upstream table is committed. There is no pipeline to wire end to end and
no external scheduler, so answering a new request is a declaration against tables that already
exist rather than a dataflow assembled from scratch.

Tables are immutable and versioned. A function run does not mutate a table, it commits a new
version, and each run is recorded against the table versions it read and wrote. Lineage and
provenance are a product of execution rather than a separate metadata pass, so any table, dataflow
or run can be inspected or replayed as of any point in time.

Tabsdata has connectors to publish and subscribe tables from local files, S3, Azure Storage,
MySQL/MariaDB, Oracle, PostgreSQL. It also provides a Connector Plugin API to write custom
connectors.

Tables can be populated with external data or using data from other tables already existing
in the Tabsdata server.

Tables can be manipulated using a [TableFrame API](https://docs.tabsdata.com/2.3.0/api/category/tableframe/)
(internally Tabsdata uses [Polars](https://github.com/pola-rs/polars)) that enables selection,
filtering, aggregation and joins operations.

The server additionally provides a semantic catalog, which classifies the metadata of connected
sources and destinations on connection and keeps it current as they change, recording what a table
holds and how it is used alongside its schema; and medallion primitives, which provide raw
capture, quality observation, change data capture and slowly changing dimensions as composable
functions.

Function code is the same whether the server runs as a single instance on a laptop or workstation
or on a Kubernetes cluster, where function execution scales out on the cluster and table storage
is backed by cloud object storage.

## Driving Tabsdata from an AI Agent

The dependency graph, table versions, execution history and semantic catalog are first-class
objects of the server API, not metadata reconstructed after the fact. An agent queries what a
table holds, what produced it, what depends on it and what changed between two versions, and it
operates on what the server recorded rather than on inference about it. Writes follow the same
rules as any other client: a function run commits a new version instead of mutating a table, under
the same access control, and earlier versions remain readable.

* **MCP server** - exposes the catalog, table versions, dependency graph and execution history, as
  well as the operations to define, trigger and inspect functions, to external AI agents.
* **Built-in agent** - in the Tabsdata UI, scoped to operations: what ran, what failed and why.

For more details refer
to [Tabsdata Getting Started](https://docs.tabsdata.com/guide/introduction)
(latest) or the [Tabsdata User Guide](https://docs.tabsdata.com/guide) (latest).

## Installation

Supported platforms:

* Windows (x86 - latest)
* macOS (Apple silicon - latest)
* Ubuntu, Debian & RedHat - (x86 - latest)
* Kubernetes (AWS EKS - latest, more distributions in upcoming releases)

```
pip install "tabsdata[all]"
```

### Running Tabsdata Locally

`tdkserver quickstart` initializes, creates and starts a local instance. AI integration is
configured with whichever key is given on the command line:

```
$ tdkserver quickstart                                    # without AI integration
$ tdkserver quickstart --anthropic-key $ANTHROPIC_API_KEY # integration with Anthropic
$ tdkserver quickstart --openai-key $OPENAI_API_KEY       # integration with OpenAI
```

## This version (2.3.0) Documentation

* [User Guide](https://docs.tabsdata.com/2.3.0/guide)
* [API Reference](https://docs.tabsdata.com/2.3.0/api)

## How Does Tabsdata Work?

The following snippets show how to publish and subscribe to tables in Tabsdata.

Endpoints and credentials are declared as connections, apart from function code, and attached to
the collection a function is registered in. Functions declare only what they read and write.

### Declaring a Connection to a MySQL Database

```
kind: connectionDef
apiVersion: '1.0'
type: tabsdatak.conn.mysql:MySQLSrcConn
spec:
  uri: mysql://127.0.0.1:3306/testing
  credentials:
    kind: userPasswordCredentials
    apiVersion: '1.0'
    type: tabsdatak.conn.common.types:UserPassword
    spec:
      user: admin
      password: tabsdata
```

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

Each function is registered in a collection, and triggering the publisher runs the rest:

```
$ tdk fn register --coll input --path input.py::pub
$ tdk fn register --coll dataprod --path transform.py::tfr
$ tdk fn register --coll output --path output.py::sub

$ tdk fn trigger --coll input --name pub
```

Every time the `pub` publisher is executed, the `tfr` transformer and the `sub` subscriber will also be
executed.

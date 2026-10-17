<!--
Copyright 2026 Tabsdata Inc.
-->

<div align="center">
    <svg
        width="825"
        height="210"
        viewBox="0 0 504 128"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
    >
        <g id="wordmark">
            <path
                fill="currentColor"
                d="M436 34V48H448V56H436V77C436 78.66 437.34 80 439 80H446V88H440C431 88 428 85 428 76V56H420V48H428V34H436Z"
            />
            <path
                fill="currentColor"
                d="M208 48H184V56H209C210.66 56 212 57.34 212 59V64H191C184.37 64 180 67 180 75.5C180 84 184.37 88 191 88H213C216.87 88 220 84.87 220 81V60C220 53.37 217 48 208 48ZM212 77C212 78.66 210.66 80 209 80H192C189.79 80 188 79 188 75.5C188 72 189.79 72 192 72H212V77Z"
            />
            <path
                fill="currentColor"
                d="M401 48H377V56H402C403.66 56 405 57.34 405 59V64H384C377.37 64 373 67 373 75.5C373 84 377.37 88 384 88H406C409.87 88 413 84.87 413 81V60C413 53.37 410 48 401 48ZM405 77C405 78.66 403.66 80 402 80H385C382.79 80 381 79 381 75.5C381 72 382.79 72 385 72H405V77Z"
            />
            <path
                fill="currentColor"
                d="M482 48H458V56H483C484.66 56 486 57.34 486 59V64H465C458.37 64 454 67 454 75.5C454 84 458.37 88 465 88H487C490.87 88 494 84.87 494 81V60C494 53.37 491 48 482 48ZM486 77C486 78.66 484.66 80 483 80H466C463.79 80 462 79 462 75.5C462 72 463.79 72 466 72H486V77Z"
            />
            <path
                fill="currentColor"
                d="M251 48H238V34H230V75C230 83.84 235 88 244 88H251C264 88 270 82 270 68C270 54 264 48 251 48ZM253 80H243C240.24 80 238 78.31 238 75V56H253C259 56 262 60 262 68C262 76 259 80 253 80Z"
            />
            <path
                fill="currentColor"
                d="M325 68C325 82 331 88 344 88H351C360 88 365 83.84 365 75V34H357V48H344C331 48 325 54 325 68ZM333 68C333 60 336 56 342 56H357V75C357 78.31 354.76 80 352 80H342C336 80 333 76 333 68Z"
            />
            <path
                fill="currentColor"
                d="M289 72H306C308.21 72 310 72 310 75.5C310 79 308.21 80 306 80H279V88H307C313.63 88 318 84 318 75.5C318 67 313.63 64 307 64H290C287.79 64 286 63 286 59.5C286 56 287.79 56 290 56H316V48H289C282.37 48 278 51 278 59.5C278 68 282.37 72 289 72Z"
            />
            <path
                fill="currentColor"
                d="M162 34V48H174V56H162V77C162 78.6599 163.34 79.9999 165 80H172V88H166C157 87.9999 154 84.9999 154 76V56H146V48H154V34H162Z"
            />
        </g>
        <g id="arrow">
            <path
                fill="#59B359"
                d="M90.8188 61.1719C91.569 61.922 91.9907 62.9391 91.9907 64C91.9907 65.0609 91.569 66.078 90.8188 66.8281L42.8188 114.828L37.1626 109.172L78.3345 68H7.99072V60H78.3345L37.1626 18.8281L42.8188 13.1719L90.8188 61.1719Z"
            />
            <path
                fill="#34BBFF"
                d="M106.819 85.1719L101.163 90.8281L86.8193 76.4844L92.4756 70.8281L106.819 85.1719Z"
            />
            <path
                fill="#34BBFF"
                d="M119.991 60V68H94.917C95.6133 66.7939 95.9902 65.4167 95.9902 64C95.9902 62.5833 95.6133 61.2061 94.917 60H119.991Z"
            />
            <path
                fill="#34BBFF"
                d="M106.819 42.8281L92.4756 57.1709L86.8193 51.5146L101.163 37.1719L106.819 42.8281Z"
            />
        </g>
    </svg>
</div>

<div align="center">
    <a href="https://tabsdata.com">Tabsdata</a> |
    <a href="https://docs.tabsdata.com/2.2.1/guide">User Guide</a> |
    <a href="https://docs.tabsdata.com/2.2.1/api">API Reference</a>
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

Tables can be manipulated using a [TableFrame API](https://docs.tabsdata.com/2.2.1/api/category/tableframe/)
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

## This version (2.2.1) Documentation

* [User Guide](https://docs.tabsdata.com/2.2.1/guide)
* [API Reference](https://docs.tabsdata.com/2.2.1/api)

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

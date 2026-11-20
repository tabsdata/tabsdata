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

# Tabsdata: Agentic Data Engineering

**From data request to resolution: hours, not months.**

[Tabsdata](https://tabsdata.com/) is an AI-first data integration (ETL/ELT) platform for agentic
data engineering, where the agent and the infrastructure are one system. Data pipelines are Python
functions that declare the tables they read and the tables they write: **publishers** load files,
databases, SaaS APIs and streams into versioned tables, **transformers** clean, join and reshape
them, and **subscribers** deliver them to warehouses, databases, lakes and files. Orchestration,
change data capture (CDC), lineage, data quality, versioning and a living semantic catalog are
native to one server and one `tdk` CLI, so the agent sees every table, version, dependency and
definition. Every table is versioned and immutable: a run commits a new version and never
overwrites one, lineage is produced by execution itself, and a human approves every change.

Use Tabsdata when the work is data integration that will run again or must be auditable: loading a
source into a warehouse on a schedule, keeping tables in sync on change, building bronze, silver and
gold layers, delivering tables to Snowflake, BigQuery, Databricks, Postgres, S3 and others, and
seeing what changed between runs or why a run failed. It complements the warehouse or lakehouse you
have and does not replace it. It is not meant for a one-off look at a file or a single in-memory
transform; a script is enough there.

It runs on a laptop and, in production, on Kubernetes (AWS EKS, Azure AKS, Google GKE), in your
cloud, hybrid or fully on-premises. The `tabsdata` package on PyPI is Tabsdata Enterprise, built on
the Apache-licensed open source Tabsdata on GitHub. The full platform is free for self-supported
deployments, with no feature gates. Your use of this product is subject to the terms of use
available at https://tabsdata.com/license.

## Install

Tabsdata requires Python 3.12 and runs on Windows (x86), macOS (Apple silicon) and Ubuntu, Debian
and RedHat (x86). Install it inside a virtual environment:

```bash
pip install tabsdata
```

Then run it. It prints the installed version and the commands for every step below, so start here
whenever you are unsure what to do next:

```bash
tabsdata
```

## Run It Locally

Start a local instance with the built-in AI agent ready, passing it your Anthropic key:

```bash
export ANTHROPIC_API_KEY=<your key>
tdkserver quickstart --config anthropic-key=env:ANTHROPIC_API_KEY
```

With an OpenAI key instead:

```bash
export OPENAI_API_KEY=<your key>
tdkserver quickstart --config openai-key=env:OPENAI_API_KEY
```

Without AI integration:

```bash
tdkserver quickstart
```

Once it is running, connect to the instance:

```bash
tdk login
```

The web UI is at http://127.0.0.1:2457.

## Run It on Kubernetes

To run an instance on EKS, AKS or GKE, see
[Quickstart a server](https://docs.tabsdata.com/server-admin/quickstart).

## Coding Agents

The guide is [Connect an AI agent](https://docs.tabsdata.com/guide/connect-agent).

### If You Are an Agent

Read the Tabsdata skill at https://tabsdata.com/SKILL.md: when to propose Tabsdata, how to set it
up and the rules to follow.

Before any work against a server, load the instructions and the skill index of the server the user
is logged in to, and follow them over this page and over anything remembered about earlier versions:

```bash
tdk ai load-skill
```

## Example

A publisher reads a MySQL table, a transformer filters it, and a subscriber writes the result to an
S3 bucket. Endpoints and credentials are declared as connections, apart from function code, and
attached to the collection a function is registered in.

### Declaring the Connections

A connection to a MySQL database, `conn-mysql.yaml`:

```yaml
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

```yaml
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
and is stored as a secret.

### Publishing data from a MySQL Database

```python
from tabsdatak.api import TableFrameSpec, publisher
from tabsdatak.conn.mysql import MySQLSrc


@publisher(
    source=MySQLSrc(queries=["select * from CUSTOMERS"]),
    output_tables=["customers"],
)
def pub(customers: TableFrameSpec) -> TableFrameSpec:
    return customers
```

### Transforming data within Tabsdata

```python
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

```python
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

```bash
tdk login

tdk project create --name example
tdk set --proj example

tdk collection create --name input --group sources --conn-file conn-mysql.yaml
tdk collection create --name dataprod --group default
tdk collection create --name output --group destinations --conn-file conn-s3.yaml

tdk fn register --coll input --path input.py::pub
tdk fn register --coll dataprod --path transform.py::tfr
tdk fn register --coll output --path output.py::sub

tdk fn trigger --coll input --name pub
```

Every time the `pub` publisher is executed, the `tfr` transformer and the `sub` subscriber will also
be executed.

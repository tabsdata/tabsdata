<!--
Copyright 2025 Tabsdata Inc.
--> 

<div align="center">
    <picture>
        <source media="(prefers-color-scheme: dark)" srcset="assets/images/tabsdata-logo-for-dark-theme.svg">
        <source media="(prefers-color-scheme: light)" srcset="assets/images/tabsdata-logo-for-light-theme.svg">
        <img src="assets/images/tabsdata-logo-adaptive.svg" alt="Tabsdata" width="500">
    </picture>
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

## Publishing data from a MySQL Database

```
@td.publisher(
    td.MySQLSource(
        "mysql://127.0.0.1:3306/testing",
        ["select * from CUSTOMERS"],
        td.UserPasswordCredentials("admin", td.EnvironmentSecret("DB_PASWORD"))
    ),
    tables=["customers"]
)
def pub(customers: td.TableFrame) -> td.TableFrame:
    return customers
```

## Subscribing, transforming and publishing data within Tabsdata

```
@td.transformer(
    input_tables=["persons"],
    output_tables=["spanish"]
)
def tfr(persons: td.TableFrame):
    return persons.filter(td.col("nationality").eq("spanish")).select(
        ["identifier", "name", "surname", "language"]
    )
```

## Subscribing to data in an S3 Bucket

```
@td.subscriber(
    "spanish",
    td.S3Destination(
        "s3://my_bucket/spanish.parquet",
        td.S3AccessKeyCredentials(
            td.EnvironmentSecret("AWS_ACCESS_KEY_ID"),
            td.EnvironmentSecret("AWS_SECRET_KEY")
        )
    ),
)
def sub(spanish: td.TableFrame):
    return spanish
```

## Executing the Publisher

To publish data to Tabsdata run the following command:

```
$ tdk fn trigger --coll examples --name pub
```

In Tabsdata binary distribution, every time the `pub` publisher is executed, the `tfr` transformer
and the `sub` subscriber will also be executed.

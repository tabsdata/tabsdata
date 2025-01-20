<!--
Copyright 2025 Tabs Data Inc.
-->

![Tabsdata](assets/images/tabsdata.png)

<p style="text-align:center;">
    <a href="https://tabsdata.com">Tabsdata</a>
    |
    <a href="https://github.com/tabsdata/tabsdata">GitHub Repository</a>
    |
    <a href="https://github.com/tabsdata/tabsdata/discussions">GitHub Discussions</a>
    |
    <a href="https://github.com/tabsdata/tabsdata/issues">GitHub Issues</a>
    |
    <a href="https://stackoverflow.com/questions/tagged/tabsdata">StackOverflow</a>
    |
    <a href="https://discord.com/invite/5stJks6W">Discord</a>
</p>

# Tabsdata Pub/Sub for Tables

[Tabsdata](https://tabsdata.com) is a publish-subscribe (pub/sub) server for tables.

Tabsdata has connectors to publish and subscribe tables from local files, S3, Azure Storage,
MySQL/MariaDB, Oracle and PostgreSQL. It also provides a Connector Plugin API to write custom
connectors.

Tables can be populated with external data or using data from other tables already existing
in the Tabsdata server.

Tables can be manipulated using a [TableFrame API](https://docs.tabsdata.com/latest/api_ref/index.html)
(internally Tabsdata uses [Polars](https://github.com/pola-rs/polars)) that enables selection,
filtering, aggregation and joins operations.

## Tabsdata Binary Distribution

Tabsdata binary distribution of the Enterprise Package (binary distribution) that is
built on this Open Source foundation and contains more valued added features is
available in [PyPi](https://pypi.org/project/tabsdata/) as a binary package for Linux,
macOS and Windows.

To install and run the binary distribution use the following command:

```
pip install tabsdata
```

### Tabsdata Binary Distribution Documentation

* [User Guide](https://docs.tabsdata.com/latest/guide/intro.html)
* [API Reference](https://docs.tabsdata.com/latest/api_ref/index.html)

## Contributing

Contributions are welcome! Please refer to the [Contributing Guide](assets/docs/CONTRIBUTING.md) for more information.

## How Does Tabsdata Work?

The following snippets show how to publish and subscribe to tables in Tabsdata.

### Publishing data from a MySQL Database

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

### Subscribing, transforming and publishing data within Tabsdata

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

### Subscribing to data in an S3 Bucket

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

### Executing the Publisher

To publish data to Tabsdata run the following command:

```
$ td fn trigger --collection examples --name pub
```

In Tabsdata binary distribution, every time the `pub` publisher is executed, the `tfr` transformer
and the `sub` subscriber will also be executed.

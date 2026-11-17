#
# Copyright 2026 Tabsdata Inc.
#

"""AWS Glue connector -- public API.

Two classes, imported from `tabsdatak.conn.awsglue`:

* `AwsGlueDestConn` -- the connection: the AWS `region` and
  `credentials` (`AwsAccessSecretKey`) of the Glue Data Catalog, the S3
  `bucket` and `base_path` the table files are written under, and an
  optional default `database`.
* `AwsGlueDest` -- the `@subscriber` destination: a list of target
  `tables`, one output slot per table. Each is an Apache Iceberg table
  on S3, registered in the AWS Glue Data Catalog.

Once a table is registered in the Glue Data Catalog it can be queried
with Amazon Athena, and with any other engine that reads the catalog
(Amazon Redshift Spectrum, Amazon EMR, AWS Glue jobs, Spark, Trino).

The connection (with credentials) is registered on a collection, not
passed to a decorator; the runtime resolves it at run time. A flow
function only references `AwsGlueDest`.

Examples:
    .. code-block:: python

        @subscriber(destination=AwsGlueDest(tables=["sales.orders"]),
                    input_tables=["mycoll/orders"])
        def publish(orders: TableFrameSpec) -> TableFrameSpec:
            return orders
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.api import (
    Dest,
    StrOrSecretSpec,
)

# noinspection PyProtectedMember
from tabsdatak.conn.awsglue import _validation
from tabsdatak.conn.awsglue.error import AwsGlueErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._schema.matrix import Resolution, resolve_dest

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import (
    AwsAccessSecretKey,
    AwsRegionSpec,
    CfgSpec,
    CloudRelativePathSpec,
    S3BucketSpec,
    _convert,
)
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------

CredentialsSpec: TypeAlias = AwsAccessSecretKey
"""The credential type for AWS Glue connections: `AwsAccessSecretKey`
(access key ID + secret access key), used for both the Glue Data Catalog
and the S3 bucket."""

DatabaseSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    _validation.DATABASE_VALIDATION,
]
"""An AWS Glue database name: 1-255 characters, lowercase letters, digits
and underscores -- the names Amazon Athena reads without quoting. Accepts
a plain string or a `Secret`."""

CatalogIdSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    _validation.CATALOG_ID_VALIDATION,
]
"""The id of an AWS Glue Data Catalog: the 12-digit id of the AWS account
that owns it. Accepts a plain string or a `Secret`."""

AwsGlueTableNameSpec: TypeAlias = Annotated[str, _validation.TABLE_VALIDATION]
"""An AWS Glue table name, `table` or `database.table`; each part is 1-255
characters, lowercase letters, digits and underscores."""

AwsGlueTableNamesSpec: TypeAlias = Annotated[
    list[AwsGlueTableNameSpec], _validation.DISTINCT_TABLES_VALIDATION
]
"""A list of AWS Glue table names (see `AwsGlueTableNameSpec`), each at
most once."""

IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
SchemaEvolutionSpec: TypeAlias = Literal["update", "strict"]


# --- AwsGlueDest.dest_cfg keys --------------------------------------
DestCfgKey: TypeAlias = Literal["awsglue.logging_level",]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
]


# --- Connection ------------------------------------------------------


@_api.dataclass(AwsGlueErrorCode.AWSGLUE_1, kw_only=True)
class AwsGlueDestConn(Conn):
    """AWS Glue Data Catalog connection used by `AwsGlueDest`.

    :categories: destination

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time. Each string field accepts a plain string or a
    `Secret`.

    The tables written through this connection are Apache Iceberg tables
    registered in the AWS Glue Data Catalog, so they can be queried with
    Amazon Athena and any other engine that reads the catalog.

    Args:
        region: The AWS region of the Glue Data Catalog and of the
            bucket, e.g. `us-east-1`.
        credentials: AWS `AwsAccessSecretKey` credentials, used for both
            the Glue Data Catalog and the bucket.
        bucket: The S3 bucket the table files are written to.
        base_path: Object-key prefix from the bucket root under which
            the table files are written; starts with `/`, has no
            trailing `/` and no empty segments (`//`). A table the
            destination creates is located at
            `s3://<bucket><base_path>/<database>/<table>`. Defaults to
            `/`.
        database: Optional default Glue database, used by the table
            names that leave the database out. A Glue database name
            (1-255 characters, lowercase letters, digits and
            underscores). The database must already exist.
        catalog_id: Optional id of the Glue Data Catalog: the 12-digit
            id of the AWS account that owns it. Defaults to the catalog
            of the account the credentials belong to.
        conn_cfg: Optional connector-config dict (see `CfgSpec`). A key
            prefixed `obstore.` is forwarded, without the prefix, to the
            object store client that writes the data files (e.g.
            `"obstore.endpoint"`). A key prefixed `pyiceberg.` is
            forwarded, without the prefix, to the Iceberg catalog as one
            of its properties: `glue.*` and `client.*` properties reach
            the Glue client (e.g. `"pyiceberg.glue.endpoint"`,
            `"pyiceberg.glue.max-retries"`), and `s3.*` properties
            reach the client that writes the Iceberg metadata files
            (e.g. `"pyiceberg.s3.endpoint"`).

    Examples:
        The connection document registered on a collection, with the
        credentials read from the environment when it is created:

        .. code-block:: yaml

            kind: connectionDef
            apiVersion: '1.0'
            type: tabsdatak.conn.awsglue:AwsGlueDestConn
            spec:
              region: 'str:us-east-1'
              credentials:
                kind: awsAccessSecretKeyCredentials
                apiVersion: '1.0'
                type: tabsdatak.conn.common.types:AwsAccessSecretKey
                spec:
                  access_key_id: '$secret:AWS_ACCESS_KEY_ID'
                  secret_access_key: '$secret:AWS_SECRET_ACCESS_KEY'
              bucket: 'str:my-lake'
              base_path: 'str:/warehouse'
              database: 'str:sales'
    """

    region: AwsRegionSpec
    credentials: CredentialsSpec
    bucket: S3BucketSpec
    base_path: CloudRelativePathSpec = "/"
    database: DatabaseSpec | None = None
    catalog_id: CatalogIdSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _region(self) -> str:
        return _convert(self.region, str)

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)

    def _database(self) -> str | None:
        return None if self.database is None else _convert(self.database, str)

    def _catalog_id(self) -> str | None:
        return None if self.catalog_id is None else _convert(self.catalog_id, str)


# --- Destination -----------------------------------------------------


@_api.dataclass(AwsGlueErrorCode.AWSGLUE_2, kw_only=True)
class AwsGlueDest(Dest):
    """AWS Glue destination for `@subscriber` -- one output slot per table.

    :categories: destination

    Each table is an Apache Iceberg table on S3, registered in the AWS
    Glue Data Catalog. Once registered it can be queried with Amazon
    Athena, and with any other engine that reads the catalog (Amazon
    Redshift Spectrum, Amazon EMR, AWS Glue jobs, Spark, Trino).

    A write is one Iceberg snapshot per table: readers see either the
    table as it was or the whole batch, never part of it. A table that
    does not exist is created from the batch, under the connection's
    `bucket` and `base_path`; its database must already exist.

    A write is idempotent. The snapshot it commits records the id of the
    trx it belongs to, and a rerun that finds a snapshot of that trx in
    the table writes nothing.

    Args:
        tables: Target table names, one write slot per entry. Each is
            `table` or `database.table`; a bare `table` is completed
            from the connection's `database`. Each part is 1-255
            characters, lowercase letters, digits and underscores.
        if_table_exists: `"append"` (the default) adds rows;
            `"replace"` empties the table and writes the batch, keeping
            the table and its columns. The files of the rows a replace
            removes stay in S3 until the table's old snapshots are
            expired.
        schema_evolution: What a write does when the batch's columns differ from
            the table's, a replace included. `"strict"` (the default) refuses a
            batch with a column the table lacks or a wider type, before a replace
            empties the table; `"update"` adds the column and widens the type,
            and refuses a change Iceberg cannot make. A column the batch lacks
            is read NULL either way.
        dest_cfg: Optional connector config; the only supported key is
            `awsglue.logging_level`.

    Schema evolution:
        What each written `schema_evolution`, and engine, resolves to
        per `if_table_exists`; registration refuses every INVALID and
        PLANNED combination and reports the one it resolves:

        .. code-block:: text

            +----------------+---------------+---------------+
            |                | append        | replace       |
            +----------------+---------------+---------------+
            | engine         | native        | native        |
            | left out       | strict        | strict        |
            | strict         | strict        | strict        |
            | update         | update/native | update/native |
            | update+iceberg | INVALID       | INVALID       |
            | other engine   | INVALID       | INVALID       |
            +----------------+---------------+---------------+

    Examples:
        Append each run's rows to an Iceberg table in the `sales`
        database:

        .. code-block:: python

            @subscriber(
                input_tables=["mycoll/orders"],
                destination=AwsGlueDest(tables=["sales.orders"]),
            )
            def publish(orders: TableFrameSpec) -> TableFrameSpec:
                return orders

        Overwrite a table on each run; the name leaves the database
        out, so the connection's `database` is used:

        .. code-block:: python

            @subscriber(
                input_tables=["mycoll/daily_totals"],
                destination=AwsGlueDest(
                    tables=["daily_totals"],
                    if_table_exists="replace",
                ),
            )
            def publish(totals: TableFrameSpec) -> TableFrameSpec:
                return totals

        Append rows and let the table gain the new columns of a batch
        via `schema_evolution`:

        .. code-block:: python

            @subscriber(
                input_tables=["mycoll/orders_v2"],
                destination=AwsGlueDest(
                    tables=["sales.orders"],
                    schema_evolution="update",
                ),
            )
            def evolve(orders: TableFrameSpec) -> TableFrameSpec:
                return orders

        Write two tables from one function, one output slot each:

        .. code-block:: python

            @subscriber(
                input_tables=["mycoll/orders", "mycoll/customers"],
                destination=AwsGlueDest(
                    tables=["sales.orders", "sales.customers"],
                ),
            )
            def publish(
                orders: TableFrameSpec, customers: TableFrameSpec
            ) -> tuple[TableFrameSpec, TableFrameSpec]:
                return orders, customers

        Query a written table from Amazon Athena:

        .. code-block:: sql

            SELECT * FROM sales.orders LIMIT 10;
    """

    tables: AwsGlueTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "strict"
    dest_cfg: DestCfgSpec | None = None

    def validate(self) -> None:
        """Cross-field validation: the disposition and policy form a
        combination the destination offers.

        Called by the framework; raises `SqlCommonValidateException`.
        """
        self._evolution()

    def _evolution(self) -> Resolution:
        """The disposition, policy and engine a write applies, resolved
        against the schema evolution matrix."""
        return resolve_dest("awsglue", self)

    def _materialized(self) -> dict[str, str]:
        return self._evolution().options()

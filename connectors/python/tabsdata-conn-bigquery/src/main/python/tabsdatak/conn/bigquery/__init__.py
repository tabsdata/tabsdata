#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery connector -- public API.

Four classes, imported from `tabsdatak.conn.bigquery`:

* `BigQueryDestConn` -- the connection: a GCS `staging` location,
  GCP `credentials`, and optional default `project` / `dataset`.
* `BigQueryDest` -- the `@subscriber` destination: a list of target
  `tables`, one output slot per table. Writes go via the GCS staging
  bucket and a `load_table_from_uri` load job.
* `CloudLocation` and its concrete form `GCSLocation` -- the
  `staging` bucket the destination lands parquet in.

BigQuery is destination-only -- there is no `BigQuerySrc`. The
connection (with credentials) is registered on a collection, not
passed to a decorator; the runtime resolves it at run time. A flow
function only references `BigQueryDest`.

Examples:
    .. code-block:: python

        @subscriber(destination=BigQueryDest(tables=["`cap_rows`"]),
                    input_tables=["mycoll/cities"])
        def publish(rows: TableFrameSpec) -> TableFrameSpec:
            return rows
"""

from __future__ import annotations

from abc import ABC
from collections.abc import Mapping
from typing import Annotated, Any, Literal, TypeAlias

# noinspection PyProtectedMember
import tabsdatak._api as _api

# noinspection PyProtectedMember
from tabsdatak.api import Dest, StrOrSecretSpec

# noinspection PyProtectedMember
from tabsdatak.conn.bigquery import _validation
from tabsdatak.conn.bigquery.error import BigqueryErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import (
    CfgSpec,
    EntityWhitelistSpec,
    FullPathSpec,
    GCPJson,
    GCSBucketSpec,
    StrOrSecretConstraints,
    _convert,
)
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = GCPJson
"""The credential type for BigQuery connections: `GCPJson`
(service-account JSON key)."""


ProjectSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.PROJECT_PATTERN)
]

DatasetSpec: TypeAlias = Annotated[
    StrOrSecretSpec, StrOrSecretConstraints(pattern=_validation.DATASET_PATTERN)
]

BigQueryTableNameSpec: TypeAlias = Annotated[
    str, _validation.BIGQUERY_TABLE_FQN_VALIDATOR
]
BigQueryTableNamesSpec: TypeAlias = list[BigQueryTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace"]
SchemaEvolutionSpec: TypeAlias = Literal["update", "strict"]


# --- BigQueryDest.dest_cfg keys -------------------------------------
DestCfgKey: TypeAlias = Literal["bigquery.logging_level",]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATOR
]


# --- Cloud locations -------------------------------------------------
#
# BigQuery only stages via GCS, but we keep the CloudLocation ABC
# parallel to the other connectors so the import + dispatch surface is
# consistent and so future location flavours can be added without
# refactoring callers.


class CloudLocation(_api.DescriptorBase, ABC):
    """Abstract base for a BigQuery `staging` location.

    :categories: destination

    The only concrete form today is `GCSLocation` -- BigQuery stages
    exclusively via GCS. Kept parallel to the other connectors so the
    import / dispatch surface stays consistent.
    """

    __kind__ = "bigQueryCloudLocation"


@_api.dataclass(BigqueryErrorCode.BIGQUERY_8, kw_only=True)
class GCSLocation(CloudLocation):
    """GCS staging area for `BigQueryDest`.

    :categories: destination

    The plugin uploads parquet here, then runs a `load_table_from_uri`
    load job against BigQuery referencing the object.

    Args:
        bucket: A valid GCS bucket name.
        credentials: GCP service-account JSON credentials.
        base_path: Object-key prefix from the bucket root; starts with
            `/` and has no trailing `/` (the leading `/` is
            trimmed when forming the key). Defaults to `/`.
        cfg: Optional connector-config dict for the GCS client.
    """

    __kind__ = "bigQueryGcsLocation"
    bucket: GCSBucketSpec
    credentials: GCPJson
    base_path: FullPathSpec = "/"
    cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


# --- Connection ------------------------------------------------------


@_api.dataclass(BigqueryErrorCode.BIGQUERY_1, kw_only=True)
class BigQueryDestConn(Conn):
    """BigQuery connection.

    :categories: destination

    Registered on a collection; the runtime resolves it and hands it to
    the plugin at run time. Each string field accepts a plain string or
    a `Secret`.

    Args:
        staging: A `GCSLocation` used as the parquet staging bucket
            for load jobs.
        credentials: GCP service-account JSON credentials (`GCPJson`).
        project: Optional default GCP project id (6-30 characters,
            lowercase letters, digits and hyphens, starting with a
            letter, ending alphanumeric). Required when `dataset` is
            set.
        dataset: Optional default BigQuery dataset id (1-1024
            characters, letters, digits and underscores).
        entity_whitelist: Optional list of regex patterns narrowing
            which tables the connector considers; `None` (the default)
            filters nothing.
        conn_cfg: Optional connector-config dict (see `CfgSpec`).
    """

    staging: GCSLocation
    credentials: CredentialsSpec
    project: ProjectSpec | None = None
    dataset: DatasetSpec | None = None
    entity_whitelist: EntityWhitelistSpec | None = None
    conn_cfg: CfgSpec | None = None

    def _project(self) -> str:
        return _convert(self.project, str)

    def _dataset(self) -> str:
        return _convert(self.dataset, str)

    def validate(self) -> None:
        """Cross-field validation: `dataset` requires `project`.

        Called by the framework; raises `BigQueryValidateException`.
        """
        if self.dataset is not None and self.project is None:
            # noinspection PyProtectedMember
            raise BigqueryErrorCode.BIGQUERY_2.exception(dataset=self.dataset)


# --- Destination -----------------------------------------------------


@_api.dataclass(BigqueryErrorCode.BIGQUERY_3, kw_only=True)
class BigQueryDest(Dest):
    """BigQuery destination for `@subscriber` -- one output slot per table.

    :categories: destination

    Args:
        tables: Target table names, one write slot per entry. Each is
            `[[project.]dataset.]table`; a bare `table` or
            `dataset.table` is completed from the connection's
            `project` / `dataset` defaults. BigQuery wraps the whole
            name in one backtick pair, e.g. `my-project.ds.t`.
        if_table_exists: `"append"` (the default) adds rows;
            `"replace"` drops and recreates the table.
        schema_evolution: `"update"` (the default) allows new columns;
            `"strict"` rejects schema changes.
        dest_cfg: Optional connector config; the only supported key is
            `bigquery.logging_level`.

    Examples:
        Overwrite the target table each run with `if_table_exists="replace"`:

        .. code-block:: python

            @subscriber(
                destination=BigQueryDest(
                    tables=["`cap_replace`"],
                    if_table_exists="replace",
                ),
                input_tables=["caps_input/cap_rows"],
            )
            def publish(rows: TableFrameSpec) -> TableFrameSpec:
                return rows

        Append a wider frame; `schema_evolution="update"` adds the new column:

        .. code-block:: python

            @subscriber(
                destination=BigQueryDest(
                    tables=["`cap_evolve`"],
                    if_table_exists="append",
                    schema_evolution="update",
                ),
                input_tables=["caps_input/cap_evo_v2"],
            )
            def evolve(v2: TableFrameSpec) -> TableFrameSpec:
                return v2
    """

    tables: BigQueryTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "update"
    dest_cfg: DestCfgSpec | None = None

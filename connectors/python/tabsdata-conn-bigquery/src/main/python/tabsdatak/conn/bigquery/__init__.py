#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery connector -- public API.

Five classes, imported from `tabsdatak.conn.bigquery`:

* `BigQueryDestConn` -- the connection: a GCS `staging` location,
  GCP `credentials`, and optional default `project` / `dataset`.
* `BigQueryDest` -- the `@subscriber` destination: a list of target
  `tables`, one output slot per table. Writes go via the GCS staging
  bucket and a `load_table_from_uri` load job.
* `CloudLocation` and its concrete form `GCSLocation` -- the
  `staging` bucket the destination lands parquet in.
* `MergeCfg` -- how `BigQueryDest(if_table_exists="merge")` matches
  incoming rows onto the target table.

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
from tabsdatak.conn.bigquery.error import BigQueryErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation as _common_validation

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import MergeCfg as SqlMergeCfg

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import (
    CfgSpec,
    CloudRelativePathSpec,
    EntityWhitelistSpec,
    GCPJson,
    GCSBucketSpec,
    _convert,
)
from tabsdatak.spi import Conn

# --- Type aliases ----------------------------------------------------


CredentialsSpec: TypeAlias = GCPJson
"""The credential type for BigQuery connections: `GCPJson`
(service-account JSON key)."""


ProjectSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    _validation.PROJECT_VALIDATION,
]

DatasetSpec: TypeAlias = Annotated[
    StrOrSecretSpec,
    _validation.DATASET_VALIDATION,
]

BigQueryTableNameSpec: TypeAlias = Annotated[
    str, _validation.BIGQUERY_TABLE_FQN_VALIDATION
]
BigQueryTableNamesSpec: TypeAlias = list[BigQueryTableNameSpec]
IfTableExistsSpec: TypeAlias = Literal["append", "replace", "merge"]
SchemaEvolutionSpec: TypeAlias = Literal["update", "strict"]

# A merge column is one BigQuery column name -- standard, or
# backtick-quoted for a flexible name. A dotted value is rejected
# instead of being read as a qualified reference.
MergeColumnSpec: TypeAlias = Annotated[str, _validation.MERGE_COLUMN_VALIDATOR]
MergeColumnsSpec: TypeAlias = list[MergeColumnSpec]


# --- BigQueryDest.dest_cfg keys -------------------------------------
DestCfgKey: TypeAlias = Literal["bigquery.logging_level",]

DestCfgSpec: TypeAlias = Annotated[
    Mapping[DestCfgKey, Any], _common_validation.CONN_CFG_VALIDATION
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


@_api.dataclass(BigQueryErrorCode.BIGQUERY_8, kw_only=True)
class GCSLocation(CloudLocation):
    """GCS staging area for `BigQueryDest`.

    :categories: destination

    The plugin uploads parquet here, then runs a `load_table_from_uri`
    load job against BigQuery referencing the object.

    Args:
        bucket: A valid GCS bucket name.
        credentials: GCP service-account JSON credentials.
        base_path: Object-key prefix from the bucket root; starts with
            `/`, has no trailing `/` and no empty segments (`//`)
            (the leading `/` is trimmed when forming the key).
            Defaults to `/`.
        cfg: Optional connector-config dict for the GCS client.
    """

    __kind__ = "bigQueryGCSLocation"
    bucket: GCSBucketSpec
    credentials: GCPJson
    base_path: CloudRelativePathSpec = "/"
    cfg: CfgSpec | None = None

    def _bucket(self) -> str:
        return _convert(self.bucket, str)

    def _base_path(self) -> str:
        return _convert(self.base_path, str)


# --- Merge -----------------------------------------------------------


@_api.dataclass(BigQueryErrorCode.BIGQUERY_11, kw_only=True)
class MergeCfg(SqlMergeCfg):
    """How `if_table_exists="merge"` matches incoming rows onto a target table.

    :categories: destination

    The BigQuery spelling of the shared `MergeCfg`. Columns are named as
    BigQuery resolves them: case-insensitively, so `on=["order_id"]` hits
    `ORDER_ID` and `OrderId` alike -- a table cannot hold two columns whose
    names differ only in case. Backticks are for a flexible column name; unlike
    Snowflake, quoting does not make a name case-sensitive.

    Every incoming row carries a marker in `op_column` saying what to do with it,
    and the marker is what decides which clause claims the row:

    * `'i'` -- insert the row, if it matches no key.
    * `'u'` -- update the row it matches.
    * `'d'` -- delete the row it matches.

    Args:
        on: The key columns rows are matched by, joined with `AND`. Must be
            present on both sides. Each is a BigQuery column name -- bare, or
            backtick-quoted for a flexible name.
        op_column: The column holding each row's `i` / `u` / `d` marker. Read
            to decide each row's fate and never written, so the target does not
            need the column and will not gain it.
        watermark_column: An optional column stamped with the id of the trx the
            merge belongs to, making the merge idempotent: a rerun that finds
            the target already carrying this trx's id writes nothing rather than
            applying the same batch twice. The target gains the column under
            either `schema_evolution` setting. Rows written before it
            existed keep a NULL there.

    Examples:
        A CDC feed whose `op` column says what to do with each row:

        .. code-block:: python

            MergeCfg(on=["order_id", "customer_id"], op_column="op")
    """

    __kind__ = "bigQueryMergeCfg"

    # Re-annotated so the shared fields carry the BigQuery column shape: a
    # value that is not a BigQuery column name is rejected here rather than
    # surfacing as invalid SQL when the MERGE is built.
    on: MergeColumnsSpec
    op_column: MergeColumnSpec
    watermark_column: MergeColumnSpec | None = None

    def _fold(self, name: str) -> str:
        return _validation.merge_column_key(name)


MergeCfgSpec: TypeAlias = MergeCfg | list[MergeCfg]
"""One `MergeCfg` applied to every target table, or one per entry in
`BigQueryDest.tables`."""


# --- Connection ------------------------------------------------------


@_api.dataclass(BigQueryErrorCode.BIGQUERY_1, kw_only=True)
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
            raise BigQueryErrorCode.BIGQUERY_2.exception(dataset=self.dataset)


# --- Destination -----------------------------------------------------


@_api.dataclass(BigQueryErrorCode.BIGQUERY_3, kw_only=True)
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
            `"replace"` drops and recreates the table;
            `"merge"` merges rows.
        schema_evolution: `"strict"` (the default) rejects schema changes;
            `"update"` allows new columns.
        merge_cfg: Required by -- and only valid with --
            `if_table_exists="merge"`: how incoming rows are matched
            onto existing ones. One `MergeCfg` applies to every table;
            pass a list to configure each table in `tables` separately.
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

        Merge mode merges incoming rows onto the target by the keys in `merge_cfg`:

        .. code-block:: python

            @subscriber(
                destination=BigQueryDest(
                    tables=["`cap_merge`"],
                    if_table_exists="merge",
                    merge_cfg=MergeCfg(
                        on=["cap_id"],
                        op_column="op",
                    ),
                ),
                input_tables=["caps_input/cap_merge_v2"],
            )
            def merge(rows: TableFrameSpec) -> TableFrameSpec:
                return rows
    """

    tables: BigQueryTableNamesSpec
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "strict"
    merge_cfg: MergeCfgSpec | None = None
    dest_cfg: DestCfgSpec | None = None

    def _merge_cfgs(self) -> list[MergeCfg] | None:
        """One `MergeCfg` per entry in `tables`, or `None` outside merge mode."""
        if self.merge_cfg is None:
            return None
        if isinstance(self.merge_cfg, MergeCfg):
            return [self.merge_cfg] * len(self.tables)
        return list(self.merge_cfg)

    def validate(self) -> None:
        """Cross-field validation: `merge_cfg` and `if_table_exists="merge"`
        require each other, and a list of configs must line up with `tables`.

        Called by the framework; raises `BigQueryValidateException`.
        """
        merging = self.if_table_exists == "merge"
        if merging and self.merge_cfg is None:
            # noinspection PyProtectedMember
            raise BigQueryErrorCode.BIGQUERY_12.exception()
        if not merging and self.merge_cfg is not None:
            # noinspection PyProtectedMember
            raise BigQueryErrorCode.BIGQUERY_13.exception(
                if_table_exists=self.if_table_exists
            )
        if isinstance(self.merge_cfg, list) and len(self.merge_cfg) != len(self.tables):
            # noinspection PyProtectedMember
            raise BigQueryErrorCode.BIGQUERY_14.exception(
                cfgs=len(self.merge_cfg), tables=len(self.tables)
            )

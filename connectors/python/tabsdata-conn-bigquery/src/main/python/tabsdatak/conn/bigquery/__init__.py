#
# Copyright 2026 Tabsdata Inc.
#

"""BigQuery connector — public API.

* :class:`BigQueryDestConn` -- per-project BigQuery connection
  (project + GCS staging + GCP credentials).
* :class:`BigQueryDest` -- destination: list of table names
  (``[[project.]dataset.]table``, partial names completed from the
  connection's ``project`` / ``dataset`` defaults); one parquet slot
  per table. Writes go via a GCS staging bucket and a
  ``load_table_from_uri`` load job.

No ``BigQuerySrc`` today. Credential fields accept ``Secret``
references.
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
    __kind__ = "bigQueryCloudLocation"


@_api.dataclass(BigqueryErrorCode.BIGQUERY_8, kw_only=True)
class GCSLocation(CloudLocation):
    """GCS staging area used by ``BigQueryDest``. The plugin uploads
    parquet files here, then issues a ``LOAD DATA`` /
    ``load_table_from_uri`` against BigQuery referencing the
    object.

    :param bucket: GCS bucket name.
    :param credentials: GCP service-account JSON credentials.
    :param base_path: Full path (object-key prefix) from the bucket
        root (e.g. ``"/staging/bigquery"``): starts with ``/``, no
        trailing ``/``. The leading ``/`` is trimmed when forming the
        object key. Defaults to ``/``.
    :param cfg: Optional cfg dict passed to the underlying GCS
        client.
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

    :param staging: :class:`GCSLocation` used as the parquet
        staging bucket for load jobs.
    :param credentials: GCP service-account JSON
        (:class:`GCPJson`).
    :param project: Default GCP project. Optional.
    :param dataset: Default BigQuery dataset. Optional.
    :param entity_whitelist: Optional regex-based table filter.
    :param conn_cfg: Optional driver-side config dict.
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
        if self.dataset is not None and self.project is None:
            # noinspection PyProtectedMember
            raise BigqueryErrorCode.BIGQUERY_2.exception(dataset=self.dataset)


# --- Destination -----------------------------------------------------


@_api.dataclass(BigqueryErrorCode.BIGQUERY_3, kw_only=True)
class BigQueryDest(Dest):
    """BigQuery destination — one parquet slot per target table.

    :param tables: Table names (``[[project.]dataset.]table``). A
        bare ``table`` or ``dataset.table`` is completed from the
        connection's ``project`` / ``dataset`` defaults. One slot per
        table.
    :param if_table_exists: ``"append"`` (default) or
        ``"replace"`` (drop + recreate).
    :param schema_evolution: ``"update"`` (default; allow new
        columns) or ``"strict"`` (reject schema changes).
    :param dest_cfg: Optional dest cfg dict; key set is
        ``"bigquery.logging_level"``.
    """

    tables: BigQueryTableNamesSpec | None = None
    if_table_exists: IfTableExistsSpec = "append"
    schema_evolution: SchemaEvolutionSpec = "update"
    dest_cfg: DestCfgSpec | None = None

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from tabsdatak.conn.clickhouse import (
    AzBlobLocation,
    ClickHouseDestConn,
    CloudLocation,
    GCSLocation,
    S3Location,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._store import forwarded
from tabsdatak.conn.common.types import _convert

# All driver-touching imports (google-cloud-storage /
# clickhouse-connect) are made inside the call-sites of the private
# helper modules below.  The conn-clickhouse package stays cheap to
# import for registry scans.


# --- Conn helpers -------------------------------------------------


def _build_clickhouse_config(conn: ClickHouseDestConn):
    """Build a private :class:`ClickHouseConfig` from a new-model
    :class:`ClickHouseDestConn`."""
    # noinspection PyProtectedMember
    from tabsdatak.conn.clickhouse._clickhouse_config import (
        ClickHouseConfig,
    )

    return ClickHouseConfig(
        host=conn._host(),
        port=conn._port(),
        database=conn._database(),
        username=_convert(conn.credentials.user, str),
        password=_convert(conn.credentials.password, str),
        secure=conn._secure(),
    )


def build_cloud_storage(staging: CloudLocation):
    """Build a private :class:`ClickHouseCloudStorage` from a new-model
    :class:`CloudLocation`."""
    # noinspection PyProtectedMember
    from tabsdatak.conn.clickhouse._cloud_storage import (
        AzureClickHouseStorage,
        GCSClickHouseStorage,
        S3ClickHouseStorage,
    )

    # noinspection PyProtectedMember
    from tabsdatak.conn.clickhouse._storage_config import (
        AzureConfig,
        GCSConfig,
        S3Config,
    )

    if isinstance(staging, S3Location):
        s3 = S3Config(
            bucket=staging._bucket(),
            region=staging._region(),
            base_path=staging._base_path(),
            access_key_id=_convert(staging.credentials.access_key_id, str),
            secret_access_key=(_convert(staging.credentials.secret_access_key, str)),
            options=forwarded(f"s3://{staging._bucket()}", staging.cfg),
        )
        return S3ClickHouseStorage(s3)

    if isinstance(staging, AzBlobLocation):
        azure = AzureConfig(
            container=staging._container(),
            base_path=staging._base_path(),
            account_name=_convert(staging.credentials.account_name, str),
            account_key=_convert(staging.credentials.account_key, str),
            options=forwarded(f"az://{staging._container()}", staging.cfg),
        )
        return AzureClickHouseStorage(azure)

    if isinstance(staging, GCSLocation):
        gcs = GCSConfig(
            bucket=staging._bucket(),
            base_path=staging._base_path(),
            service_account_json=_convert(staging.credentials.json, str),
            hmac_access_key=_convert(staging.hmac_credentials.access_key, str),
            hmac_secret_key=_convert(staging.hmac_credentials.secret_key, str),
            options=forwarded(f"gs://{staging._bucket()}", staging.cfg),
        )
        return GCSClickHouseStorage(gcs)

    raise TypeError(
        f"Unsupported ClickHouse staging location: {type(staging).__name__}"
    )


# --- Dest: write impl ---------------------------------------------


@dataclass(kw_only=True)
class ClickHouseWriteCfg:
    """Carrier for the parameters needed by the ClickHouse write side.

    The legacy ClickHouse runtime goes through a cloud-staged bulk
    loader rather than SQLAlchemy, so this does not inherit from
    ``SqlWriteCfg``.
    """

    conn: ClickHouseDestConn
    target_tables: list[str]
    if_table_exists: str  # "append" | "replace"
    schema_evolution: str
    function_id: str
    transaction_id: str


def write_via_bulk_loader(
    cfg: ClickHouseWriteCfg,
    tables: list[pl.LazyFrame],
) -> None:
    """Drive the private :class:`CloudStagedBulkLoader` for ClickHouse.

    ``tables[i]`` is staged as Parquet and loaded into
    ``cfg.target_tables[i]`` via the connection's staging location, as
    ``<function_id>/<transaction_id>_<i>.parquet``. The function's folder is
    its own, and two runs of one function never overlap, so clearing it
    touches no other load. The caller is expected to have validated the
    lengths match.
    """
    # noinspection PyProtectedMember
    from tabsdatak.conn.clickhouse._bulk_loader import (
        CloudStagedBulkLoader,
    )

    # noinspection PyProtectedMember
    from tabsdatak.conn.clickhouse._dialect import ClickHouseDialect

    # noinspection PyProtectedMember
    from tabsdatak.conn.clickhouse._modes import WriteMode

    cloud_storage = build_cloud_storage(cfg.conn.staging)
    ch_config = _build_clickhouse_config(cfg.conn)
    dialect = ClickHouseDialect(ch_config, cloud_storage)

    write_mode = (
        WriteMode.REPLACE if cfg.if_table_exists == "replace" else WriteMode.APPEND
    )

    loader = CloudStagedBulkLoader(
        cloud_storage=cloud_storage,
        dialect=dialect,
        write_mode=write_mode,
        schema_evolution=cfg.schema_evolution,
        staging_folder=cfg.function_id,
    )

    table_frames: dict[str, list[tuple[str, pl.LazyFrame]]] = {}
    for position, (name, frame) in enumerate(zip(cfg.target_tables, tables)):
        filename = f"{cfg.transaction_id}_{position}.parquet"
        table_frames.setdefault(name, []).append((filename, frame))
    loader.load(table_frames)

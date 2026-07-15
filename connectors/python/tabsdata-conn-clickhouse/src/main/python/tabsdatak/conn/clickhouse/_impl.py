from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from tabsdatak.conn.clickhouse import (
    AzBlobLocation,
    ClickHouseDestConn,
    CloudLocation,
    GCSLocation,
    S3Location,
)

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert

# All driver-touching imports (boto3 / azure / google-cloud-storage /
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
        cfg = S3Config(
            bucket=staging._bucket(),
            region=staging._region(),
            base_path=staging._base_path(),
            access_key_id=_convert(staging.credentials.access_key_id, str),
            secret_access_key=(_convert(staging.credentials.secret_access_key, str)),
        )
        return S3ClickHouseStorage(cfg)

    if isinstance(staging, AzBlobLocation):
        cfg = AzureConfig(
            container=staging._container(),
            base_path=staging._base_path(),
            account_name=_convert(staging.credentials.account_name, str),
            account_key=_convert(staging.credentials.account_key, str),
        )
        return AzureClickHouseStorage(cfg)

    if isinstance(staging, GCSLocation):
        cfg = GCSConfig(
            bucket=staging._bucket(),
            base_path=staging._base_path(),
            service_account_json=_convert(staging.credentials.json, str),
            hmac_access_key=_convert(staging.hmac_credentials.access_key, str),
            hmac_secret_key=_convert(staging.hmac_credentials.secret_key, str),
        )
        return GCSClickHouseStorage(cfg)

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
    schema_evolution: str  # "strict" | "iceberg"


def write_via_bulk_loader(
    cfg: ClickHouseWriteCfg,
    tables: list[Path],
) -> None:
    """Drive the private :class:`CloudStagedBulkLoader` for ClickHouse.

    Parquet files at ``tables[i]`` are loaded into
    ``cfg.target_tables[i]`` via the connection's staging location.
    The caller is expected to have validated the lengths match.
    """
    # noinspection PyProtectedMember
    from tabsdatak.conn.clickhouse._bulk_loader import (
        CloudStagedBulkLoader,
    )

    # noinspection PyProtectedMember
    from tabsdatak.conn.clickhouse._dialect import ClickHouseDialect

    # noinspection PyProtectedMember
    from tabsdatak.conn.clickhouse._id import new_upload_id

    # noinspection PyProtectedMember
    from tabsdatak.conn.clickhouse._modes import (
        SchemaEvolutionMode,
        WriteMode,
    )

    cloud_storage = build_cloud_storage(cfg.conn.staging)
    ch_config = _build_clickhouse_config(cfg.conn)
    dialect = ClickHouseDialect(ch_config, cloud_storage)

    write_mode = (
        WriteMode.REPLACE if cfg.if_table_exists == "replace" else WriteMode.APPEND
    )
    schema_evo = (
        SchemaEvolutionMode.ICEBERG
        if cfg.schema_evolution == "iceberg"
        else SchemaEvolutionMode.NONE
    )

    # Time-ordered, lexicographically sortable per-load namespace
    # -- the GCS / S3 / Azure delete() sweep uses ``upload_id <=
    # up_to_id`` to clean up files from this run and orphans from
    # any earlier ones; that's only meaningful for UUIDv7-shaped
    # ids.
    upload_id = new_upload_id()

    loader = CloudStagedBulkLoader(
        cloud_storage=cloud_storage,
        dialect=dialect,
        write_mode=write_mode,
        schema_evolution=schema_evo,
        upload_id=upload_id,
    )

    table_files: dict[str, list[Path]] = {
        name: [path] for name, path in zip(cfg.target_tables, tables)
    }
    loader.load(table_files)

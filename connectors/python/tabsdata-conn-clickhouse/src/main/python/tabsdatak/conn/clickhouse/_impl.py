from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from tabsdatak.conn.clickhouse import (
    AzBlobLocation,
    ClickHouseConn,
    CloudLocation,
    GCSLocation,
    S3Location,
)

# All driver-touching imports (boto3 / azure / google-cloud-storage /
# clickhouse-connect) are made inside the call-sites of the private
# helper modules below.  The conn-clickhouse package stays cheap to
# import for registry scans.


# --- Conn helpers -------------------------------------------------


def _build_clickhouse_config(conn: ClickHouseConn):
    """Build a private :class:`ClickHouseConfig` from a new-model
    :class:`ClickHouseConn`."""
    # noinspection PyProtectedMember
    from tabsdatak.conn.clickhouse._clickhouse_config import (
        ClickHouseConfig,
    )

    host = (
        conn.host.value()
        if hasattr(conn.host, "value")
        else conn.host
    )
    database = (
        conn.database.value()
        if hasattr(conn.database, "value")
        else conn.database
    )
    user = conn.credentials.user.value()
    password = conn.credentials.password.value()

    return ClickHouseConfig(
        host=host,
        port=conn.port,
        database=database,
        username=user,
        password=password,
        secure=conn.secure,
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
            bucket=staging.bucket.value(),
            region=staging.region.value(),
            base_path=staging.base_path or "",
            access_key_id=staging.credentials.access_key_id.value(),
            secret_access_key=(
                staging.credentials.secret_access_key.value()
            ),
        )
        return S3ClickHouseStorage(cfg)

    if isinstance(staging, AzBlobLocation):
        cfg = AzureConfig(
            container=staging.container.value(),
            base_path=staging.base_path or "",
            account_name=staging.credentials.account_name.value(),
            account_key=staging.credentials.account_key.value(),
        )
        return AzureClickHouseStorage(cfg)

    if isinstance(staging, GCSLocation):
        cfg = GCSConfig(
            bucket=staging.bucket.value(),
            base_path=staging.base_path or "",
            service_account_json=staging.credentials.json.value(),
            hmac_access_key=staging.hmac_credentials.access_key.value(),
            hmac_secret_key=staging.hmac_credentials.secret_key.value(),
        )
        return GCSClickHouseStorage(cfg)

    raise TypeError(
        f"Unsupported ClickHouse staging location: "
        f"{type(staging).__name__}"
    )


# --- Dest: write impl ---------------------------------------------


@dataclass(kw_only=True)
class ClickHouseWriteCfg:
    """Carrier for the parameters needed by the ClickHouse write side.

    The legacy ClickHouse runtime goes through a cloud-staged bulk
    loader rather than SQLAlchemy, so this does not inherit from
    ``SqlWriteCfg``.
    """

    conn: ClickHouseConn
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
        WriteMode.REPLACE
        if cfg.if_table_exists == "replace"
        else WriteMode.APPEND
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

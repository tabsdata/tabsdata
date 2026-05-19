from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tabsdatak.api import Env, Secret
from tabsdatak.conn.common.types import (
    AzBlobLocation,
    CloudLocation,
    GCSLocation,
    S3Location,
    UserPassword,
)

logger = logging.getLogger(__name__)


# --- Value-resolution helpers ----------------------------------------


def _resolve(value: Any) -> str:
    if isinstance(value, (Secret, Env)):
        return value.value()
    return str(value)


# --- StarRocks SQL engine (MySQL protocol) ---------------------------


def build_starrocks_engine(
    host: str,
    port: int,
    database: str,
    credentials: UserPassword,
    connect_args: dict | None = None,
):
    from sqlalchemy import create_engine

    user = credentials.user.value()
    password = credentials.password.value()
    url = (
        f"mysql+mysqlconnector://{user}:{password}"
        f"@{host}:{port}/{database}"
    )
    return create_engine(url, connect_args=dict(connect_args or {}))


# --- FILES(...) expression builders ----------------------------------
#
# StarRocks accepts cloud-staged Parquet via the ``FILES()`` table
# function. The exact credential keys vary per cloud, but every
# variant shares the ``"path" = "<uri>"`` and ``"format" = "parquet"``
# entries.


def _kv(items: dict[str, str]) -> str:
    return ", ".join(f'"{k}" = "{v}"' for k, v in items.items())


def _common_directory(paths: list[str]) -> str:
    parts_list = [p.rsplit("/", 1)[0] for p in paths]
    if len(set(parts_list)) == 1:
        return parts_list[0]
    prefix = parts_list[0]
    for p in parts_list[1:]:
        while not p.startswith(prefix):
            prefix = prefix[: prefix.rfind("/")]
    return prefix


def _path_expr(uris: list[str]) -> str:
    if len(uris) == 1:
        return uris[0]
    return _common_directory(uris) + "/*"


def build_files_function(staging: CloudLocation, staged_uris: list[str]) -> str:
    if isinstance(staging, S3Location):
        bucket = _resolve(staging.bucket)
        region = _resolve(staging.region)
        creds = staging.credentials
        return "FILES(" + _kv({
            "path": _path_expr(staged_uris),
            "format": "parquet",
            "aws.s3.access_key": creds.access_key_id.value(),
            "aws.s3.secret_key": creds.secret_access_key.value(),
            "aws.s3.region": region,
        }) + ")"
    if isinstance(staging, AzBlobLocation):
        creds = staging.credentials
        return "FILES(" + _kv({
            "path": _path_expr(staged_uris),
            "format": "parquet",
            "azure.blob.shared_key": creds.account_key.value(),
        }) + ")"
    if isinstance(staging, GCSLocation):
        # StarRocks reads from GCS via the S3-compatible HMAC layer; the
        # ``s3a://`` paths are constructed by the caller before this
        # point. ``GCPJson`` credentials are not HMAC keys, so we fall
        # through to the unsupported branch -- GCS staging requires
        # users wire HMAC creds via a future credentials subclass.
        from tabsdatak.conn.starrocks.error import StarRocksErrorCode

        raise StarRocksErrorCode.STARROCKS_7.exception(
            kind="gcsLocation (HMAC required)",
        )
    from tabsdatak.conn.starrocks.error import StarRocksErrorCode

    raise StarRocksErrorCode.STARROCKS_7.exception(
        kind=type(staging).__name__,
    )


# --- Cloud-storage URI builders --------------------------------------
#
# Each branch reflects how StarRocks' ``FILES()`` expects the path to
# be expressed for that backend.


def staged_uri(staging: CloudLocation, key: str) -> str:
    rel = staging.full_path(key) if hasattr(staging, "full_path") else key.lstrip("/")
    if isinstance(staging, S3Location):
        bucket = _resolve(staging.bucket)
        return f"s3://{bucket}/{rel}"
    if isinstance(staging, AzBlobLocation):
        creds = staging.credentials
        container = _resolve(staging.container)
        account = creds.account_name.value()
        return f"wasbs://{container}@{account}.blob.core.windows.net/{rel}"
    if isinstance(staging, GCSLocation):
        bucket = _resolve(staging.bucket)
        return f"s3a://{bucket}/{rel}"
    from tabsdatak.conn.starrocks.error import StarRocksErrorCode

    raise StarRocksErrorCode.STARROCKS_7.exception(
        kind=type(staging).__name__,
    )


# --- Transporter upload helpers --------------------------------------
#
# The transporter binary handles cloud uploads. We construct one
# ``TransporterLocation`` per cloud backend; credential resolution
# happens in the caller and is passed in through ``env``.


def transporter_target(staging: CloudLocation, key: str):
    # Local imports keep the conn-common-file dependency optional for
    # plain Conn-construction users who never call write_out.
    from tabsdatak.conn.common.file import _transporter as t

    rel = staging.full_path(key) if hasattr(staging, "full_path") else key.lstrip("/")
    if isinstance(staging, S3Location):
        bucket = _resolve(staging.bucket)
        return t.TransporterS3(
            url=f"s3://{bucket}/{rel}",
            access_key=t.TransporterEnv(t.SERVER_SIDE_AWS_ACCESS_KEY_ID),
            secret_key=t.TransporterEnv(t.SERVER_SIDE_AWS_SECRET_ACCESS_KEY),
            region=t.TransporterEnv(t.SERVER_SIDE_AWS_REGION),
        )
    if isinstance(staging, AzBlobLocation):
        container = _resolve(staging.container)
        return t.TransporterAzure(
            url=f"az://{container}/{rel}",
            account_name=t.TransporterEnv(t.SERVER_SIDE_AZURE_ACCOUNT_NAME),
            account_key=t.TransporterEnv(t.SERVER_SIDE_AZURE_ACCOUNT_KEY),
        )
    if isinstance(staging, GCSLocation):
        bucket = _resolve(staging.bucket)
        return t.TransporterGCS(
            url=f"gs://{bucket}/{rel}",
            credentials=t.TransporterEnv(t.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON),
        )
    from tabsdatak.conn.starrocks.error import StarRocksErrorCode

    raise StarRocksErrorCode.STARROCKS_7.exception(
        kind=type(staging).__name__,
    )


def transporter_env(staging: CloudLocation) -> dict[str, str]:
    from tabsdatak.conn.common.file import _transporter as t

    if isinstance(staging, S3Location):
        creds = staging.credentials
        return {
            t.SERVER_SIDE_AWS_ACCESS_KEY_ID: creds.access_key_id.value(),
            t.SERVER_SIDE_AWS_SECRET_ACCESS_KEY: creds.secret_access_key.value(),
            t.SERVER_SIDE_AWS_REGION: _resolve(staging.region),
        }
    if isinstance(staging, AzBlobLocation):
        creds = staging.credentials
        return {
            t.SERVER_SIDE_AZURE_ACCOUNT_NAME: creds.account_name.value(),
            t.SERVER_SIDE_AZURE_ACCOUNT_KEY: creds.account_key.value(),
        }
    if isinstance(staging, GCSLocation):
        creds = staging.credentials
        return {
            t.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON: creds.json.value(),
        }
    from tabsdatak.conn.starrocks.error import StarRocksErrorCode

    raise StarRocksErrorCode.STARROCKS_7.exception(
        kind=type(staging).__name__,
    )


# --- Write config + main write entry point ---------------------------


@dataclass(kw_only=True)
class StarRocksWriteCfg:
    host: str
    port: int
    database: str
    credentials: UserPassword
    staging: CloudLocation
    target_tables: list[str]
    if_table_exists: str = "append"
    schema_evolution: str = "strict"
    conn_cfg: dict | None = None
    upload_prefix: str = "tabsdata-starrocks"


def _quote_ident(name: str) -> str:
    if name.startswith("`") and name.endswith("`"):
        return name
    return f"`{name}`"


def _qualify(database: str, table: str) -> str:
    from tabsdatak.conn.common._validation import split_fqn

    parts = split_fqn(table, quote="`")
    if len(parts) == 2:
        return f"{_quote_ident(parts[0])}.{_quote_ident(parts[1])}"
    return f"{_quote_ident(database)}.{_quote_ident(parts[0])}"


def upload_parquet(
    staging: CloudLocation,
    work_dir: Path,
    upload_id: str,
    parquet_path: Path,
    table_key: str,
) -> str:
    from tabsdatak.conn.common.file import _output, _transporter as t

    rel = f"{upload_id}/{table_key}/{parquet_path.name}"
    target_loc = transporter_target(staging, rel)
    source_loc = t.TransporterLocalFile(t.path_to_file_uri(parquet_path))
    env = transporter_env(staging)

    _output.copy_one(
        source_target_pairs=[(source_loc, target_loc)],
        work_dir=work_dir,
        format=t.TransporterParquetFormat(),
        env=env,
    )

    return staged_uri(staging, rel)


def execute(
    cfg: StarRocksWriteCfg,
    tables: list[Path],
    work_dir: Path,
    upload_id: str,
) -> None:
    from sqlalchemy import text

    if cfg.if_table_exists not in ("append", "replace"):
        raise ValueError(
            f"if_table_exists must be 'append' or 'replace', "
            f"got {cfg.if_table_exists!r}",
        )

    engine = build_starrocks_engine(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        credentials=cfg.credentials,
        connect_args=cfg.conn_cfg,
    )
    try:
        for table_name, parquet in zip(cfg.target_tables, tables):
            staged = upload_parquet(
                cfg.staging,
                work_dir,
                upload_id,
                parquet,
                table_key=table_name.replace("`", "").replace(".", "_"),
            )
            files_fn = build_files_function(cfg.staging, [staged])
            qualified = _qualify(cfg.database, table_name)
            with engine.begin() as conn:
                if cfg.if_table_exists == "replace":
                    conn.execute(text(f"TRUNCATE TABLE {qualified}"))
                conn.execute(text(f"INSERT INTO {qualified} SELECT * FROM {files_fn}"))
            logger.info(
                "starrocks: loaded %s from %d staged file(s)",
                qualified, 1,
            )
    finally:
        engine.dispose()

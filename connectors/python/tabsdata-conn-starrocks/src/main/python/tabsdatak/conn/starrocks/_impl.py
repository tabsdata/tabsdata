#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from tenacity import retry, retry_if_exception, stop_after_delay, wait_fixed

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import UserPassword, _convert, full_path
from tabsdatak.conn.starrocks import (
    AzBlobLocation,
    CloudLocation,
    GCSLocation,
    S3Location,
)

logger = logging.getLogger(__name__)

# Mirror the StarRocks retry policy used by
# ``tabsdata/_io/cloud_staging/starrocks/dialect.py``: StarRocks
# transiently reports "Current available backends: []" under heavy
# concurrent transactional load. Retry the SQL block for up to 5
# minutes on a 5-second cadence -- but only for that transient
# condition. Deterministic failures (e.g. a missing target table, which
# surfaces as a 1064 "analyzing error") must fail immediately instead of
# being retried for the full timeout.
_STARROCKS_QUERY_RETRY_TIMEOUT_SECONDS = 300
_STARROCKS_QUERY_RETRY_INTERVAL_SECONDS = 5

# Substrings marking a StarRocks error as transient and worth retrying.
# "Current available backends: []" is reported under concurrent load on a
# single-BE cluster before a backend (re)registers.
_STARROCKS_TRANSIENT_MARKERS = ("current available backends",)


def _is_transient_starrocks_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in _STARROCKS_TRANSIENT_MARKERS)


_starrocks_retry = retry(
    stop=stop_after_delay(_STARROCKS_QUERY_RETRY_TIMEOUT_SECONDS),
    wait=wait_fixed(_STARROCKS_QUERY_RETRY_INTERVAL_SECONDS),
    retry=retry_if_exception(_is_transient_starrocks_error),
    reraise=True,
)


# --- StarRocks SQL engine (MySQL protocol) ---------------------------


def build_starrocks_engine(
    host: str,
    port: int,
    database: str,
    credentials: UserPassword,
    connect_args: dict | None = None,
):
    """Create a SQLAlchemy engine talking to StarRocks via the
    MySQL wire protocol. ``mysql+mysqlconnector://`` is used so the
    runtime only needs ``mysql-connector-python`` (which the legacy
    plugin already required) and not a separate driver shim.
    """
    from sqlalchemy import create_engine

    user = _convert(credentials.user, str)
    password = _convert(credentials.password, str)
    url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
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
    """Longest shared directory prefix across all *paths*. Mirrors the
    legacy ``cloud_storage._common_directory`` helper."""
    parts_list = [p.rsplit("/", 1)[0] for p in paths]
    if len(set(parts_list)) == 1:
        return parts_list[0]
    prefix = parts_list[0]
    for p in parts_list[1:]:
        while not p.startswith(prefix):
            prefix = prefix[: prefix.rfind("/")]
    return prefix


def _path_expr(uris: list[str]) -> str:
    """One URI passes through verbatim; many uris collapse to a
    ``<commondir>/*`` wildcard which StarRocks' ``FILES()`` accepts."""
    if len(uris) == 1:
        return uris[0]
    return _common_directory(uris) + "/*"


def build_files_function(staging: CloudLocation, staged_uris: list[str]) -> str:
    """Build a ``FILES(...)`` table-function call for *staged_uris*.

    Per-cloud credential and endpoint settings are pulled from
    *staging* (the user-supplied :class:`CloudLocation` on
    :class:`StarRocksDestConn`).
    """
    if isinstance(staging, S3Location):
        region = staging._region()
        creds = staging.credentials
        return (
            "FILES("
            + _kv({
                "path": _path_expr(staged_uris),
                "format": "parquet",
                "aws.s3.access_key": _convert(creds.access_key_id, str),
                "aws.s3.secret_key": _convert(creds.secret_access_key, str),
                "aws.s3.region": region,
            })
            + ")"
        )
    if isinstance(staging, AzBlobLocation):
        creds = staging.credentials
        return (
            "FILES("
            + _kv({
                "path": _path_expr(staged_uris),
                "format": "parquet",
                "azure.blob.shared_key": _convert(creds.account_key, str),
            })
            + ")"
        )
    if isinstance(staging, GCSLocation):
        # StarRocks reads GCS only through its S3-compatible endpoint, so the
        # ``s3a://`` paths built by ``staged_uri`` are fed to ``FILES()`` with
        # the HMAC access/secret keys and the GCS endpoint.
        creds = staging.hmac_credentials
        return (
            "FILES("
            + _kv({
                "path": _path_expr(staged_uris),
                "format": "parquet",
                "aws.s3.access_key": _convert(creds.access_key, str),
                "aws.s3.secret_key": _convert(creds.secret_key, str),
                "aws.s3.endpoint": "https://storage.googleapis.com",
            })
            + ")"
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
    """Render an absolute URI under *staging* for the relative *key*.
    The URI format is the one StarRocks' ``FILES()`` table function
    consumes for the corresponding cloud backend.
    """
    rel = full_path(staging._base_path(), key)
    if isinstance(staging, S3Location):
        bucket = staging._bucket()
        return f"s3://{bucket}/{rel}"
    if isinstance(staging, AzBlobLocation):
        creds = staging.credentials
        container = staging._container()
        account = _convert(creds.account_name, str)
        return f"wasbs://{container}@{account}.blob.core.windows.net/{rel}"
    if isinstance(staging, GCSLocation):
        bucket = staging._bucket()
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


def transporter_target(staging: CloudLocation, key: str, handler):
    """Build the transporter location *destination* for a parquet upload
    to *staging* under the relative *key*."""
    rel = full_path(staging._base_path(), key)
    if isinstance(staging, S3Location):
        bucket = staging._bucket()
        return handler.s3_location(
            uri=f"s3://{bucket}/{rel}",
            access_key_env=handler.aws_access_key_id_env,
            secret_key_env=handler.aws_secret_access_key_env,
            region_env=handler.aws_region_env,
        )
    if isinstance(staging, AzBlobLocation):
        container = staging._container()
        return handler.azure_location(
            uri=f"az://{container}/{rel}",
            account_name_env=handler.azure_account_name_env,
            account_key_env=handler.azure_account_key_env,
        )
    if isinstance(staging, GCSLocation):
        bucket = staging._bucket()
        return handler.gcs_location(
            uri=f"gs://{bucket}/{rel}",
            service_account_key_env=handler.gcp_service_account_json_env,
        )
    from tabsdatak.conn.starrocks.error import StarRocksErrorCode

    raise StarRocksErrorCode.STARROCKS_7.exception(
        kind=type(staging).__name__,
    )


def transporter_env(staging: CloudLocation, handler) -> dict[str, str]:
    """Resolve the credentials on *staging* into env vars the
    transporter binary reads at runtime."""
    if isinstance(staging, S3Location):
        creds = staging.credentials
        return {
            handler.aws_access_key_id_env: _convert(creds.access_key_id, str),
            handler.aws_secret_access_key_env: _convert(creds.secret_access_key, str),
            handler.aws_region_env: staging._region(),
        }
    if isinstance(staging, AzBlobLocation):
        creds = staging.credentials
        return {
            handler.azure_account_name_env: _convert(creds.account_name, str),
            handler.azure_account_key_env: _convert(creds.account_key, str),
        }
    if isinstance(staging, GCSLocation):
        creds = staging.credentials
        return {
            handler.gcp_service_account_json_env: _convert(creds.json, str),
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
    """Quote a StarRocks identifier with backticks. If the value is
    already backtick-quoted we leave it as-is."""
    if name.startswith("`") and name.endswith("`"):
        return name
    return f"`{name}`"


def _qualify(database: str, table: str) -> str:
    r"""Return ``\`db\`.\`tbl\``` -- handling pre-qualified inputs
    (``a.b``) by splitting at the unquoted dot."""
    from tabsdatak.conn.common._validation import complete_levels, split_fqn

    raw = split_fqn(table, quote="`")
    parts: list[str | None] = [None] * (2 - len(raw)) + list(raw)
    db, tbl = complete_levels(parts, [database, None])
    return f"{_quote_ident(db)}.{_quote_ident(tbl)}"


def upload_parquet(
    staging: CloudLocation,
    work_dir: Path,
    upload_id: str,
    parquet_path: Path,
    table_key: str,
) -> str:
    """Upload one local parquet to *staging* under
    ``<upload_id>/<table_key>/<filename>`` and return the staged URI.

    Returns the same URI shape that :func:`build_files_function` /
    :func:`staged_uri` expect.
    """
    from tabsdatak.common._fileformat.taxonomy import DestFileFormat
    from tabsdatak.common._transporter.facade import make_handler
    from tabsdatak.conn.common.file import _output

    handler = make_handler()
    rel = f"{upload_id}/{table_key}/{parquet_path.name}"
    target_loc = transporter_target(staging, rel, handler)
    source_loc = handler.local_path_location(parquet_path)
    env = transporter_env(staging, handler)

    _output.copy_one(
        handler=handler,
        source_target_pairs=[(source_loc, target_loc)],
        work_dir=work_dir,
        file_format=DestFileFormat.PARQUET,
        paths=[rel],
        env=env,
    )

    return staged_uri(staging, rel)


def _parquet_column_names(parquet: Path) -> list[str]:
    """Column names of a staged parquet, in file order. Used to build an
    explicit INSERT column list so a target column the file does not carry
    (e.g. an ``AUTO_INCREMENT`` key) is left for StarRocks to populate rather
    than triggering a positional column-count mismatch."""
    import pyarrow.parquet as pq

    return list(pq.read_schema(parquet).names)


def execute(
    cfg: StarRocksWriteCfg,
    tables: list[Path],
    work_dir: Path,
    upload_id: str,
) -> None:
    """Stage each parquet to cloud, then issue one
    ``INSERT INTO <table> (<cols>) SELECT * FROM FILES(...)`` per table.

    Each table is loaded independently (StarRocks does not support
    multi-statement transactions for bulk loads); a failure after some
    tables have loaded leaves those tables with the new rows -- the
    documented recovery path is to re-run in ``replace`` mode.
    """
    from sqlalchemy import text

    if cfg.if_table_exists not in ("append", "replace"):
        raise ValueError(
            f"if_table_exists must be 'append' or 'replace', "
            f"got {cfg.if_table_exists!r}",
        )

    logger.debug(
        "starrocks: establishing SQLAlchemy engine to %s:%s", cfg.host, cfg.port
    )
    engine = build_starrocks_engine(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        credentials=cfg.credentials,
        connect_args=cfg.conn_cfg,
    )

    @_starrocks_retry
    def _load_one(qualified: str, files_fn: str, columns: list[str]) -> None:
        """``TRUNCATE`` (replace mode) + ``INSERT INTO <t> (<cols>) ... FROM
        FILES(...)`` for a single table, retried on transient StarRocks errors
        such as "Current available backends: []" that surface under heavy
        concurrent load on a single-BE cluster.

        Columns are named explicitly (from the staged parquet) rather than a
        bare ``SELECT *`` insert, so a target column the file omits -- e.g. an
        ``AUTO_INCREMENT`` key -- is left for StarRocks to populate instead of
        causing a positional column-count mismatch."""
        collist = ", ".join(_quote_ident(c) for c in columns)
        with engine.begin() as conn:
            if cfg.if_table_exists == "replace":
                conn.execute(text(f"TRUNCATE TABLE {qualified}"))
            conn.execute(
                text(f"INSERT INTO {qualified} ({collist}) SELECT * FROM {files_fn}")
            )

    total = len(cfg.target_tables)
    try:
        for i, (table_name, parquet) in enumerate(
            zip(cfg.target_tables, tables),
            1,
        ):
            logger.info(
                "starrocks: writing table %d/%d: %r",
                i,
                total,
                table_name,
            )
            logger.info("starrocks: staging parquet for %r", table_name)
            staged = upload_parquet(
                cfg.staging,
                work_dir,
                upload_id,
                parquet,
                table_key=table_name.replace("`", "").replace(".", "_"),
            )
            files_fn = build_files_function(cfg.staging, [staged])
            qualified = _qualify(cfg.database, table_name)
            _load_one(qualified, files_fn, _parquet_column_names(parquet))
            logger.info(
                "starrocks: loaded %s from %d staged file(s)",
                qualified,
                1,
            )
    finally:
        engine.dispose()

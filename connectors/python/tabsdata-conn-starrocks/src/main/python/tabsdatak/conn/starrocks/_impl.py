#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass

import polars as pl
from tenacity import retry, retry_if_exception, stop_after_delay, wait_fixed

from tabsdatak.common._storage.transfer import (
    AWS_ACCESS_KEY_ID_OPTION,
    AWS_REGION_OPTION,
    AWS_SECRET_ACCESS_KEY_OPTION,
    AZURE_STORAGE_ACCOUNT_KEY_OPTION,
    AZURE_STORAGE_ACCOUNT_NAME_OPTION,
    GOOGLE_SERVICE_ACCOUNT_KEY_OPTION,
)
from tabsdatak.conn.common.file._store import FileStore, forwarded

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import UserPassword, _convert, cloud_full_path
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
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.sql._engine import sqlalchemy_utils

    user = _convert(credentials.user, str)
    password = _convert(credentials.password, str)
    url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
    return sqlalchemy_utils.create_engine(url, connect_args=dict(connect_args or {}))


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
        account = staging.credentials
        return (
            "FILES("
            + _kv({
                "path": _path_expr(staged_uris),
                "format": "parquet",
                "azure.blob.shared_key": _convert(account.account_key, str),
            })
            + ")"
        )
    if isinstance(staging, GCSLocation):
        # StarRocks reads GCS only through its S3-compatible endpoint, so the
        # ``s3a://`` paths built by ``staged_uri`` are fed to ``FILES()`` with
        # the HMAC access/secret keys and the GCS endpoint.
        hmac = staging.hmac_credentials
        return (
            "FILES("
            + _kv({
                "path": _path_expr(staged_uris),
                "format": "parquet",
                "aws.s3.access_key": _convert(hmac.access_key, str),
                "aws.s3.secret_key": _convert(hmac.secret_key, str),
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
    rel = cloud_full_path(staging._base_path(), key)
    if isinstance(staging, S3Location):
        bucket = staging._bucket()
        return f"s3://{bucket}/{rel}"
    if isinstance(staging, AzBlobLocation):
        container = staging._container()
        name = _convert(staging.credentials.account_name, str)
        return f"wasbs://{container}@{name}.blob.core.windows.net/{rel}"
    if isinstance(staging, GCSLocation):
        bucket = staging._bucket()
        return f"s3a://{bucket}/{rel}"
    from tabsdatak.conn.starrocks.error import StarRocksErrorCode

    raise StarRocksErrorCode.STARROCKS_7.exception(
        kind=type(staging).__name__,
    )


# --- Staging store ---------------------------------------------------


def staging_store(staging: CloudLocation) -> FileStore:
    """The object store the staged parquet is written to: the bucket or
    container root of *staging*, with its credentials and any ``obstore.*``
    keys of its ``cfg``."""
    if isinstance(staging, S3Location):
        root = f"s3://{staging._bucket()}"
        creds = staging.credentials
        options = {
            AWS_ACCESS_KEY_ID_OPTION: _convert(creds.access_key_id, str),
            AWS_SECRET_ACCESS_KEY_OPTION: _convert(creds.secret_access_key, str),
            AWS_REGION_OPTION: staging._region(),
        }
    elif isinstance(staging, AzBlobLocation):
        root = f"az://{staging._container()}"
        account = staging.credentials
        options = {
            AZURE_STORAGE_ACCOUNT_NAME_OPTION: _convert(account.account_name, str),
            AZURE_STORAGE_ACCOUNT_KEY_OPTION: _convert(account.account_key, str),
        }
    elif isinstance(staging, GCSLocation):
        root = f"gs://{staging._bucket()}"
        options = {
            GOOGLE_SERVICE_ACCOUNT_KEY_OPTION: _convert(staging.credentials.json, str),
        }
    else:
        from tabsdatak.conn.starrocks.error import StarRocksErrorCode

        raise StarRocksErrorCode.STARROCKS_7.exception(
            kind=type(staging).__name__,
        )
    return FileStore(root, {**options, **forwarded(root, staging.cfg)})


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
    assert db is not None and tbl is not None
    return f"{_quote_ident(db)}.{_quote_ident(tbl)}"


def stage(
    staging: CloudLocation,
    store: FileStore,
    frame: pl.LazyFrame,
    rel: str,
) -> tuple[str, str]:
    """Write *frame* as parquet to *staging* under *rel* and return its key
    in *store* and the URI :func:`build_files_function` takes."""
    key = cloud_full_path(staging._base_path(), rel)
    writer = store.open_writer(key)
    frame.sink_parquet(writer)
    writer.close()
    return key, staged_uri(staging, rel)


def discard(store: FileStore, keys: list[str]) -> None:
    try:
        store.delete(keys)
    except Exception as error:
        logger.warning("starrocks: staged files remain (%s): %s", keys, error)


def execute(
    cfg: StarRocksWriteCfg,
    tables: list[pl.LazyFrame],
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

    from tabsdatak.conn.starrocks.error import StarRocksErrorCode

    store = staging_store(cfg.staging)
    total = len(cfg.target_tables)
    try:
        for i, (table_name, frame) in enumerate(
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
            table_key = table_name.replace("`", "").replace(".", "_")
            try:
                key, staged = stage(
                    cfg.staging,
                    store,
                    frame,
                    f"{upload_id}/{table_key}/{i - 1}.parquet",
                )
            except Exception as error:
                raise StarRocksErrorCode.STARROCKS_5.exception(
                    cause=error,
                    table=table_name,
                )
            try:
                files_fn = build_files_function(cfg.staging, [staged])
                qualified = _qualify(cfg.database, table_name)
                _load_one(qualified, files_fn, frame.collect_schema().names())
            finally:
                discard(store, [key])
            logger.info(
                "starrocks: loaded %s from %d staged file(s)",
                qualified,
                1,
            )
    finally:
        engine.dispose()

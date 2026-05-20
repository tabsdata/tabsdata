"""Snowflake adapter: every driver-touching piece used by ``_plugin.py``.

The legacy :class:`tabsdata.connector.snowflake._connector.SnowflakeDestination`
is ported here, adapted to the new-model ``SnowflakeConn`` /
``SnowflakeDest`` classes. Snowflake bulk loads go through the native
:mod:`snowflake.connector` Python driver -- a stage (temporary by
default; permanent when ``conn.stage`` is set) + ``PUT`` to upload the
parquet file + ``COPY INTO`` to ingest. NO SQLAlchemy / polars
write_database path -- the legacy approach is dramatically faster and
sidesteps the polars ``Unexpected three-part table name`` quirk.

All driver imports happen inside method bodies so importing
``tabsdatak.conn.snowflake`` stays cheap when nothing actually runs
the plugin (registry scans pay only for the DestDef metadata).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tabsdatak.conn.common._validation import fqn, unfqn
from tabsdatak.conn.common.types import UserPassword
from tabsdatak.conn.snowflake import SnowflakeConn
from tabsdatak.conn.snowflake._validation import _SNOWFLAKE_FQN_DEF

if TYPE_CHECKING:
    import snowflake.connector as sfc

logger = logging.getLogger(__name__)


# --- Constants (matched to legacy) ----------------------------------

# Snowflake ``COPY INTO`` defaults that mirror the legacy
# ``tabsdata-snowflake`` connector. Match by column name (case-
# insensitive) so column ordering in the parquet file is irrelevant,
# and purge the staged file once it has been ingested so we don't
# accumulate orphans.
_DEFAULT_MATCH_BY_COLUMN_NAME = "CASE_INSENSITIVE"
_DEFAULT_PURGE = "TRUE"

# Fallback stage name used when ``conn.stage`` is not set. Created as
# a TEMPORARY stage so it disappears with the session.
_DEFAULT_TEMP_STAGE = "tabsdata_temp_stage"


# --- Write cfg ------------------------------------------------------


@dataclass(kw_only=True)
class SnowflakeWriteCfg:
    conn: SnowflakeConn
    target_tables: list[str]  # raw user input -- already FQN-shaped
    if_table_exists: str  # "append" | "replace"
    match_by_column_name: str = _DEFAULT_MATCH_BY_COLUMN_NAME
    purge: str = _DEFAULT_PURGE


# --- Conn helpers ---------------------------------------------------


def _credentials(conn: SnowflakeConn) -> UserPassword:
    creds = conn.credentials
    if not isinstance(creds, UserPassword):
        raise TypeError(
            f"SnowflakeConn.credentials must be UserPassword, got "
            f"{type(creds).__name__}"
        )
    return creds


def _connection_parameters(conn: SnowflakeConn) -> dict[str, str]:
    """Resolve ``SnowflakeConn`` into the ``snowflake.connector.connect``
    kwargs. ``password`` carries either a Snowflake password or a PAT;
    the driver accepts both at the same slot."""
    creds = _credentials(conn)
    params: dict[str, str] = {
        "account": conn.account.value(),
        "user": creds.user.value(),
        "password": creds.password.value(),
    }
    if conn.role is not None:
        params["role"] = conn.role.value()
    if conn.database is not None:
        params["database"] = conn.database.value()
    if conn.schema is not None:
        params["schema"] = conn.schema.value()
    if conn.warehouse is not None:
        params["warehouse"] = conn.warehouse.value()
    return params


def build_snowflake_connection(conn: SnowflakeConn) -> sfc.SnowflakeConnection:
    """Open a :class:`snowflake.connector.SnowflakeConnection` bound
    to ``conn``'s resolved parameters."""
    try:
        from snowflake.connector import connect
    except ImportError as e:
        raise ImportError(
            "snowflake-connector-python is required for the Snowflake"
            " connector. Install it with:"
            " pip install snowflake-connector-python"
        ) from e
    return connect(**_connection_parameters(conn))


# --- FQN quoting ---------------------------------------------------


def _fq_for_sql(fq_table: str) -> str:
    """Return the SQL-safe (per-part double-quoted) form of a 1/2/3-part
    snowflake identifier.

    Accepts a bare dotted name OR a name whose parts are already
    double-quoted -- a single ``unfqn`` -> ``fqn`` round-trip strips
    any pre-existing quotes so the result never double-quotes.
    Embedded double quotes inside a part are preserved via the
    Snowflake doubled-quote escape convention (``""``).
    """
    parts = unfqn(fq_table, _SNOWFLAKE_FQN_DEF)
    return fqn(parts, _SNOWFLAKE_FQN_DEF)


# --- Stage / upload / load helpers ---------------------------------


def _stage(conn: SnowflakeConn) -> tuple[str, bool]:
    """Return ``(stage_name, is_temporary)`` for the connection. When
    ``conn.stage`` is set the stage is treated as permanent (caller
    owns its lifecycle); otherwise a session-scoped TEMPORARY stage
    is used."""
    if conn.stage is not None:
        return conn.stage.value(), False
    return _DEFAULT_TEMP_STAGE, True


def _create_stage_if_not_exists(cursor, stage: str, is_temp: bool) -> None:
    kind = "TEMPORARY " if is_temp else ""
    cursor.execute(f"CREATE {kind}STAGE IF NOT EXISTS {stage}")


def _put_file_to_stage(cursor, file_path: Path, stage: str) -> str:
    """``PUT`` ``file_path`` into ``@stage`` and return the basename
    snowflake will index it under."""
    # snowflake's PUT accepts a file:// URI; use absolute path so
    # the driver doesn't try to resolve against its own cwd.
    file_uri = f"file://{file_path.resolve()}"
    cursor.execute(f"PUT {file_uri} @{stage}")
    return file_path.name


def _create_table_from_inferred_schema(
    cursor,
    stage: str,
    file_name: str,
    sql_table: str,
) -> None:
    """``CREATE TABLE IF NOT EXISTS`` for ``sql_table`` (already
    routed through :func:`_fq_for_sql`) using a schema inferred from
    the staged parquet file (mirrors the legacy template-with-
    infer_schema approach)."""
    cursor.execute("CREATE FILE FORMAT IF NOT EXISTS tabsdata_parquet TYPE = PARQUET")
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {sql_table} USING template ("
        f"SELECT array_agg(object_construct(*)) "
        f"FROM table ("
        f"infer_schema("
        f"location => '@{stage}/{file_name}', "
        f"file_format => 'tabsdata_parquet', "
        f"ignore_case => true"
        f")"
        f")"
        f")"
    )


def _copy_into(
    cursor,
    stage: str,
    file_name: str,
    sql_table: str,
    *,
    match_by_column_name: str,
    purge: str,
) -> None:
    cursor.execute(
        f"COPY INTO {sql_table} FROM @{stage} FILES = ('{file_name}') "
        f"FILE_FORMAT = (TYPE = 'PARQUET') "
        f"MATCH_BY_COLUMN_NAME = {match_by_column_name} "
        f"PURGE = {purge}"
    )


# --- Write entry point ----------------------------------------------


def write_all_tables(cfg: SnowflakeWriteCfg, files: list[Path]) -> None:
    """Bulk-load each parquet file in ``files`` into its corresponding
    table in ``cfg.target_tables``. Opens a single Snowflake connection
    per call and re-uses it across every (file, table) pair."""
    logger.debug(
        "snowflake: establishing connection to account %r",
        cfg.conn.account.value(),
    )
    sf_conn = build_snowflake_connection(cfg.conn)
    try:
        if cfg.conn.warehouse is not None:
            warehouse = cfg.conn.warehouse.value()
            logger.debug("Using warehouse %r", warehouse)
            with sf_conn.cursor() as cur:
                cur.execute(f"USE WAREHOUSE {warehouse}")
        stage, is_temp = _stage(cfg.conn)
        for i, (file_path, table) in enumerate(
            zip(
                files,
                cfg.target_tables,
                strict=True,
            ),
            1,
        ):
            logger.info(
                "snowflake: writing table %d/%d: %r",
                i,
                len(cfg.target_tables),
                table,
            )
            _write_one(
                sf_conn,
                cfg,
                stage,
                is_temp,
                Path(file_path),
                table,
            )
            logger.debug("snowflake: finished table %r", table)
    finally:
        logger.debug("snowflake: closing connection")
        sf_conn.close()


def _write_one(
    sf_conn,
    cfg: SnowflakeWriteCfg,
    stage: str,
    is_temp: bool,
    file_path: Path,
    table: str,
) -> None:
    sql_table = _fq_for_sql(table)
    logger.info("Uploading %s -> %s", file_path.name, sql_table)
    cursor = sf_conn.cursor()
    try:
        _create_stage_if_not_exists(cursor, stage, is_temp)
        file_name = _put_file_to_stage(cursor, file_path, stage)
        _create_table_from_inferred_schema(cursor, stage, file_name, sql_table)
        if cfg.if_table_exists == "replace":
            logger.debug(
                "Truncating %s before COPY (if_table_exists=replace)", sql_table
            )
            cursor.execute(f"TRUNCATE {sql_table}")
        _copy_into(
            cursor,
            stage,
            file_name,
            sql_table,
            match_by_column_name=cfg.match_by_column_name,
            purge=cfg.purge,
        )
    finally:
        cursor.close()

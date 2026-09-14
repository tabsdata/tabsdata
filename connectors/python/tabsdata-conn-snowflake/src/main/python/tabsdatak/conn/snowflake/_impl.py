#
# Copyright 2026 Tabsdata Inc.
#

"""Snowflake adapter: every driver-touching piece used by ``_plugin.py``.

The legacy :class:`tabsdata.connector.snowflake._connector.SnowflakeDestination`
is ported here, adapted to the new-model ``SnowflakeDestConn`` /
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
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tabsdatak.conn.common._validation import fqn, unfqn

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._driver import merge_batch
from tabsdatak.conn.common.types import UserPassword, _convert
from tabsdatak.conn.snowflake import MergeCfg, SnowflakeDestConn
from tabsdatak.conn.snowflake._evolution import SnowflakeSchemaEvolver
from tabsdatak.conn.snowflake._merge_dialect import SnowflakeMergeDialect
from tabsdatak.conn.snowflake._staging import (
    copy_into,
    create_file_format,
    inferred_columns,
    inferred_schema_template,
    put_file_to_stage,
)
from tabsdatak.conn.snowflake._validation import _SNOWFLAKE_FQN_DEF
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode
from tabsdatak.spi import DestContext

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
    conn: SnowflakeDestConn
    target_tables: list[str]  # raw user input -- already FQN-shaped
    schema_evolution: str  # "strict" | "update"
    schema_evolution_engine: str  # "native" | "iceberg"
    if_table_exists: str  # "append" | "replace" | "merge"
    ctx: DestContext  # the trx the write belongs to; a merge stamps its id
    # One entry per target table when merging, already normalized by
    # ``SnowflakeDest._merge_cfgs()`` (a single MergeCfg broadcast across every
    # table); ``None`` in every other disposition.
    merge_cfgs: list[MergeCfg] | None = None
    match_by_column_name: str = _DEFAULT_MATCH_BY_COLUMN_NAME
    purge: str = _DEFAULT_PURGE


# --- Conn helpers ---------------------------------------------------


def _credentials(conn: SnowflakeDestConn) -> UserPassword:
    creds = conn.credentials
    if not isinstance(creds, UserPassword):
        raise TypeError(
            f"SnowflakeDestConn.credentials must be UserPassword, got "
            f"{type(creds).__name__}"
        )
    return creds


def _connection_parameters(conn: SnowflakeDestConn) -> dict[str, str]:
    """Resolve ``SnowflakeDestConn`` into the ``snowflake.connector.connect``
    kwargs. ``password`` carries either a Snowflake password or a PAT;
    the driver accepts both at the same slot."""
    creds = _credentials(conn)
    params: dict[str, str] = {
        "account": conn._account(),
        "user": _convert(creds.user, str),
        "password": _convert(creds.password, str),
    }
    if conn.role is not None:
        params["role"] = conn._role()
    if conn.database is not None:
        params["database"] = conn._database()
    if conn.schema is not None:
        params["schema"] = conn._schema()
    if conn.warehouse is not None:
        params["warehouse"] = conn._warehouse()
    return params


def build_snowflake_connection(conn: SnowflakeDestConn) -> sfc.SnowflakeConnection:
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


# Snowflake's unquoted-identifier grammar: a letter/underscore start followed by
# letters, digits, underscores or dollar signs. Such a name is CASE-INSENSITIVE
# -- Snowflake upper-folds it -- so an unquoted reference resolves onto an object
# that was created unquoted (upper-folded), whatever case we happen to spell it in.
_UNQUOTED_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*\Z")


def _use_ident(name: str) -> str:
    """Reference `name` in a ``USE DATABASE/SCHEMA``.

    Any name that fits the unquoted-identifier grammar is therefore emitted
    UNQUOTED here so Snowflake folds it back onto the stored object regardless of
    the case we were handed.
    """
    return name if _UNQUOTED_IDENT_RE.fullmatch(name) else _fq_for_sql(name)


def _use_namespace(sf_conn, conn: SnowflakeDestConn) -> None:
    """Force the session onto ``conn``'s database/schema so every bare object
    reference (the stage, unqualified target tables) resolves where the caller
    intends. ``connect()`` passes database/schema as login parameters only; the
    session silently falls back to the role's default namespace when the
    requested one can't be adopted, so re-assert it explicitly and fail with a
    clear error if the database/schema is missing or not granted."""
    if conn.database is None:
        return
    database = conn._database()
    schema = conn._schema() if conn.schema is not None else None
    try:
        with sf_conn.cursor() as cur:
            cur.execute(f"USE DATABASE {_use_ident(database)}")
            if schema is not None:
                cur.execute(f"USE SCHEMA {_use_ident(schema)}")
    except Exception as e:
        raise SnowflakeErrorCode.SNOWFLAKE_7.exception(
            cause=e, database=database, schema=schema
        ) from e


# --- Stage / upload / load helpers ---------------------------------


def _stage(conn: SnowflakeDestConn) -> tuple[str, bool]:
    """Return ``(stage_name, is_temporary)`` for the connection. When
    ``conn.stage`` is set the stage is treated as permanent (caller
    owns its lifecycle); otherwise a session-scoped TEMPORARY stage
    is used."""
    if conn.stage is not None:
        return conn._stage(), False
    return _DEFAULT_TEMP_STAGE, True


def _create_stage_if_not_exists(cursor, stage: str, is_temp: bool) -> None:
    kind = "TEMPORARY " if is_temp else ""
    cursor.execute(f"CREATE {kind}STAGE IF NOT EXISTS {stage}")


def _create_table_from_inferred_schema(
    cursor,
    stage: str,
    file_name: str,
    sql_table: str,
) -> None:
    """``CREATE TABLE IF NOT EXISTS`` for ``sql_table`` (already
    routed through :func:`_fq_for_sql`) using a schema inferred from
    the staged parquet file (mirrors the legacy template-with-
    infer_schema approach).

    A merge target is not created here -- it is built from the staged relation
    by the merge dialect, which is also what keeps the marker column out of it.
    """
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {sql_table} USING template ("
        f"{inferred_schema_template(stage, file_name)}"
        f")"
    )


def _set_copy_into_schema_evolution(
    cursor, schema_evolution: bool, sql_table: str
) -> None:
    cursor.execute(
        f"ALTER TABLE {sql_table} SET ENABLE_SCHEMA_EVOLUTION = {schema_evolution};"
    )


def reject_unsupported_columns(
    target_tables: list[str],
    files: list[Path],
) -> None:
    """Fail before loading anything if a column has a type the bulk load cannot
    carry.

    COPY INTO matches columns by name, which routes every parquet value through
    a variant, and a variant cannot be cast to BINARY ("Failed to cast variant
    value ... to BINARY"). Snowflake reports that per file once the stage and the
    table already exist, so reading the parquet footer up front turns it into one
    error naming the table and the offending columns.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    from tabsdatak.conn.snowflake.error import SnowflakeErrorCode

    for table, path in zip(target_tables, files):
        schema = pq.read_schema(str(path))
        offenders = [
            f.name
            for f in schema
            if pa.types.is_binary(f.type)
            or pa.types.is_large_binary(f.type)
            or pa.types.is_fixed_size_binary(f.type)
        ]
        if offenders:
            raise SnowflakeErrorCode.SNOWFLAKE_8.exception(
                table=table,
                columns=", ".join(repr(name) for name in offenders),
            )


def write_all_tables(cfg: SnowflakeWriteCfg, files: list[Path]) -> None:
    """Bulk-load each parquet file in ``files`` into its corresponding
    table in ``cfg.target_tables``. Opens a single Snowflake connection
    per call and re-uses it across every (file, table) pair."""
    reject_unsupported_columns(list(cfg.target_tables), files)

    logger.debug(
        "snowflake: establishing connection to account %r",
        cfg.conn._account(),
    )
    sf_conn = build_snowflake_connection(cfg.conn)
    try:
        if cfg.conn.warehouse is not None:
            warehouse = cfg.conn._warehouse()
            logger.debug("Using warehouse %r", warehouse)
            with sf_conn.cursor() as cur:
                cur.execute(f"USE WAREHOUSE {warehouse}")
        _use_namespace(sf_conn, cfg.conn)
        with sf_conn.cursor() as cur:
            create_file_format(cur)
        stage, is_temp = _stage(cfg.conn)
        # ``strict`` also holds the merge configs to one per table: the plugin
        # normalizes them to that shape, and a mismatch here would silently
        # merge one table by another's keys.
        merge_cfgs = cfg.merge_cfgs or [None] * len(cfg.target_tables)
        for i, (file_path, table, merge_cfg) in enumerate(
            zip(
                files,
                cfg.target_tables,
                merge_cfgs,
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
                merge_cfg,
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
    merge_cfg: MergeCfg | None = None,
) -> None:
    sql_table = _fq_for_sql(table)
    logger.info("Uploading %s -> %s", file_path.name, sql_table)
    cursor = sf_conn.cursor()
    try:
        _create_stage_if_not_exists(cursor, stage, is_temp)
        if cfg.if_table_exists == "merge":
            _merge_one(cursor, cfg, stage, file_path, sql_table, merge_cfg)
            return
        file_name = put_file_to_stage(cursor, file_path, stage)
        _create_table_from_inferred_schema(cursor, stage, file_name, sql_table)
        evolving = cfg.schema_evolution == "update"
        natively = cfg.schema_evolution_engine == "native"
        logger.debug(
            "Setting schema evolution for %s (if_table_exists=%s,"
            " schema_evolution=%s, engine=%s)",
            sql_table,
            cfg.if_table_exists,
            cfg.schema_evolution,
            cfg.schema_evolution_engine,
        )
        # Emitted whichever engine is in play, and false for both of the cases
        # that are not native evolution: the mark persists on the table, so a
        # target an earlier native run set would otherwise go on evolving
        # underneath the Iceberg pass -- or after the write asked for strict.
        _set_copy_into_schema_evolution(cursor, evolving and natively, sql_table)
        if evolving and not natively:
            # Reads the staged file before the COPY purges it, and runs before
            # the TRUNCATE so a replace still evolves the table it is about to
            # refill.
            SnowflakeSchemaEvolver(cursor).evolve(
                sql_table, inferred_columns(cursor, stage, file_name)
            )

        if cfg.if_table_exists == "replace":
            logger.debug(
                "Truncating %s before COPY (if_table_exists=replace)", sql_table
            )
            cursor.execute(f"TRUNCATE {sql_table}")
        copy_into(
            cursor,
            stage,
            file_name,
            sql_table,
            match_by_column_name=cfg.match_by_column_name,
            purge=cfg.purge,
        )
    finally:
        cursor.close()


def _merge_one(
    cursor,
    cfg: SnowflakeWriteCfg,
    stage: str,
    file_path: Path,
    sql_table: str,
    merge_cfg: MergeCfg,
) -> None:
    """Drive one CDC merge through the shared driver.

    Everything the merge does -- coalescing the batch, staging it, creating the
    target, evolving it, running the statement -- belongs to the driver and to
    :class:`~tabsdatak.conn.snowflake._merge_dialect.SnowflakeMergeDialect`.

    ``ENABLE_SCHEMA_EVOLUTION`` is not set here, unlike the other dispositions.
    The mark is read by ``COPY INTO``, and the only COPY a merge runs loads the
    scratch table, which is templated from the batch and has nothing to evolve
    onto. A merge evolves its target with ``ALTER`` statements instead.
    """
    merge_batch(
        SnowflakeMergeDialect(
            cursor,
            stage,
            match_by_column_name=cfg.match_by_column_name,
            purge=cfg.purge,
        ),
        target=sql_table,
        batch=file_path,
        cfg=merge_cfg,
        schema_evolution=cfg.schema_evolution,
        ctx=cfg.ctx,
    )

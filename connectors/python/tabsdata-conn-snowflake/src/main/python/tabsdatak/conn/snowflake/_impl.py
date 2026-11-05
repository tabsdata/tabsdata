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
from typing import TYPE_CHECKING

from tabsdatak.conn.common._validation import fqn

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge._driver import merge_batch

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._watermark import watermarked_batch
from tabsdatak.conn.common.types import UserPassword, _convert
from tabsdatak.conn.snowflake import MergeCfg, SnowflakeDestConn
from tabsdatak.conn.snowflake._evolution import (
    SnowflakeSchemaEvolver,
    check_watermark_column,
    ensure_watermark_column,
)
from tabsdatak.conn.snowflake._merge import SnowflakeMergeDialect
from tabsdatak.conn.snowflake._staging import (
    copy_into,
    create_file_format,
    first_part,
    inferred_columns,
    inferred_schema_template,
    staged_frame,
)
from tabsdatak.conn.snowflake._validation import _SNOWFLAKE_FQN_DEF, bare_parts
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode
from tabsdatak.spi import DestContext

if TYPE_CHECKING:
    import polars as pl

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
    ctx: DestContext  # the trx the write belongs to; a write stamps its id
    # The target column each row is stamped with the trx id in, already in the
    # form Snowflake stores it under; ``None`` when the write names no
    # watermark. A merge reads its own off ``merge_cfgs``.
    watermark_column: str | None = None
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
    optional = {
        "role": conn._role(),
        "database": conn._database(),
        "schema": conn._schema(),
        "warehouse": conn._warehouse(),
    }
    params.update({key: value for key, value in optional.items() if value is not None})
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
    return fqn(bare_parts(fq_table), _SNOWFLAKE_FQN_DEF)


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
    database = conn._database()
    if database is None:
        return
    schema = conn._schema()
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
    stage = conn._stage()
    if stage is not None:
        return stage, False
    return _DEFAULT_TEMP_STAGE, True


def _create_stage_if_not_exists(cursor, stage: str, is_temp: bool) -> None:
    if is_temp:
        # Server-side encryption only: a client-side encrypted stage makes the
        # driver encrypt each part into a second copy in memory before the PUT.
        cursor.execute(
            f"CREATE TEMPORARY STAGE IF NOT EXISTS {stage}"
            f" ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')"
        )
    else:
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage}")


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
    tables: list[pl.LazyFrame],
) -> None:
    """Fail before loading anything if a column has a type the bulk load cannot
    carry.

    COPY INTO matches columns by name, which routes every parquet value through
    a variant, and a variant cannot be cast to BINARY ("Failed to cast variant
    value ... to BINARY"). Snowflake reports that per file once the stage and the
    table already exist, so reading the schema up front turns it into one error
    naming the table and the offending columns.
    """
    import polars as pl

    from tabsdatak.conn.snowflake.error import SnowflakeErrorCode

    for table, frame in zip(target_tables, tables):
        offenders = [
            name for name, dtype in frame.collect_schema().items() if dtype == pl.Binary
        ]
        if offenders:
            raise SnowflakeErrorCode.SNOWFLAKE_8.exception(
                table=table,
                columns=", ".join(repr(name) for name in offenders),
            )


def write_all_tables(cfg: SnowflakeWriteCfg, tables: list[pl.LazyFrame]) -> None:
    """Bulk-load each frame in ``tables`` into its corresponding table in
    ``cfg.target_tables``. Opens a single Snowflake connection per call and
    re-uses it across every (frame, table) pair."""
    reject_unsupported_columns(list(cfg.target_tables), tables)

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
        merge_cfgs: list[MergeCfg | None] = (
            list(cfg.merge_cfgs) if cfg.merge_cfgs else [None] * len(cfg.target_tables)
        )
        for i, (frame, table, merge_cfg) in enumerate(
            zip(
                tables,
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
                frame,
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
    frame: pl.LazyFrame,
    table: str,
    merge_cfg: MergeCfg | None = None,
) -> None:
    sql_table = _fq_for_sql(table)
    logger.info("Staging the table for %s", sql_table)
    cursor = sf_conn.cursor()
    try:
        _create_stage_if_not_exists(cursor, stage, is_temp)
        if cfg.if_table_exists == "merge":
            assert merge_cfg is not None
            _merge_one(cursor, cfg, stage, frame, sql_table, merge_cfg)
            return
        # Append is the only disposition that stamps: a merge has returned
        # above, and a replace is refused the column by SnowflakeDest.validate.
        # Named again here rather than inferred from those two, so widening the
        # API later cannot quietly start stamping a disposition that has no
        # business being stamped.
        watermark = cfg.watermark_column if cfg.if_table_exists == "append" else None
        if watermark is not None and check_watermark_column(
            cursor, sql_table, watermark, cfg.ctx.transaction_id
        ):
            # Asked before the batch is staged: a rerun that has nothing to do
            # should not pay to read and ship rows it will not load.
            logger.info(
                "Skipping append onto %s: watermark column %s already has value %s",
                sql_table,
                watermark,
                cfg.ctx.transaction_id,
            )
            return
        with staged_frame(
            cursor,
            watermarked_batch(frame, watermark, cfg.ctx.transaction_id),
            stage,
        ) as folder:
            _create_table_from_inferred_schema(
                cursor, stage, first_part(folder), sql_table
            )
            if watermark is not None:
                # Strict is what makes this necessary: it adds nothing, so without
                # this the target never gains the column, the COPY drops what it
                # cannot match, and the rerun guard reads every run as the first.
                # The same exception the merge carves out for its own watermark.
                #
                # Ahead of the evolution pass so that under "update" the column is
                # already there when the diff runs, leaving one place that adds it
                # rather than two engines each having a claim.
                #
                # A target that predates the watermark keeps NULL in the rows it
                # already holds; only the rows this COPY brings in are stamped.
                ensure_watermark_column(cursor, sql_table, watermark)
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
            if not (evolving and natively):
                # Under "strict" too: a COPY matching by name drops a column the
                # target lacks without a word, so the pass is what refuses it.
                # Reads the staged file before the COPY purges it, and runs before
                # the TRUNCATE so a replace still evolves the table it is about to
                # refill.
                SnowflakeSchemaEvolver(cursor).evolve(
                    sql_table,
                    inferred_columns(cursor, stage, first_part(folder)),
                    cfg.schema_evolution,
                )

            if cfg.if_table_exists == "replace":
                logger.debug(
                    "Truncating %s before COPY (if_table_exists=replace)", sql_table
                )
                cursor.execute(f"TRUNCATE {sql_table}")
            copy_into(
                cursor,
                stage,
                folder,
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
    frame: pl.LazyFrame,
    sql_table: str,
    merge_cfg: MergeCfg,
) -> None:
    """Drive one CDC merge through the shared driver.

    Everything the merge does -- coalescing the batch, staging it, creating the
    target, evolving it, running the statement -- belongs to the driver and to
    :class:`~tabsdatak.conn.snowflake._merge.SnowflakeMergeDialect`.

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
        batch=frame,
        cfg=merge_cfg._sql_merge_cfg(),
        schema_evolution=cfg.schema_evolution,
        ctx=cfg.ctx,
        watermark_column=cfg.watermark_column,
    )

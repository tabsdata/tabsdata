#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from tabsdatak.conn.postgres import (
    DestCfgKey,
    DestCfgSpec,
    PostgresDest,
    PostgresDestConn,
    PostgresSrc,
    PostgresSrcConn,
    SrcCfgKey,
    SrcCfgSpec,
)
from tabsdatak.conn.postgres._explorer import PostgresConnectionExplorer
from tabsdatak.conn.postgres.error import PostgresErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    TableFileInput,
    TableFileSpec,
    TableMode,
)

logger = logging.getLogger(__name__)


# --- src_cfg / dest_cfg keys ----------------------------------------
_KEY_SRC_LOGGING_LEVEL: SrcCfgKey = "postgres.logging_level"
_KEY_SRC_READ_ENGINE: SrcCfgKey = "tabsdata.sql.sql.readEngine"
_KEY_SRC_CHUNK_SIZE: SrcCfgKey = "tabsdata.sql.chunkSize"
_KEY_SRC_DROP_METADATA: SrcCfgKey = "tabsdata.src_metadata.drop"

_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "postgres.logging_level"
_KEY_DEST_WRITE_ENGINE: DestCfgKey = "tabsdata.sql.sql.writeEngine"
_KEY_DEST_CHUNK_SIZE: DestCfgKey = "tabsdata.sql.chunkSize"

_SRC_CFGS_DEFAULTS: dict[SrcCfgKey, Any] = {
    _KEY_SRC_LOGGING_LEVEL: logging.ERROR,
    _KEY_SRC_READ_ENGINE: None,
    _KEY_SRC_CHUNK_SIZE: None,
    _KEY_SRC_DROP_METADATA: False,
}

_DEST_CFGS_DEFAULTS: dict[DestCfgKey, Any] = {
    _KEY_DEST_LOGGING_LEVEL: logging.ERROR,
    _KEY_DEST_WRITE_ENGINE: None,
    _KEY_DEST_CHUNK_SIZE: None,
}


@dataclass(frozen=True)
class _ReaderCfg:
    logging_level: int
    read_engine: str | None
    chunk_size: int | None
    drop_metadata: bool


@dataclass(frozen=True)
class _WriterCfg:
    logging_level: int
    write_engine: str | None
    chunk_size: int | None


def _resolve_src_cfg(src_cfg: SrcCfgSpec | None) -> _ReaderCfg:
    cfg = src_cfg or {}
    for k in cfg:
        if k not in _SRC_CFGS_DEFAULTS:
            logger.warning(
                f"Unknown src_cfg key {k!r}, ignoring (allowed: "
                f"{sorted(_SRC_CFGS_DEFAULTS)})"
            )

    def _get(key: SrcCfgKey) -> Any:
        return cfg.get(key, _SRC_CFGS_DEFAULTS[key])

    raw_chunk = _get(_KEY_SRC_CHUNK_SIZE)
    return _ReaderCfg(
        logging_level=int(_get(_KEY_SRC_LOGGING_LEVEL)),
        read_engine=_get(_KEY_SRC_READ_ENGINE),
        chunk_size=int(raw_chunk) if raw_chunk is not None else None,
        drop_metadata=bool(_get(_KEY_SRC_DROP_METADATA)),
    )


def _resolve_dest_cfg(dest_cfg: DestCfgSpec | None) -> _WriterCfg:
    cfg = dest_cfg or {}
    for k in cfg:
        if k not in _DEST_CFGS_DEFAULTS:
            logger.warning(
                f"Unknown dest_cfg key {k!r}, ignoring (allowed: "
                f"{sorted(_DEST_CFGS_DEFAULTS)})"
            )

    def _get(key: DestCfgKey) -> Any:
        return cfg.get(key, _DEST_CFGS_DEFAULTS[key])

    raw_chunk = _get(_KEY_DEST_CHUNK_SIZE)
    return _WriterCfg(
        logging_level=int(_get(_KEY_DEST_LOGGING_LEVEL)),
        write_engine=_get(_KEY_DEST_WRITE_ENGINE),
        chunk_size=int(raw_chunk) if raw_chunk is not None else None,
    )


# All sqlalchemy, conn-common-sql and DBAPI-driver imports are made
# inside the plugin method bodies below. The conn-postgres package
# should be cheap to import even when no flow actually uses the
# plugin (registry scans pay only for the SrcDef / DestDef metadata
# and the abstract Src / Dest classes, not the runtime IO stack).


class PostgresSrcPlugin(SrcPlugin[PostgresSrcConn, PostgresSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: PostgresSrcConn,
        src: PostgresSrc,
    ) -> list[list[TableFileInput]]:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as factory

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine.initial_values import (
            resolve_initial_values,
        )
        from tabsdatak.conn.common.sql._metadata import to_sql_table_file_inputs
        from tabsdatak.conn.postgres._impl import (
            POSTGRES_READ_DEFAULTS,
            PostgresReadCfg,
        )

        cfg = _resolve_src_cfg(src.src_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("postgres: read_in starting (%d queries)", len(src.queries or []))

        read_cfg = PostgresReadCfg(
            conn=conn,
            queries=src.queries,
            transactional=src.transactional,
            schema_overrides=src.schema_overrides,
            initial_values=resolve_initial_values(ctx, src.initial_values),
            cfg=src.src_cfg,
        )
        engine = factory.get_read_engine(
            src.src_cfg,
            POSTGRES_READ_DEFAULTS,
        )

        try:
            return to_sql_table_file_inputs(
                engine.read(read_cfg, ctx.work_dir),
                read_cfg.queries,
                read_cfg.initial_values,
                drop_metadata=cfg.drop_metadata,
            )
        except OperationalError as e:
            # Engine connect / handshake failures bubble up as
            # OperationalError both for the initial connect and
            # for execute(). Tenacity has already retried up to
            # the deadline; if we got here, retries are exhausted.
            raise PostgresErrorCode.POSTGRES_5.exception(
                cause=e,
                uri=read_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Anything else from SQLAlchemy is a query failure
            # (programming error, statement error, integrity, ...).
            # We don't know which query slot tripped it without
            # cooperation from the loader; surface -1 as the
            # placeholder.
            raise PostgresErrorCode.POSTGRES_6.exception(
                cause=e,
                query_index=-1,
            )


class PostgresDestPlugin(DestPlugin[PostgresDestConn, PostgresDest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: PostgresDestConn,
        dest: PostgresDest,
        tables: list[TableFileSpec],
    ) -> None:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as _engine_factory
        from tabsdatak.conn.postgres import _impl as _postgres_sql
        from tabsdatak.conn.postgres._impl import PostgresWriteCfg

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info(
            "postgres: write_out starting (%d table(s))", len(dest.tables or [])
        )

        # Verifies that the number of incoming parquet files
        # matches the destination's declared ``tables`` (POSTGRES_7).
        if len(dest.tables) != len(tables):
            raise PostgresErrorCode.POSTGRES_7.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        write_cfg = PostgresWriteCfg(
            conn=conn,
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
            cfg=dest.dest_cfg,
        )

        engine = _engine_factory.get_write_engine(
            dest.dest_cfg,
            _postgres_sql.POSTGRES_WRITE_DEFAULTS,
        )
        try:
            engine.write(write_cfg, tables)
        except OperationalError as e:
            raise PostgresErrorCode.POSTGRES_8.exception(
                cause=e,
                uri=write_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Best-effort table identification: if the loader's
            # error includes the table in its message we keep
            # that; otherwise emit the dest's tables as a cue.
            raise PostgresErrorCode.POSTGRES_9.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


POSTGRES_SRC = SrcDef(
    conn=PostgresSrcConn,
    type_="postgres-sql-in",
    system="PostgreSQL",
    src_version="v1",
    src=PostgresSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=PostgresSrcPlugin,
    explorer=PostgresConnectionExplorer,
    icon="conn/postgres/resources/postgresql.svg",
)

POSTGRES_DEST = DestDef(
    conn=PostgresDestConn,
    type_="postgres-sql-out",
    system="PostgreSQL",
    dest_version="v1",
    dest=PostgresDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=PostgresDestPlugin,
    explorer=PostgresConnectionExplorer,
    icon="conn/postgres/resources/postgresql.svg",
)

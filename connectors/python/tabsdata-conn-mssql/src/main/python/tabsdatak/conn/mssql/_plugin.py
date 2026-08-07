#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from tabsdatak.conn.mssql import (
    DestCfgKey,
    DestCfgSpec,
    MSSQLDest,
    MSSQLDestConn,
    MSSQLSrc,
    MSSQLSrcConn,
    SrcCfgKey,
    SrcCfgSpec,
)
from tabsdatak.conn.mssql._explorer import MSSQLServerExplorer
from tabsdatak.conn.mssql.error import MssqlErrorCode
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
_KEY_SRC_LOGGING_LEVEL: SrcCfgKey = "mssql.logging_level"
_KEY_SRC_READ_ENGINE: SrcCfgKey = "tabsdata.sql.sql.readEngine"
_KEY_SRC_CHUNK_SIZE: SrcCfgKey = "tabsdata.sql.chunkSize"

_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "mssql.logging_level"
_KEY_DEST_WRITE_ENGINE: DestCfgKey = "tabsdata.sql.sql.writeEngine"
_KEY_DEST_CHUNK_SIZE: DestCfgKey = "tabsdata.sql.chunkSize"

_SRC_CFGS_DEFAULTS: dict[SrcCfgKey, Any] = {
    _KEY_SRC_LOGGING_LEVEL: logging.ERROR,
    _KEY_SRC_READ_ENGINE: None,
    _KEY_SRC_CHUNK_SIZE: None,
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
# inside the plugin method bodies below. The conn-mssql package
# should be cheap to import even when no flow actually uses the
# plugin (registry scans pay only for the SrcDef / DestDef metadata
# and the abstract Src / Dest classes, not the runtime IO stack).


class MSSQLSrcPlugin(SrcPlugin[MSSQLSrcConn, MSSQLSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: MSSQLSrcConn,
        src: MSSQLSrc,
    ) -> list[list[TableFileInput]]:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as factory

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine.initial_values import (
            resolve_initial_values,
        )
        from tabsdatak.conn.common.sql._metadata import to_sql_table_file_inputs
        from tabsdatak.conn.mssql._impl import MSSQL_READ_DEFAULTS, MSSQLReadCfg

        cfg = _resolve_src_cfg(src.src_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("mssql: read_in starting (%d queries)", len(src.queries or []))

        read_cfg = MSSQLReadCfg(
            conn=conn,
            queries=src.queries,
            transactional=src.transactional,
            schema_overrides=src.schema_overrides,
            initial_values=resolve_initial_values(ctx, src.initial_values),
            cfg=src.src_cfg,
        )
        engine = factory.get_read_engine(
            src.src_cfg,
            MSSQL_READ_DEFAULTS,
        )

        try:
            return to_sql_table_file_inputs(
                engine.read(read_cfg, ctx.work_dir),
                read_cfg.queries,
                read_cfg.initial_values,
            )
        except OperationalError as e:
            # Engine connect / handshake failures bubble up as
            # OperationalError both for the initial connect and
            # for execute(). Tenacity has already retried up to
            # the deadline; if we got here, retries are exhausted.
            raise MssqlErrorCode.MSSQL_5.exception(
                cause=e,
                uri=read_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Anything else from SQLAlchemy is a query failure
            # (programming error, statement error, integrity, ...).
            # We don't know which query slot tripped it without
            # cooperation from the loader; surface -1 as the
            # placeholder.
            raise MssqlErrorCode.MSSQL_6.exception(
                cause=e,
                query_index=-1,
            )


class MSSQLDestPlugin(DestPlugin[MSSQLDestConn, MSSQLDest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: MSSQLDestConn,
        dest: MSSQLDest,
        tables: list[TableFileSpec],
    ) -> None:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as _engine_factory
        from tabsdatak.conn.mssql import _impl as _mssql_sql
        from tabsdatak.conn.mssql._impl import MSSQLWriteCfg

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("mssql: write_out starting (%d table(s))", len(dest.tables or []))

        # Verifies that the number of incoming parquet files
        # matches the destination's declared ``tables`` (MSSQL_7).
        if len(dest.tables) != len(tables):
            raise MssqlErrorCode.MSSQL_7.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        write_cfg = MSSQLWriteCfg(
            conn=conn,
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
            cfg=dest.dest_cfg,
        )

        engine = _engine_factory.get_write_engine(
            dest.dest_cfg,
            _mssql_sql.MSSQL_WRITE_DEFAULTS,
        )
        try:
            engine.write(write_cfg, tables)
        except OperationalError as e:
            raise MssqlErrorCode.MSSQL_8.exception(
                cause=e,
                uri=write_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Best-effort table identification: if the loader's
            # error includes the table in its message we keep
            # that; otherwise emit the dest's tables as a cue.
            raise MssqlErrorCode.MSSQL_9.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


MSSQL_SRC = SrcDef(
    conn=MSSQLSrcConn,
    type_="mssql-sql-in",
    system="MS SQLServer",
    src_version="v1",
    src=MSSQLSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=MSSQLSrcPlugin,
    explorer=MSSQLServerExplorer,
    icon="conn/mssql/resources/mssql-server.svg",
)

MSSQL_DEST = DestDef(
    conn=MSSQLDestConn,
    type_="mssql-sql-out",
    system="MS SQLServer",
    dest_version="v1",
    dest=MSSQLDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=MSSQLDestPlugin,
    explorer=MSSQLServerExplorer,
    icon="conn/mssql/resources/mssql-server.svg",
)

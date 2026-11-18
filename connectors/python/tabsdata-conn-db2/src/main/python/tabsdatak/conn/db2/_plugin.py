#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import polars as pl

from tabsdatak.conn.db2 import (
    Db2Dest,
    Db2DestConn,
    Db2Src,
    Db2SrcConn,
    DestCfgKey,
    DestCfgSpec,
    SrcCfgKey,
    SrcCfgSpec,
)
from tabsdatak.conn.db2._explorer import Db2ConnectionExplorer
from tabsdatak.conn.db2.error import Db2ErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    LazyFrameInput,
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    TableMode,
)

logger = logging.getLogger(__name__)


# --- src_cfg / dest_cfg keys ----------------------------------------
_KEY_SRC_LOGGING_LEVEL: SrcCfgKey = "db2.logging_level"
_KEY_SRC_READ_ENGINE: SrcCfgKey = "tabsdata.sql.read_engine"
_KEY_SRC_CHUNK_SIZE: SrcCfgKey = "tabsdata.sql.chunk_size"
_KEY_SRC_DROP_METADATA: SrcCfgKey = "tabsdata.src_metadata.drop"

_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "db2.logging_level"
_KEY_DEST_WRITE_ENGINE: DestCfgKey = "tabsdata.sql.write_engine"
_KEY_DEST_CHUNK_SIZE: DestCfgKey = "tabsdata.sql.chunk_size"

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
# inside the plugin method bodies below. The conn-db2 package
# should be cheap to import even when no flow actually uses the
# plugin (registry scans pay only for the SrcDef / DestDef metadata
# and the abstract Src / Dest classes, not the runtime IO stack).


class Db2SrcPlugin(SrcPlugin[Db2SrcConn, Db2Src]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: Db2SrcConn,
        src: Db2Src,
    ) -> list[Iterable[LazyFrameInput]]:
        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as factory

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine.initial_values import (
            resolve_initial_values,
        )

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine.reader import SqlReadFaults
        from tabsdatak.conn.db2._impl import DB2_READ_DEFAULTS, Db2ReadCfg

        cfg = _resolve_src_cfg(src.src_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("db2: read_in starting (%d queries)", len(src.queries or []))

        read_cfg = Db2ReadCfg(
            conn=conn,
            queries=src.queries,
            transactional=src.transactional,
            schema_overrides=src.schema_overrides,
            initial_values=resolve_initial_values(ctx, src.initial_values),
            cfg=src.src_cfg,
        )
        engine = factory.get_read_engine(
            src.src_cfg,
            DB2_READ_DEFAULTS,
        )

        return engine.read(
            read_cfg,
            SqlReadFaults(
                unreachable=Db2ErrorCode.DB2_5,
                failed=Db2ErrorCode.DB2_6,
                uri=read_cfg.conn.uri,
            ),
            drop_metadata=cfg.drop_metadata,
        )


class Db2DestPlugin(DestPlugin[Db2DestConn, Db2Dest, list[pl.LazyFrame | None]]):
    def write_out(
        self,
        ctx: DestContext,
        conn: Db2DestConn,
        dest: Db2Dest,
        tables: list[pl.LazyFrame | None],
    ) -> None:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as _engine_factory
        from tabsdatak.conn.db2 import _impl as _db2_sql
        from tabsdatak.conn.db2._impl import Db2WriteCfg

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("db2: write_out starting (%d table(s))", len(dest.tables or []))

        # Verifies that the number of incoming parquet files
        # matches the destination's declared ``tables`` (DB2_7).
        if len(dest.tables) != len(tables):
            raise Db2ErrorCode.DB2_7.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        write_cfg = Db2WriteCfg(
            conn=conn,
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
            schema_evolution=dest._evolution().schema_evolution,
            cfg=dest.dest_cfg,
        )

        engine = _engine_factory.get_write_engine(
            dest.dest_cfg,
            _db2_sql.DB2_WRITE_DEFAULTS,
        )
        try:
            engine.write(write_cfg, tables)
        except OperationalError as e:
            raise Db2ErrorCode.DB2_8.exception(
                cause=e,
                uri=write_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Best-effort table identification: if the loader's
            # error includes the table in its message we keep
            # that; otherwise emit the dest's tables as a cue.
            raise Db2ErrorCode.DB2_9.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


DB2_SRC = SrcDef(
    conn=Db2SrcConn,
    type_="db2-sql-in",
    system="IBM Db2",
    src_version="v1",
    src=Db2Src,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=Db2SrcPlugin,
    explorer=Db2ConnectionExplorer,
    icon="conn/db2/resources/db2.svg",
)

DB2_DEST = DestDef(
    conn=Db2DestConn,
    type_="db2-sql-out",
    system="IBM Db2",
    dest_version="v1",
    dest=Db2Dest,
    cardinality=lambda dest: len(dest.tables),
    plugin=Db2DestPlugin,
    explorer=Db2ConnectionExplorer,
    icon="conn/db2/resources/db2.svg",
)

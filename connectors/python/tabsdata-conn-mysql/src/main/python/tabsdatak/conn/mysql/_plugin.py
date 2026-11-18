#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, cast

import polars as pl

from tabsdatak.conn.mysql import (
    DestCfgKey,
    DestCfgSpec,
    MySQLDest,
    MySQLDestConn,
    MySQLSrc,
    MySQLSrcConn,
    SrcCfgKey,
    SrcCfgSpec,
)
from tabsdatak.conn.mysql._explorer import MySQLConnectionExplorer
from tabsdatak.conn.mysql.error import MysqlErrorCode
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
#
# The ``SrcCfgKey`` / ``DestCfgKey`` Literals live in ``__init__.py``
# (so they travel with the schema). The ``_KEY_*`` constants below are
# the runtime spellings the plugin pulls out of ``src_cfg`` /
# ``dest_cfg``; their ``SrcCfgKey`` / ``DestCfgKey`` annotation means a
# typo or rename gets flagged against the schema's Literal.
_KEY_SRC_LOGGING_LEVEL: SrcCfgKey = "mysql.logging_level"
_KEY_SRC_READ_ENGINE: SrcCfgKey = "tabsdata.sql.read_engine"
_KEY_SRC_CHUNK_SIZE: SrcCfgKey = "tabsdata.sql.chunk_size"
_KEY_SRC_DROP_METADATA: SrcCfgKey = "tabsdata.src_metadata.drop"

_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "mysql.logging_level"
_KEY_DEST_WRITE_ENGINE: DestCfgKey = "tabsdata.sql.write_engine"
_KEY_DEST_CHUNK_SIZE: DestCfgKey = "tabsdata.sql.chunk_size"

# Defaults for every ``SrcCfgKey`` / ``DestCfgKey``. ``_resolve_*``
# reads from cfg and falls back to these. ``None`` for engine /
# chunk-size means "let the SQL engine factory pick its own default";
# the plugin still passes ``src_cfg`` through to that factory so the
# real defaults stay centralized there.
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
    # Pydantic's ``SrcCfgSpec`` (Mapping[SrcCfgKey, Any]) rejects
    # unknown keys at construction, so reaching this branch means the
    # cfg was assembled in a way that bypassed validation. Warn and
    # carry on -- known keys still resolve normally.
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
# inside the plugin method bodies below. The conn-mysql package
# should be cheap to import even when no flow actually uses the
# plugin (registry scans pay only for the SrcDef / DestDef metadata
# and the abstract Src / Dest classes, not the runtime IO stack).


class MySQLSrcPlugin(SrcPlugin[MySQLSrcConn, MySQLSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: MySQLSrcConn,
        src: MySQLSrc,
    ) -> list[Iterable[LazyFrameInput]]:
        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as factory

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine.initial_values import (
            resolve_initial_values,
        )

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine.reader import SqlReadFaults
        from tabsdatak.conn.mysql._impl import MYSQL_READ_DEFAULTS, MySQLReadCfg

        cfg = _resolve_src_cfg(src.src_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("mysql: read_in starting (%d queries)", len(src.queries or []))

        read_cfg = MySQLReadCfg(
            conn=conn,
            queries=src.queries,
            transactional=src.transactional,
            schema_overrides=src.schema_overrides,
            initial_values=resolve_initial_values(ctx, src.initial_values),
            cfg=cast(dict, src.src_cfg),
        )
        engine = factory.get_read_engine(
            cast(dict, src.src_cfg),
            MYSQL_READ_DEFAULTS,
        )

        return engine.read(
            read_cfg,
            SqlReadFaults(
                unreachable=MysqlErrorCode.MYSQL_5,
                failed=MysqlErrorCode.MYSQL_6,
                uri=read_cfg.conn.uri,
            ),
            drop_metadata=cfg.drop_metadata,
        )


class MySQLDestPlugin(DestPlugin[MySQLDestConn, MySQLDest, list[pl.LazyFrame | None]]):
    def write_out(
        self,
        ctx: DestContext,
        conn: MySQLDestConn,
        dest: MySQLDest,
        tables: list[pl.LazyFrame | None],
    ) -> None:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as _engine_factory
        from tabsdatak.conn.mysql import _impl as _mysql_sql
        from tabsdatak.conn.mysql._impl import MySQLWriteCfg

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("mysql: write_out starting (%d table(s))", len(dest.tables or []))

        # Verifies that the number of incoming parquet files
        # matches the destination's declared ``tables`` (MYSQL_7).
        if len(dest.tables) != len(tables):
            raise MysqlErrorCode.MYSQL_7.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        write_cfg = MySQLWriteCfg(
            conn=conn,
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
            schema_evolution=dest._evolution().schema_evolution,
            cfg=cast(dict, dest.dest_cfg),
        )

        engine = _engine_factory.get_write_engine(
            cast(dict, dest.dest_cfg),
            _mysql_sql.MYSQL_WRITE_DEFAULTS,
        )
        try:
            engine.write(write_cfg, tables)
        except OperationalError as e:
            raise MysqlErrorCode.MYSQL_8.exception(
                cause=e,
                uri=write_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Best-effort table identification: if the loader's
            # error includes the table in its message we keep
            # that; otherwise emit the dest's tables as a cue.
            raise MysqlErrorCode.MYSQL_9.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


MYSQL_SRC = SrcDef(
    conn=MySQLSrcConn,
    type_="mysql-sql-in",
    system="MySQL",
    src_version="v1",
    src=MySQLSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=MySQLSrcPlugin,
    explorer=MySQLConnectionExplorer,
    icon="conn/mysql/resources/mysql.svg",
)

MYSQL_DEST = DestDef(
    conn=MySQLDestConn,
    type_="mysql-sql-out",
    system="MySQL",
    dest_version="v1",
    dest=MySQLDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=MySQLDestPlugin,
    explorer=MySQLConnectionExplorer,
    icon="conn/mysql/resources/mysql.svg",
)

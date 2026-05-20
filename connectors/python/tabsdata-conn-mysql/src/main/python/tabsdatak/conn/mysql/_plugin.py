from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, cast

from tabsdatak.conn.mysql import (
    DestCfgKey,
    DestCfgSpec,
    MySQLConn,
    MySQLDest,
    MySQLSrc,
    SrcCfgKey,
    SrcCfgSpec,
)
from tabsdatak.conn.mysql.error import MysqlErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    SrcContext,
    SrcDef,
    SrcPlugin,
    TableFileSpec,
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
_KEY_SRC_READ_ENGINE: SrcCfgKey = "tabsdata.sql.sql.readEngine"
_KEY_SRC_CHUNK_SIZE: SrcCfgKey = "tabsdata.sql.chunkSize"

_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "mysql.logging_level"
_KEY_DEST_WRITE_ENGINE: DestCfgKey = "tabsdata.sql.sql.writeEngine"
_KEY_DEST_CHUNK_SIZE: DestCfgKey = "tabsdata.sql.chunkSize"

# Defaults for every ``SrcCfgKey`` / ``DestCfgKey``. ``_resolve_*``
# reads from cfg and falls back to these. ``None`` for engine /
# chunk-size means "let the SQL engine factory pick its own default";
# the plugin still passes ``src_cfg`` through to that factory so the
# real defaults stay centralized there.
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


class MySQLSrcPlugin(SrcPlugin[MySQLSrc]):
    def read_in(
        self,
        ctx: SrcContext,
        src: MySQLSrc,
    ) -> list[list[TableFileSpec]]:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as factory
        from tabsdatak.conn.mysql._impl import MYSQL_READ_DEFAULTS, MySQLReadCfg

        cfg = _resolve_src_cfg(src.src_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("mysql: read_in starting (%d queries)", len(src.queries or []))

        read_cfg = MySQLReadCfg(
            conn=cast(MySQLConn, src.conn),
            queries=src.queries,
            transactional=src.transactional,
            schema_overrides=src.schema_overrides,
            query_params=src.query_params or None,
            cfg=cast(dict, src.src_cfg),
        )
        engine = factory.get_read_engine(
            cast(dict, src.src_cfg),
            MYSQL_READ_DEFAULTS,
        )

        try:
            return engine.read(read_cfg, ctx.work_dir())
        except OperationalError as e:
            # Engine connect / handshake failures bubble up as
            # OperationalError both for the initial connect and
            # for execute(). Tenacity has already retried up to
            # the deadline; if we got here, retries are exhausted.
            raise MysqlErrorCode.MYSQL_5.exception(
                cause=e,
                uri=read_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            # Anything else from SQLAlchemy is a query failure
            # (programming error, statement error, integrity, ...).
            # We don't know which query slot tripped it without
            # cooperation from the loader; surface -1 as the
            # placeholder.
            raise MysqlErrorCode.MYSQL_6.exception(
                cause=e,
                query_index=-1,
            )


class MySQLDestPlugin(DestPlugin[MySQLDest]):
    def write_out(
        self,
        ctx: DestContext,
        dest: MySQLDest,
        tables: list[TableFileSpec],
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
            conn=cast(MySQLConn, dest.conn),
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
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


class MySQLExplorer(Explorer[MySQLConn]):
    def metadata(self, conn: MySQLConn) -> Any:
        # TODO
        pass

    def data(self, conn: MySQLConn, query: str) -> Any:
        # TODO
        pass


MYSQL_SRC = SrcDef(
    conn=MySQLConn,
    src_type="mysql",
    src_version="v1",
    src=MySQLSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=MySQLSrcPlugin,
    explorer=MySQLExplorer,
)

MYSQL_DEST = DestDef(
    conn=MySQLConn,
    dest_type="mysql",
    dest_version="v1",
    dest=MySQLDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=MySQLDestPlugin,
    explorer=MySQLExplorer,
)

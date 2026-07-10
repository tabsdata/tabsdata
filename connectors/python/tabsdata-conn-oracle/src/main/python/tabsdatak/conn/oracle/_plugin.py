from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from tabsdatak.conn.oracle import (
    DestCfgKey,
    DestCfgSpec,
    OracleDest,
    OracleDestConn,
    OracleSrc,
    OracleSrcConn,
    SrcCfgKey,
    SrcCfgSpec,
)
from tabsdatak.conn.oracle.error import OracleErrorCode
from tabsdatak.conn.oracle._explorer import OracleConnectionExplorer
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    TableFileInput,
    TableFileSpec,
    TableMode,
    to_table_file_inputs,
)

logger = logging.getLogger(__name__)


# --- src_cfg / dest_cfg keys ----------------------------------------
_KEY_SRC_LOGGING_LEVEL: SrcCfgKey = "oracle.logging_level"
_KEY_SRC_READ_ENGINE: SrcCfgKey = "tabsdata.sql.sql.readEngine"
_KEY_SRC_CHUNK_SIZE: SrcCfgKey = "tabsdata.sql.chunkSize"

_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "oracle.logging_level"
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
# inside the plugin method bodies below. The conn-oracle package
# should be cheap to import even when no flow actually uses the
# plugin (registry scans pay only for the SrcDef / DestDef metadata
# and the abstract Src / Dest classes, not the runtime IO stack).


class OracleSrcPlugin(SrcPlugin[OracleSrcConn, OracleSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: OracleSrcConn,
        src: OracleSrc,
    ) -> list[list[TableFileInput]]:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as factory

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine.initial_values import (
            resolve_initial_values,
        )
        from tabsdatak.conn.oracle._impl import ORACLE_READ_DEFAULTS, OracleReadCfg

        cfg = _resolve_src_cfg(src.src_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("oracle: read_in starting (%d queries)", len(src.queries or []))

        read_cfg = OracleReadCfg(
            conn=conn,
            queries=src.queries,
            transactional=src.transactional,
            schema_overrides=src.schema_overrides,
            initial_values=resolve_initial_values(ctx, src.initial_values),
            cfg=src.src_cfg,
        )
        engine = factory.get_read_engine(
            src.src_cfg,
            ORACLE_READ_DEFAULTS,
        )

        try:
            return to_table_file_inputs(engine.read(read_cfg, ctx.work_dir))
        except OperationalError as e:
            raise OracleErrorCode.ORACLE_5.exception(
                cause=e,
                uri=read_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            raise OracleErrorCode.ORACLE_6.exception(
                cause=e,
                query_index=-1,
            )


class OracleDestPlugin(DestPlugin[OracleDestConn, OracleDest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: OracleDestConn,
        dest: OracleDest,
        tables: list[TableFileSpec],
    ) -> None:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.common.sql._engine import factory as _engine_factory
        from tabsdatak.conn.oracle import _impl as _oracle_sql
        from tabsdatak.conn.oracle._impl import OracleWriteCfg

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("oracle: write_out starting (%d table(s))", len(dest.tables or []))

        # Verifies that the number of incoming parquet files
        # matches the destination's declared ``tables`` (ORACLE_7).
        if len(dest.tables) != len(tables):
            raise OracleErrorCode.ORACLE_7.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        write_cfg = OracleWriteCfg(
            conn=conn,
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            transactional=dest.transactional,
            cfg=dest.dest_cfg,
        )

        engine = _engine_factory.get_write_engine(
            dest.dest_cfg,
            _oracle_sql.ORACLE_WRITE_DEFAULTS,
        )
        try:
            engine.write(write_cfg, tables)
        except OperationalError as e:
            raise OracleErrorCode.ORACLE_8.exception(
                cause=e,
                uri=write_cfg.conn.uri,
            )
        except SQLAlchemyError as e:
            raise OracleErrorCode.ORACLE_9.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


ORACLE_SRC = SrcDef(
    conn=OracleSrcConn,
    type_="oracle-sql-in",
    system="Oracle",
    src_version="v1",
    src=OracleSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=OracleSrcPlugin,
    explorer=OracleConnectionExplorer,
    icon="conn/oracle/resources/oracle.svg",
)

ORACLE_DEST = DestDef(
    conn=OracleDestConn,
    type_="oracle-sql-out",
    system="Oracle",
    dest_version="v1",
    dest=OracleDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=OracleDestPlugin,
    explorer=OracleConnectionExplorer,
    icon="conn/oracle/resources/oracle.svg",
)

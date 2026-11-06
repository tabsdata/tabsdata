#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

import polars as pl

from tabsdatak.conn.starrocks import (
    DestCfgKey,
    DestCfgSpec,
    StarRocksDest,
    StarRocksDestConn,
)
from tabsdatak.conn.starrocks._explorer import StarRocksConnectionExplorer
from tabsdatak.conn.starrocks.error import StarRocksErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
)

logger = logging.getLogger(__name__)


# --- dest_cfg keys --------------------------------------------------
_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "starrocks.logging_level"
_KEY_DEST_SCHEMA_EVOLUTION_ENGINE: DestCfgKey = "starrocks.schema_evolution_engine"

_DEST_CFGS_DEFAULTS: dict[DestCfgKey, Any] = {
    _KEY_DEST_LOGGING_LEVEL: logging.ERROR,
    _KEY_DEST_SCHEMA_EVOLUTION_ENGINE: "iceberg",
}

_SCHEMA_EVOLUTION_ENGINES = ("iceberg",)


@dataclass(frozen=True)
class _WriterCfg:
    logging_level: int


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

    engine = _get(_KEY_DEST_SCHEMA_EVOLUTION_ENGINE)
    if engine not in _SCHEMA_EVOLUTION_ENGINES:
        raise StarRocksErrorCode.STARROCKS_13.exception(
            value=engine,
            allowed=" or ".join(repr(e) for e in _SCHEMA_EVOLUTION_ENGINES),
        )

    return _WriterCfg(
        logging_level=int(_get(_KEY_DEST_LOGGING_LEVEL)),
    )


# All sqlalchemy and object-store imports are made inside the plugin
# method bodies below. The conn-starrocks package should be cheap to
# import even when no flow actually uses the plugin (registry scans
# pay only for the DestDef metadata and the abstract Dest class, not
# the runtime IO stack).


class StarRocksDestPlugin(
    DestPlugin[StarRocksDestConn, StarRocksDest, list[pl.LazyFrame]],
):
    def write_out(
        self,
        ctx: DestContext,
        conn: StarRocksDestConn,
        dest: StarRocksDest,
        tables: list[pl.LazyFrame],
    ) -> None:
        from sqlalchemy.exc import OperationalError, SQLAlchemyError

        # noinspection PyProtectedMember
        from tabsdatak.conn.starrocks import _impl as adapter

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info(
            "starrocks: write_out starting (%d table(s))", len(dest.tables or [])
        )

        # Verifies that the number of incoming tables matches
        # the destination's declared ``tables`` (STARROCKS_3).
        if len(dest.tables) != len(tables):
            raise StarRocksErrorCode.STARROCKS_3.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        host = conn._host()
        port = conn._port()
        database = conn._database()

        write_cfg = adapter.StarRocksWriteCfg(
            host=host,
            port=port,
            database=database,
            credentials=conn.credentials,
            staging=conn.staging,
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            schema_evolution=dest.schema_evolution,
            conn_cfg=dict(conn.conn_cfg) if conn.conn_cfg else None,
        )

        # A fresh upload id keys this write's staged files so
        # multiple concurrent transactions on the same staging bucket
        # don't collide. The framework's ``transaction_id`` would be ideal but
        # is opaque here; a uuid is sufficient.
        upload_id = uuid.uuid4().hex

        try:
            adapter.execute(write_cfg, list(tables), upload_id)
        except OperationalError as e:
            raise StarRocksErrorCode.STARROCKS_4.exception(
                cause=e,
                host=host,
                port=port,
            )
        except SQLAlchemyError as e:
            raise StarRocksErrorCode.STARROCKS_5.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


STARROCKS_DEST = DestDef(
    conn=StarRocksDestConn,
    type_="starrocks-bulk-out",
    system="StarRocks",
    dest_version="v1",
    dest=StarRocksDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=StarRocksDestPlugin,
    explorer=StarRocksConnectionExplorer,
    icon="conn/starrocks/resources/starrocks.svg",
)

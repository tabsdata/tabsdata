#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from tabsdatak.conn.snowflake import (
    DestCfgKey,
    DestCfgSpec,
    SnowflakeDest,
    SnowflakeDestConn,
)
from tabsdatak.conn.snowflake._explorer import SnowflakeConnectionExplorer
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    TableFileSpec,
)

logger = logging.getLogger(__name__)


# --- dest_cfg keys --------------------------------------------------
_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "snowflake.logging_level"
_KEY_DEST_SCHEMA_EVOLUTION_ENGINE: DestCfgKey = "snowflake.schema_evolution_engine"

_DEST_CFGS_DEFAULTS: dict[DestCfgKey, Any] = {
    _KEY_DEST_LOGGING_LEVEL: logging.ERROR,
    _KEY_DEST_SCHEMA_EVOLUTION_ENGINE: "native",
}

_SCHEMA_EVOLUTION_ENGINES = ("native", "iceberg")


@dataclass(frozen=True)
class _WriterCfg:
    logging_level: int
    schema_evolution_engine: str


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
        # A typo here would otherwise fall back to native evolution and lose
        # the widening the write asked for, with nothing said about it.
        # noinspection PyProtectedMember
        raise SnowflakeErrorCode.SNOWFLAKE_21.exception(
            value=engine,
            allowed=" or ".join(repr(e) for e in _SCHEMA_EVOLUTION_ENGINES),
        )

    return _WriterCfg(
        logging_level=int(_get(_KEY_DEST_LOGGING_LEVEL)),
        schema_evolution_engine=engine,
    )


# All snowflake.connector + _impl imports are made inside the plugin
# method bodies below. The conn-snowflake package should be cheap to
# import even when no flow actually uses the plugin (registry scans
# pay only for the DestDef metadata and the abstract Dest class, not
# the runtime IO stack).


# --- Connection-class error classifier ------------------------------
#
# snowflake-connector-python's exception hierarchy:
#   - DatabaseError: connection / auth / transport (we surface this
#     as SNOWFLAKE_5)
#   - ProgrammingError: SQL / COPY-INTO failure (SNOWFLAKE_6)
#   - InterfaceError, OperationalError: transport-level connection
#     failures (also SNOWFLAKE_5)
# We inspect by class name so the conn-snowflake package stays cheap
# to import.
def _is_connection_error(exc: BaseException) -> bool:
    name = type(exc).__name__
    return name in {
        "DatabaseError",
        "InterfaceError",
        "OperationalError",
    }


class SnowflakeDestPlugin(DestPlugin[SnowflakeDestConn, SnowflakeDest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: SnowflakeDestConn,
        dest: SnowflakeDest,
        tables: list[TableFileSpec],
    ) -> None:
        # noinspection PyProtectedMember
        from tabsdatak.conn.snowflake import _impl

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)
        # snowflake-connector-python's own logger is markedly noisier
        # than ours; pin it to the caller-requested level so it
        # doesn't fight us.
        logging.getLogger("snowflake").setLevel(cfg.logging_level)

        # Verifies that the number of incoming parquet files matches
        # the destination's declared ``tables`` (SNOWFLAKE_4).
        if len(dest.tables) != len(tables):
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_4.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        logger.info("snowflake: write_out starting (%d table(s))", len(dest.tables))
        write_cfg = _impl.SnowflakeWriteCfg(
            conn=conn,
            target_tables=list(dest.tables),
            schema_evolution=dest.schema_evolution,
            schema_evolution_engine=cfg.schema_evolution_engine,
            if_table_exists=dest.if_table_exists,
            ctx=ctx,
            merge_cfgs=dest._merge_cfgs(),
        )
        try:
            _impl.write_all_tables(write_cfg, list(tables))
        except Exception as e:  # noqa: BLE001 -- runtime wrapper
            if _is_connection_error(e):
                # noinspection PyProtectedMember
                raise SnowflakeErrorCode.SNOWFLAKE_5.exception(
                    cause=e,
                    account=conn._account(),
                )
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_6.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )
        logger.info("snowflake: write_out finished (%d table(s))", len(dest.tables))


SNOWFLAKE_DEST = DestDef(
    conn=SnowflakeDestConn,
    type_="snowflake-bulk-out",
    system="Snowflake",
    dest_version="v1",
    dest=SnowflakeDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=SnowflakeDestPlugin,
    explorer=SnowflakeConnectionExplorer,
    icon="conn/snowflake/resources/snowflake.svg",
)

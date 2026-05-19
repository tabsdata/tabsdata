from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, cast

from tabsdatak.conn.snowflake import (
    DestCfgKey,
    DestCfgSpec,
    SnowflakeConn,
    SnowflakeDest,
)
from tabsdatak.conn.snowflake.error import SnowflakeErrorCode
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    TableFileSpec,
)

logger = logging.getLogger(__name__)


# --- dest_cfg keys --------------------------------------------------
_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "snowflake.logging_level"

_DEST_CFGS_DEFAULTS: dict[DestCfgKey, Any] = {
    _KEY_DEST_LOGGING_LEVEL: logging.ERROR,
}


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

    return _WriterCfg(
        logging_level=int(_get(_KEY_DEST_LOGGING_LEVEL)),
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


class SnowflakeDestPlugin(DestPlugin[SnowflakeDest]):
    def write_out(
        self,
        ctx: DestContext,
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

        conn = cast(SnowflakeConn, dest.conn)
        write_cfg = _impl.SnowflakeWriteCfg(
            conn=conn,
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
        )
        try:
            _impl.write_all_tables(write_cfg, list(tables))
        except Exception as e:  # noqa: BLE001 -- runtime wrapper
            if _is_connection_error(e):
                # noinspection PyProtectedMember
                raise SnowflakeErrorCode.SNOWFLAKE_5.exception(
                    cause=e,
                    account=conn.account.value(),
                )
            # noinspection PyProtectedMember
            raise SnowflakeErrorCode.SNOWFLAKE_6.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


class SnowflakeExplorer(Explorer[SnowflakeConn]):
    def metadata(self, conn: SnowflakeConn) -> Any:
        # TODO
        pass

    def data(self, conn: SnowflakeConn, query: str) -> Any:
        # TODO
        pass


SNOWFLAKE_DEST = DestDef(
    conn=SnowflakeConn,
    dest_type="snowflake",
    dest_version="v1",
    dest=SnowflakeDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=SnowflakeDestPlugin,
    explorer=SnowflakeExplorer,
)

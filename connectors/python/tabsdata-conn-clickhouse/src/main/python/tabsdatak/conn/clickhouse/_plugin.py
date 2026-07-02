from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from tabsdatak.conn.clickhouse import (
    ClickHouseDest,
    ClickHouseDestConn,
    DestCfgKey,
    DestCfgSpec,
)
from tabsdatak.conn.clickhouse.error import ClickHouseErrorCode

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    TableFileSpec,
)

logger = logging.getLogger(__name__)


# --- dest_cfg keys --------------------------------------------------
#
# The ``DestCfgKey`` Literal lives in ``__init__.py`` (so it travels
# with the schema). The ``_KEY_*`` constants below are the runtime
# spellings the plugin pulls out of ``dest_cfg``; their
# ``DestCfgKey`` annotation means a typo or rename gets flagged
# against the schema's Literal.
_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "clickhouse.logging_level"

# Defaults for every ``DestCfgKey``. ``_resolve_dest_cfg`` reads
# from cfg and falls back to these.
_DEST_CFGS_DEFAULTS: dict[DestCfgKey, Any] = {
    _KEY_DEST_LOGGING_LEVEL: logging.ERROR,
}


@dataclass(frozen=True)
class _WriterCfg:
    logging_level: int


def _resolve_dest_cfg(dest_cfg: DestCfgSpec | None) -> _WriterCfg:
    cfg = dest_cfg or {}
    # Pydantic's ``DestCfgSpec`` (Mapping[DestCfgKey, Any]) rejects
    # unknown keys at construction, so reaching this branch means the
    # cfg was assembled in a way that bypassed validation. Warn and
    # carry on -- known keys still resolve normally.
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


# Impl / cloud-staging imports are made inside the plugin method
# bodies below.  The conn-clickhouse package should be cheap to
# import even when no flow actually uses the plugin (registry
# scans pay only for the DestDef metadata and the abstract Dest
# classes, not the runtime IO stack).


class _ClickHouseDestPlugin(DestPlugin[ClickHouseDestConn, ClickHouseDest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: ClickHouseDestConn,
        dest: ClickHouseDest,
        tables: list[TableFileSpec],
    ) -> None:
        # noinspection PyProtectedMember
        from tabsdatak.conn.clickhouse import _impl

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info(
            "clickhouse: write_out starting (%d table(s))", len(dest.tables or [])
        )

        # Verifies that the number of incoming parquet files
        # matches the destination's declared ``tables``
        # (CLICKHOUSE_4).
        if len(dest.tables) != len(tables):
            raise ClickHouseErrorCode.CLICKHOUSE_4.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        write_cfg = _impl.ClickHouseWriteCfg(
            conn=conn,
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            schema_evolution=dest.schema_evolution,
        )

        try:
            _impl.write_via_bulk_loader(write_cfg, list(tables))
        except TypeError as e:
            # Unsupported staging location surfaced by
            # ``build_cloud_storage``.
            raise ClickHouseErrorCode.CLICKHOUSE_7.exception(
                cause=e,
                staging=type(conn.staging).__name__,
            )
        except Exception as e:  # noqa: BLE001 -- runtime wrapper
            host = _convert(conn.host, str)
            # Heuristic: connect-class failures from
            # clickhouse-connect raise ``DatabaseError`` or
            # similar; we surface the connection-failure code
            # when a known clickhouse-connect connection error
            # bubbles up, otherwise the generic bulk-load code.
            if _is_connection_error(e):
                raise ClickHouseErrorCode.CLICKHOUSE_5.exception(
                    cause=e,
                    host=host,
                )
            raise ClickHouseErrorCode.CLICKHOUSE_6.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


def _is_connection_error(exc: BaseException) -> bool:
    """Best-effort classification of a clickhouse-connect failure.

    ``clickhouse_connect`` raises ``DatabaseError`` for connect /
    handshake failures; we look at the exception name rather than
    importing the package eagerly so the conn-clickhouse package
    stays cheap to import.
    """
    name = type(exc).__name__
    return name in {
        "DatabaseError",
        "OperationalError",
        "InterfaceError",
        "ConnectionError",
    }


class _ClickHouseExplorer(Explorer[ClickHouseDestConn]):
    def metadata(self, conn: ClickHouseDestConn) -> Any:
        # TODO
        pass

    def data(self, conn: ClickHouseDestConn, query: str) -> Any:
        # TODO
        pass


CLICKHOUSE_DEST = DestDef(
    conn=ClickHouseDestConn,
    type_="clickhouse-bulk-out",
    system="ClickHouse",
    dest_version="v1",
    dest=ClickHouseDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=_ClickHouseDestPlugin,
    explorer=_ClickHouseExplorer,
    icon="conn/clickhouse/resources/clickhouse.svg",
)

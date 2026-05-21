from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, cast

from tabsdatak.conn.databricks import (
    DatabricksConn,
    DatabricksDest,
    DatabricksSrc,
    DestCfgKey,
    DestCfgSpec,
    SrcCfgKey,
    SrcCfgSpec,
)
from tabsdatak.conn.databricks.error import (
    DatabricksErrorCode,
    DatabricksValidateException,
)
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    ParquetFile,
    SrcPluginCtx,
    SrcDef,
    SrcPlugin,
    TableFileSpec,
    TableMode,
)

logger = logging.getLogger(__name__)


# --- src_cfg / dest_cfg keys ----------------------------------------
#
# The ``SrcCfgKey`` / ``DestCfgKey`` Literals live in ``__init__.py``
# (so they travel with the schema). The ``_KEY_*`` constants below
# are the runtime spellings the plugin pulls out of ``src_cfg`` /
# ``dest_cfg``; their ``SrcCfgKey`` / ``DestCfgKey`` annotation means
# a typo or rename gets flagged against the schema's Literal.
_KEY_SRC_LOGGING_LEVEL: SrcCfgKey = "databricks.logging_level"
_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "databricks.logging_level"

_SRC_CFGS_DEFAULTS: dict[SrcCfgKey, Any] = {
    _KEY_SRC_LOGGING_LEVEL: logging.ERROR,
}

_DEST_CFGS_DEFAULTS: dict[DestCfgKey, Any] = {
    _KEY_DEST_LOGGING_LEVEL: logging.ERROR,
}


@dataclass(frozen=True)
class _ReaderCfg:
    logging_level: int


@dataclass(frozen=True)
class _WriterCfg:
    logging_level: int


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

    return _ReaderCfg(
        logging_level=int(_get(_KEY_SRC_LOGGING_LEVEL)),
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

    return _WriterCfg(
        logging_level=int(_get(_KEY_DEST_LOGGING_LEVEL)),
    )


# All databricks-sdk / databricks-sql / requests / pyarrow imports
# are made inside the plugin method bodies (and inside ``_impl``).
# The conn-databricks package should be cheap to import even when no
# flow actually uses the plugin (registry scans pay only for the
# SrcDef / DestDef metadata).


# --- Connection-class error classifier ------------------------------
#
# databricks-sdk raises ``DatabricksError`` (and subclasses
# ``Unauthenticated`` / ``PermissionDenied`` / etc.); databricks-sql
# raises ``InterfaceError`` / ``OperationalError`` on connection
# trouble. We inspect by class name so the conn-databricks package
# stays cheap to import.
def _is_connection_error(exc: BaseException) -> bool:
    name = type(exc).__name__
    return name in {
        "DatabricksError",
        "Unauthenticated",
        "PermissionDenied",
        "InterfaceError",
        "OperationalError",
        "ConnectionError",
    }


class _DatabricksSrcPlugin(SrcPlugin[DatabricksSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        src: DatabricksSrc,
    ) -> list[list[TableFileSpec]]:
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks import _impl

        cfg = _resolve_src_cfg(src.src_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("databricks: read_in starting (%d queries)", len(src.queries or []))
        # The databricks SDK's own logger is far noisier than the
        # caller's logger.setLevel here -- forward the same level
        # so the SDK doesn't fight us.
        logging.getLogger("databricks").setLevel(cfg.logging_level)

        conn = cast(DatabricksConn, src.conn)
        read_cfg = _impl.DatabricksReadCfg(
            conn=conn,
            queries=list(src.queries),
            initial_values=(
                dict(src.initial_values) if src.initial_values is not None else None
            ),
        )
        host = (
            conn.host_url.value() if hasattr(conn.host_url, "value") else conn.host_url
        )
        try:
            parquet_paths = _impl.read_all_queries(
                read_cfg, ctx.work_dir
            )
        except DatabricksValidateException:
            raise  # adapter-side validation is already typed
        except Exception as e:  # noqa: BLE001 -- runtime wrapper
            if _is_connection_error(e):
                raise DatabricksErrorCode.DATABRICKS_5.exception(
                    cause=e,
                    host=host,
                )
            # Query/download failure; we don't know which slot
            # without cooperation from the adapter, so emit -1.
            raise DatabricksErrorCode.DATABRICKS_6.exception(
                cause=e,
                query_index=-1,
            )

        # One parquet per query; SrcDef.table_mode is SINGLE.
        # ``TableFileSpec`` aliases ``ParquetFile``, a ``Path``
        # subclass -- wrap each adapter-returned path.
        return [[ParquetFile(p)] for p in parquet_paths]


class _DatabricksDestPlugin(DestPlugin[DatabricksDest]):
    def write_out(
        self,
        ctx: DestContext,
        dest: DatabricksDest,
        tables: list[TableFileSpec],
    ) -> None:
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks import _impl

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info(
            "databricks: write_out starting (%d table(s))", len(dest.tables or [])
        )
        logging.getLogger("databricks").setLevel(cfg.logging_level)

        # Verifies that the number of incoming parquet files matches
        # the destination's declared ``tables`` (DATABRICKS_7).
        if len(dest.tables) != len(tables):
            raise DatabricksErrorCode.DATABRICKS_7.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        conn = cast(DatabricksConn, dest.conn)
        write_cfg = _impl.DatabricksWriteCfg(
            conn=conn,
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            schema_evolution=dest.schema_evolution,
        )
        host = (
            conn.host_url.value() if hasattr(conn.host_url, "value") else conn.host_url
        )
        try:
            # ``tables`` is ``list[TableFileSpec]`` =
            # ``list[ParquetFile]`` (a ``Path`` subclass), so each
            # entry is path-like directly -- no ``.path`` access.
            _impl.write_all_tables(write_cfg, list(tables))
        except DatabricksValidateException:
            raise  # adapter-side validation is already typed (DATABRICKS_10)
        except Exception as e:  # noqa: BLE001 -- runtime wrapper
            if _is_connection_error(e):
                raise DatabricksErrorCode.DATABRICKS_8.exception(
                    cause=e,
                    host=host,
                )
            # COPY INTO failure -- best-effort table identification
            # via dest.tables.
            raise DatabricksErrorCode.DATABRICKS_9.exception(
                cause=e,
                table=", ".join(write_cfg.target_tables),
            )


class _DatabricksExplorer(Explorer[DatabricksConn]):
    def metadata(self, conn: DatabricksConn) -> Any:
        # TODO
        pass

    def data(self, conn: DatabricksConn, query: str) -> Any:
        # TODO
        pass


DATABRICKS_SRC = SrcDef(
    conn=DatabricksConn,
    src_type="databricks",
    src_version="v1",
    src=DatabricksSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=_DatabricksSrcPlugin,
    explorer=_DatabricksExplorer,
)

DATABRICKS_DEST = DestDef(
    conn=DatabricksConn,
    dest_type="databricks",
    dest_version="v1",
    dest=DatabricksDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=_DatabricksDestPlugin,
    explorer=_DatabricksExplorer,
)

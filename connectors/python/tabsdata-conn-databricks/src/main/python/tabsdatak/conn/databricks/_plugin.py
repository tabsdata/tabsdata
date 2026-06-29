from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.databricks import (
    DatabricksDest,
    DatabricksDestConn,
    DatabricksSrc,
    DatabricksSrcConn,
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
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    TableFileInput,
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


def _resolve_initial_values(ctx: SrcPluginCtx, src: DatabricksSrc) -> dict | None:
    """Look up the active bind dict for this run.

    Walks ``src.initial_values`` (the user's first-run seed) bind
    name by bind name. For each, prefers ``ctx.get_attr(name)`` --
    that's where the previous run's persisted value lands -- and
    falls back to the seed when the ctx scratch has no entry for
    that name.

    Returns ``None`` when ``src.initial_values`` is None (no
    placeholders in any query), signalling no substitution is needed.

    Uses ``is not None`` rather than truthiness so a legitimately
    falsy value stored last run (``0``, ``False``, ``""``) isn't
    silently shadowed by the seed.
    """
    if src.initial_values is None:
        return None
    out: dict[str, Any] = {}
    for name, seed in src.initial_values.items():
        from_ctx = ctx.get_attr(name)
        out[name] = from_ctx if from_ctx is not None else seed
    return out


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


class _DatabricksSrcPlugin(SrcPlugin[DatabricksSrcConn, DatabricksSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: DatabricksSrcConn,
        src: DatabricksSrc,
    ) -> list[list[TableFileInput]]:
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks import _impl

        cfg = _resolve_src_cfg(src.src_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info("databricks: read_in starting (%d queries)", len(src.queries or []))
        # The databricks SDK's own logger is far noisier than the
        # caller's logger.setLevel here -- forward the same level
        # so the SDK doesn't fight us.
        logging.getLogger("databricks").setLevel(cfg.logging_level)

        read_cfg = _impl.DatabricksReadCfg(
            conn=conn,
            queries=list(src.queries),
            initial_values=_resolve_initial_values(ctx, src),
        )
        host = _convert(conn.host_url, str)
        try:
            parquet_paths = _impl.read_all_queries(read_cfg, ctx.work_dir)
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
        # Wrap each adapter-returned path as a ``TableFileInput``
        # (metadata defaults to ``None``).
        return [[TableFileInput(file=Path(p))] for p in parquet_paths]


class _DatabricksDestPlugin(DestPlugin[DatabricksDestConn, DatabricksDest]):
    def write_out(
        self,
        ctx: DestContext,
        conn: DatabricksDestConn,
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

        write_cfg = _impl.DatabricksWriteCfg(
            conn=conn,
            target_tables=list(dest.tables),
            if_table_exists=dest.if_table_exists,
            schema_evolution=dest.schema_evolution,
        )
        host = _convert(conn.host_url, str)
        try:
            # ``tables`` is ``list[TableFileSpec]`` =
            # ``list[Path]`` (a ``Path`` subclass), so each
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


class _DatabricksSrcExplorer(Explorer[DatabricksSrcConn]):
    def metadata(self, conn: DatabricksSrcConn) -> Any:
        # TODO
        pass

    def data(self, conn: DatabricksSrcConn, query: str) -> Any:
        # TODO
        pass


class _DatabricksDestExplorer(Explorer[DatabricksDestConn]):
    def metadata(self, conn: DatabricksDestConn) -> Any:
        # TODO
        pass

    def data(self, conn: DatabricksDestConn, query: str) -> Any:
        # TODO
        pass


DATABRICKS_SRC = SrcDef(
    conn=DatabricksSrcConn,
    src_type="databricks",
    src_version="v1",
    src=DatabricksSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=_DatabricksSrcPlugin,
    explorer=_DatabricksSrcExplorer,
)

DATABRICKS_DEST = DestDef(
    conn=DatabricksDestConn,
    dest_type="databricks",
    dest_version="v1",
    dest=DatabricksDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=_DatabricksDestPlugin,
    explorer=_DatabricksDestExplorer,
)

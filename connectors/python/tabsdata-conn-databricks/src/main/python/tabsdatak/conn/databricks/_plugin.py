#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import re
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
from tabsdatak.conn.databricks._explorer import DatabricksConnectionExplorer
from tabsdatak.conn.databricks.error import (
    DatabricksErrorCode,
    DatabricksValidateException,
)
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
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
_KEY_SRC_DROP_METADATA: SrcCfgKey = "tabsdata.src_metadata.drop"
_KEY_DEST_LOGGING_LEVEL: DestCfgKey = "databricks.logging_level"

_SRC_CFGS_DEFAULTS: dict[SrcCfgKey, Any] = {
    _KEY_SRC_LOGGING_LEVEL: logging.ERROR,
    _KEY_SRC_DROP_METADATA: False,
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
    drop_metadata: bool


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


_UNBOUND_PARAMETER = re.compile(
    r"unbound parameter[:\s]+`?([A-Za-z_]\w*)`?", re.IGNORECASE
)


def _unbound_bind(exc: BaseException) -> str | None:
    """What the query binds that ``initial_values`` did not provide, or ``None``
    when the failure is something else.

    Only the names in ``initial_values`` are sent as statement parameters, so a
    ``:name`` the seed omits reaches the warehouse unbound and the warehouse --
    not this adapter -- rejects the statement. The name therefore has to be read
    back out of its message; when the marker is there but the name is not, the
    diagnosis still holds, so report it without one.
    """
    text = str(exc)
    if "UNBOUND_SQL_PARAMETER" not in text:
        return None
    found = _UNBOUND_PARAMETER.search(text)
    return f"'{found.group(1)}'" if found else "a parameter it does not name"


class _DatabricksSrcPlugin(SrcPlugin[DatabricksSrcConn, DatabricksSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: DatabricksSrcConn,
        src: DatabricksSrc,
    ) -> list[list[TableFileInput]]:
        from tabsdatak.conn.common.sql._metadata import to_sql_table_file_inputs

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
            unbound = _unbound_bind(e)
            if unbound is not None:
                raise DatabricksErrorCode.DATABRICKS_12.exception(
                    cause=e,
                    parameter=unbound,
                )
            # Query/download failure; we don't know which slot
            # without cooperation from the adapter, so emit -1.
            raise DatabricksErrorCode.DATABRICKS_6.exception(
                cause=e,
                query_index=-1,
            )

        # One parquet per query; SrcDef.table_mode is SINGLE. Each
        # adapter-returned path becomes a ``TableFileInput`` carrying the
        # query and the bind values it ran with as metadata.
        return to_sql_table_file_inputs(
            [[Path(p)] for p in parquet_paths],
            read_cfg.queries,
            read_cfg.initial_values,
            drop_metadata=cfg.drop_metadata,
        )


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


DATABRICKS_SRC = SrcDef(
    conn=DatabricksSrcConn,
    type_="databricks-bulk-in",
    system="Databricks",
    src_version="v1",
    src=DatabricksSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=_DatabricksSrcPlugin,
    explorer=DatabricksConnectionExplorer,
    icon="conn/databricks/resources/databricks.svg",
)

DATABRICKS_DEST = DestDef(
    conn=DatabricksDestConn,
    type_="databricks-bulk-out",
    system="Databricks",
    dest_version="v1",
    dest=DatabricksDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=_DatabricksDestPlugin,
    explorer=DatabricksConnectionExplorer,
    icon="conn/databricks/resources/databricks.svg",
)

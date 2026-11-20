#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

import polars as pl

# noinspection PyProtectedMember
from tabsdatak.conn.common._dest import present

# noinspection PyProtectedMember
from tabsdatak.conn.common._schemas import SlotSchemas, typed
from tabsdatak.conn.common.error import ConnException

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._engine.initial_values import resolve_initial_values
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
    DatabricksRuntimeException,
    DatabricksValidateException,
)
from tabsdatak.spi import (
    DestContext,
    DestDef,
    DestPlugin,
    FrameMetadata,
    LazyFrameInput,
    MetadataSpec,
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
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


def _inputs(
    tables: Iterable[Any],
    index: int,
    metadata: FrameMetadata | None,
    host: str,
    schemas: SlotSchemas,
) -> Iterator[LazyFrameInput]:
    try:
        rows = 0
        columns: pl.DataFrame | None = None
        for table in tables:
            frame = pl.DataFrame(table)
            if frame.height == 0:
                if columns is None:
                    columns = frame
                continue
            rows += frame.height
            yield schemas.served(index, LazyFrameInput(frame.lazy(), metadata))
        if rows == 0:
            empty = (
                schemas.served(index, LazyFrameInput(columns.lazy(), metadata))
                if columns is not None and typed(columns.schema)
                else schemas.empty(index, metadata)
            )
            if empty is not None:
                yield empty
    except (DatabricksValidateException, DatabricksRuntimeException):
        raise
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
        raise DatabricksErrorCode.DATABRICKS_6.exception(
            cause=e,
            query_index=index,
        )


class _DatabricksSrcPlugin(SrcPlugin[DatabricksSrcConn, DatabricksSrc]):
    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: DatabricksSrcConn,
        src: DatabricksSrc,
    ) -> list[Iterable[LazyFrameInput]]:
        from tabsdatak.conn.common.sql._metadata import sql_metadata

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
            initial_values=resolve_initial_values(ctx, src.initial_values),
        )
        host = _convert(conn.host_url, str)
        schemas = SlotSchemas.of(ctx)
        inputs: list[Iterable[LazyFrameInput]] = []
        for index, (query, tables) in enumerate(
            zip(read_cfg.queries, _impl.read_all_queries(read_cfg), strict=True)
        ):
            metadata = None
            if not cfg.drop_metadata:
                columns: MetadataSpec = {**sql_metadata(query, read_cfg.initial_values)}
                metadata = FrameMetadata(columns=columns)
            inputs.append(_inputs(tables, index, metadata, host, schemas))
        return inputs


class _DatabricksDestPlugin(
    DestPlugin[DatabricksDestConn, DatabricksDest, list[pl.LazyFrame | None]],
):
    def write_out(
        self,
        ctx: DestContext,
        conn: DatabricksDestConn,
        dest: DatabricksDest,
        tables: list[pl.LazyFrame | None],
    ) -> None:
        # noinspection PyProtectedMember
        from tabsdatak.conn.databricks import _impl

        cfg = _resolve_dest_cfg(dest.dest_cfg)
        logger.setLevel(cfg.logging_level)
        logger.info(
            "databricks: write_out starting (%d table(s))", len(dest.tables or [])
        )
        logging.getLogger("databricks").setLevel(cfg.logging_level)

        # Verifies that the number of incoming tables matches
        # the destination's declared ``tables`` (DATABRICKS_7).
        if len(dest.tables) != len(tables):
            raise DatabricksErrorCode.DATABRICKS_7.exception(
                tables=len(dest.tables),
                slots=len(tables),
            )

        merge_cfgs = dest._merge_cfgs()
        if merge_cfgs is None:
            frames, target_tables = present(tables, dest.tables)
        else:
            frames, target_tables, merge_cfgs = present(tables, dest.tables, merge_cfgs)
        if not frames:
            logger.info("databricks: no table to write; nothing to do")
            return

        write_cfg = _impl.DatabricksWriteCfg(
            conn=conn,
            target_tables=target_tables,
            if_table_exists=dest.if_table_exists,
            schema_evolution=dest._evolution().schema_evolution,
            ctx=ctx,
            watermark_column=dest._watermark_column(),
            merge_cfgs=merge_cfgs,
        )
        host = _convert(conn.host_url, str)
        try:
            _impl.write_all_tables(write_cfg, frames)
        except ConnException:
            raise
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

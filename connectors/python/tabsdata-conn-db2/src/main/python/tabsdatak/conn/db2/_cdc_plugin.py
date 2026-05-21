"""Db2 CDC plugin glue.

Mirrors the layering of the batch side (``_plugin.py`` / ``_impl.py``):
the plugin lives here, the dialect / ingester / offset machinery
sits one layer below in ``_cdc_impl.py``.

* :class:`Db2CdcStreamSrcPlugin` -- wires :class:`Db2CdcStreamSrc`
  to the dialect-agnostic ``run_cdc`` loop.
* :data:`DB2_CDC_STREAM_SRC` -- entry-point ``StreamSrcDef``.
* ``_to_internal_start_from`` -- maps the db2-public ``start_from``
  shapes onto the runner-internal representation (the db2-local
  :class:`TimestampPosition` is mapped onto the common one;
  :class:`CommitSeqPosition` / :class:`TableCommitSeqPosition`
  pass through).
* ``_build_cdc_run_config`` -- translates ``src_cfg`` into
  ``CdcRunConfig``, filling in common-sql defaults for missing keys.
"""

from __future__ import annotations

import logging
from typing import Any, cast

from tabsdatak.conn.common.sql.cdc._runner import CdcRunConfig, run_cdc
from tabsdatak.conn.common.sql.cdc.constants import (
    DEFAULT_BLOCKING_TIMEOUT_SECONDS,
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECS,
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_TRIGGER_MAX_BYTES,
    DEFAULT_TRIGGER_MAX_ROWS,
    DEFAULT_TRIGGER_MAX_SECS,
)
from tabsdatak.conn.common.sql.cdc.typing import (
    CdcFormat as _CommonCdcFormat,
    TimestampPosition as _CommonTimestampPosition,
)
from tabsdatak.conn.db2 import (
    CdcSrcCfgKey,
    Db2CdcConn,
    Db2CdcStartFromSpec,
    Db2CdcStreamSrc,
    TimestampPosition,
)

# noinspection PyProtectedMember
from tabsdatak.conn.db2._cdc_impl import DEFAULT_FETCH_SIZE, SqlReplicaIngester
from tabsdatak.spi import (
    Explorer,
    StreamSrcDef,
    StreamSrcPlugin,
    StreamSrcPluginCtx,
)

logger = logging.getLogger(__name__)


# Allowed CDC src_cfg keys -- the Literal annotation gates typos at
# schema-construction time. ``_build_cdc_run_config`` reads from
# src_cfg and falls back to common-sql defaults.
_KEY_CDC_BLOCKING_TIMEOUT: CdcSrcCfgKey = "db2.cdc.blocking_timeout_seconds"
_KEY_CDC_FETCH_SIZE: CdcSrcCfgKey = "db2.cdc.fetch_size"
_KEY_CDC_LOGGING_LEVEL: CdcSrcCfgKey = "db2.cdc.logging_level"
_KEY_CDC_BUFFER_MAX_ROWS: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_rows"
_KEY_CDC_BUFFER_MAX_BYTES: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_bytes"
_KEY_CDC_BUFFER_MAX_SECS: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_secs"
_KEY_CDC_TRIGGER_MAX_ROWS: CdcSrcCfgKey = "tabsdata.cdc.trigger.max_rows"
_KEY_CDC_TRIGGER_MAX_BYTES: CdcSrcCfgKey = "tabsdata.cdc.trigger.max_bytes"
_KEY_CDC_TRIGGER_MAX_SECS: CdcSrcCfgKey = "tabsdata.cdc.trigger.max_secs"
_KEY_CDC_POLL_INTERVAL: CdcSrcCfgKey = "tabsdata.cdc.poll_interval_seconds"


def _to_internal_start_from(spec: Db2CdcStartFromSpec):
    """Translate the user-facing ``start_from`` into the shape the
    ingester / runner consume. ``CommitSeqPosition`` and
    ``TableCommitSeqPosition`` stay db2-local (the ingester is
    db2-only); the db2-local :class:`TimestampPosition` is mapped
    onto the common one so the ingester's match arms (which use
    the common type) fire."""
    if isinstance(spec, TimestampPosition):
        return _CommonTimestampPosition(ts=spec.ts)
    return spec


def _build_cdc_run_config(
    src: Db2CdcStreamSrc,
) -> CdcRunConfig:
    """Translate the user-facing ``src_cfg`` mapping into a runner
    ``CdcRunConfig``. Missing keys fall back to common-sql defaults."""
    cfg = dict(src.src_cfg or {})
    return CdcRunConfig(
        cdc_format=_CommonCdcFormat(
            columns_format=src.columns_format,
            flatten_columns=src.flatten_columns,
        ),
        buffer_max_rows=cfg.get(_KEY_CDC_BUFFER_MAX_ROWS, DEFAULT_BUFFER_MAX_ROWS),
        buffer_max_bytes=cfg.get(_KEY_CDC_BUFFER_MAX_BYTES, DEFAULT_BUFFER_MAX_BYTES),
        buffer_max_secs=cfg.get(_KEY_CDC_BUFFER_MAX_SECS, DEFAULT_BUFFER_MAX_SECS),
        trigger_max_rows=cfg.get(_KEY_CDC_TRIGGER_MAX_ROWS, DEFAULT_TRIGGER_MAX_ROWS),
        trigger_max_bytes=cfg.get(_KEY_CDC_TRIGGER_MAX_BYTES, DEFAULT_TRIGGER_MAX_BYTES),
        trigger_max_secs=cfg.get(_KEY_CDC_TRIGGER_MAX_SECS, DEFAULT_TRIGGER_MAX_SECS),
        poll_interval_seconds=cfg.get(
            _KEY_CDC_POLL_INTERVAL, DEFAULT_POLL_INTERVAL_SECONDS
        ),
        logging_level=int(cfg.get(_KEY_CDC_LOGGING_LEVEL, logging.ERROR)),
    )


class Db2CdcStreamSrcPlugin(StreamSrcPlugin[Db2CdcStreamSrc]):
    """Wires a :class:`Db2CdcStreamSrc` to the dialect-agnostic
    ``run_cdc`` loop."""

    def run(
        self,
        ctx: StreamSrcPluginCtx,
        stream_src: Db2CdcStreamSrc,
    ):
        conn = cast(Db2CdcConn, stream_src.conn)
        cfg = _build_cdc_run_config(stream_src)
        # Sync this connector's logger level with the runner +
        # sentinels (``run_cdc`` handles the common-sql namespace).
        logger.setLevel(cfg.logging_level)
        logger.info(
            "db2-cdc: starting (%d CD table(s), start_from=%r)",
            len(stream_src.tables or []),
            stream_src.start_from,
        )
        src_cfg = stream_src.src_cfg or {}
        blocking_timeout = src_cfg.get(
            _KEY_CDC_BLOCKING_TIMEOUT, DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        )
        fetch_size = src_cfg.get(_KEY_CDC_FETCH_SIZE, DEFAULT_FETCH_SIZE)
        ingester = SqlReplicaIngester(
            conn=conn,
            tables=list(stream_src.tables),
            start_from=_to_internal_start_from(stream_src.start_from),
            blocking_timeout_seconds=blocking_timeout,
            fetch_size=fetch_size,
        )
        try:
            run_cdc(ctx=ctx, ingester=ingester, cfg=cfg)
        finally:
            logger.info("db2-cdc: stopped")


class Db2CdcExplorer(Explorer[Db2CdcConn]):
    def metadata(self, conn: Db2CdcConn) -> Any:
        # TODO
        pass

    def data(self, conn: Db2CdcConn, query: str) -> Any:
        # TODO
        pass


DB2_CDC_STREAM_SRC = StreamSrcDef(
    conn=Db2CdcConn,
    src_type="db2-cdc",
    src_version="v1",
    src=Db2CdcStreamSrc,
    cardinality=lambda src: len(src.tables),
    uses_manifest=True,
    plugin=Db2CdcStreamSrcPlugin,
    explorer=Db2CdcExplorer,
)

"""MySQL CDC plugin glue.

Mirrors the layering of the batch side (``_plugin.py`` / ``_impl.py``):
the plugin lives here, the dialect / ingester / offset machinery
sits one layer below in ``_cdc_impl.py``.

* :class:`MySQLCdcStreamSrcPlugin` -- wires :class:`MySQLCdcStreamSrc`
  to the dialect-agnostic ``run_cdc`` loop. Builds a
  :class:`BinlogIngester` from the source's connection +
  ``start_from`` + ``server_id`` and runs until the trx context
  goes inactive or a signal arrives.
* :data:`MYSQL_CDC_STREAM_SRC` -- entry-point ``StreamSrcDef``.
* ``_to_internal_start_from`` -- maps the mysql-public ``start_from``
  shapes onto the runner-internal representation (in particular,
  the mysql-local ``TimestampPosition`` is mapped onto the common
  one consumed by the ingester's pattern-matching).
* ``_build_cdc_run_config`` -- translates ``src_cfg`` into
  ``CdcRunConfig``, filling in common-sql defaults for missing keys.
"""

from __future__ import annotations

import logging
from typing import cast

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
from tabsdatak.conn.mysql import (
    CdcSrcCfgKey,
    MySQLCdcConn,
    MySQLCdcStartFromSpec,
    MySQLCdcStreamSrc,
    TimestampPosition,
)

# noinspection PyProtectedMember
from tabsdatak.conn.mysql._cdc_impl import BinlogIngester
from tabsdatak.spi import (
    StreamSrcDef,
    StreamSrcPlugin,
    StreamSrcPluginCtx,
)

logger = logging.getLogger(__name__)


# Allowed CDC src_cfg keys -- the Literal annotation gates typos at
# schema-construction time. ``_resolve_*`` reads from src_cfg and
# falls back to common-sql defaults.
_KEY_CDC_BLOCKING_TIMEOUT: CdcSrcCfgKey = "mysql.cdc.blocking_timeout_seconds"
_KEY_CDC_BUFFER_MAX_ROWS: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_rows"
_KEY_CDC_BUFFER_MAX_BYTES: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_bytes"
_KEY_CDC_BUFFER_MAX_SECS: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_secs"
_KEY_CDC_TRIGGER_MAX_ROWS: CdcSrcCfgKey = "tabsdata.cdc.trigger.max_rows"
_KEY_CDC_TRIGGER_MAX_BYTES: CdcSrcCfgKey = "tabsdata.cdc.trigger.max_bytes"
_KEY_CDC_TRIGGER_MAX_SECS: CdcSrcCfgKey = "tabsdata.cdc.trigger.max_secs"
_KEY_CDC_POLL_INTERVAL: CdcSrcCfgKey = "tabsdata.cdc.poll_interval_seconds"


def _to_internal_start_from(spec: MySQLCdcStartFromSpec):
    """Translate the user-facing ``start_from`` into the shape the
    ingester / runner consume. ``GtidPosition`` and ``BinlogPosition``
    stay mysql-local (the ingester is mysql-only); the mysql-local
    :class:`TimestampPosition` is mapped onto the common one so the
    ingester's match arms (which use the common type) fire."""
    if isinstance(spec, TimestampPosition):
        return _CommonTimestampPosition(ts=spec.ts)
    return spec


def _build_cdc_run_config(
    src: MySQLCdcStreamSrc,
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
    )


class MySQLCdcStreamSrcPlugin(StreamSrcPlugin[MySQLCdcStreamSrc]):
    """Wires a :class:`MySQLCdcStreamSrc` to the dialect-agnostic
    ``run_cdc`` loop."""

    def run(
        self,
        ctx: StreamSrcPluginCtx,
        stream_src: MySQLCdcStreamSrc,
    ):
        conn = cast(MySQLCdcConn, stream_src.conn)
        cfg = _build_cdc_run_config(stream_src)
        blocking_timeout = (
            (stream_src.src_cfg or {}).get(
                _KEY_CDC_BLOCKING_TIMEOUT, DEFAULT_BLOCKING_TIMEOUT_SECONDS
            )
        )
        ingester = BinlogIngester(
            conn=conn,
            tables=list(stream_src.tables),
            start_from=_to_internal_start_from(stream_src.start_from),
            server_id=conn.server_id,
            blocking_timeout_seconds=blocking_timeout,
        )
        run_cdc(ctx=ctx, ingester=ingester, cfg=cfg)


MYSQL_CDC_STREAM_SRC = StreamSrcDef(
    conn=MySQLCdcConn,
    src_type="mysql-cdc",
    src_version="v1",
    src=MySQLCdcStreamSrc,
    cardinality=lambda src: len(src.tables),
    uses_manifest=True,
    plugin=MySQLCdcStreamSrcPlugin,
    explorer=None,
)

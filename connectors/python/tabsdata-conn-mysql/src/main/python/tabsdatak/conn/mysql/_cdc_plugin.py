#
# Copyright 2026 Tabsdata Inc.
#

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
from typing import TYPE_CHECKING

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream._floor import decode_seconds, manifest_bounds

# noinspection PyProtectedMember
from tabsdatak.conn.common._stream.constants import (
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECS,
    DEFAULT_POLL_IDLE_SECS,
    DEFAULT_POLL_MAX_SECS,
    DEFAULT_TRIGGER_MAX_BYTES,
    DEFAULT_TRIGGER_MAX_ROWS,
    DEFAULT_TRIGGER_MAX_SECS,
)
from tabsdatak.conn.common.sql.cdc._runner import CdcPlugin, CdcRunConfig
from tabsdatak.conn.common.sql.cdc._stage_plugin import CdcStageSrc
from tabsdatak.conn.common.sql.cdc.constants import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_FETCH_SIZE,
    DEFAULT_MAX_LARGE_TEXT_LENGTH,
)
from tabsdatak.conn.common.sql.cdc.typing import (
    CdcFormat as _CommonCdcFormat,
    TimestampPosition as _CommonTimestampPosition,
)
from tabsdatak.conn.mysql import (
    CdcSrcCfgKey,
    MySQLCdcStartFromSpec,
    MySQLCdcStreamSrc,
    MySQLCdcStreamSrcConn,
    TimestampPosition,
)

# noinspection PyProtectedMember
from tabsdatak.conn.mysql._explorer import MySQLCdcConnectionExplorer
from tabsdatak.spi import StreamSrcDef

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.mysql._cdc_impl import BinlogIngester

# Allowed CDC src_cfg keys -- the Literal annotation gates typos at
# schema-construction time. ``_resolve_*`` reads from src_cfg and
# falls back to common-sql defaults.
_KEY_CDC_POLL_MAX_SECS: CdcSrcCfgKey = "tabsdata.cdc.poll.max_secs"
_KEY_CDC_LOGGING_LEVEL: CdcSrcCfgKey = "mysql.cdc.logging_level"
_KEY_CDC_BUFFER_MAX_ROWS: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_rows"
_KEY_CDC_BUFFER_MAX_BYTES: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_bytes"
_KEY_CDC_BUFFER_MAX_SECS: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_secs"
_KEY_CDC_TRIGGER_MAX_ROWS: CdcSrcCfgKey = "tabsdata.cdc.trigger.max_rows"
_KEY_CDC_TRIGGER_MAX_BYTES: CdcSrcCfgKey = "tabsdata.cdc.trigger.max_bytes"
_KEY_CDC_TRIGGER_MAX_SECS: CdcSrcCfgKey = "tabsdata.cdc.trigger.max_secs"
_KEY_CDC_MANIFEST_MIN_SECS: CdcSrcCfgKey = "tabsdata.cdc.manifest.min_secs"
_KEY_CDC_MANIFEST_MAX_SECS: CdcSrcCfgKey = "tabsdata.cdc.manifest.max_secs"
_KEY_CDC_POLL_IDLE_SECS: CdcSrcCfgKey = "tabsdata.cdc.poll.idle_secs"
_KEY_CDC_MAX_LARGE_TEXT_LENGTH: CdcSrcCfgKey = "tabsdata.cdc.max_large_text_length"
_KEY_CDC_FETCH_SIZE: CdcSrcCfgKey = "tabsdata.cdc.fetch_size"
_KEY_CDC_CHUNK_SIZE: CdcSrcCfgKey = "tabsdata.cdc.chunk_size"


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
    manifest_min_secs, manifest_max_secs = manifest_bounds(
        cfg,
        min_key=_KEY_CDC_MANIFEST_MIN_SECS,
        max_key=_KEY_CDC_MANIFEST_MAX_SECS,
        trigger_key=_KEY_CDC_TRIGGER_MAX_SECS,
    )
    return CdcRunConfig(
        cdc_format=_CommonCdcFormat(
            columns_format=src.columns_format,
            flatten_columns=src.flatten_columns,
        ),
        buffer_max_rows=cfg.get(_KEY_CDC_BUFFER_MAX_ROWS, DEFAULT_BUFFER_MAX_ROWS),
        buffer_max_bytes=cfg.get(_KEY_CDC_BUFFER_MAX_BYTES, DEFAULT_BUFFER_MAX_BYTES),
        buffer_max_secs=cfg.get(_KEY_CDC_BUFFER_MAX_SECS, DEFAULT_BUFFER_MAX_SECS),
        trigger_max_rows=cfg.get(_KEY_CDC_TRIGGER_MAX_ROWS, DEFAULT_TRIGGER_MAX_ROWS),
        trigger_max_bytes=cfg.get(
            _KEY_CDC_TRIGGER_MAX_BYTES, DEFAULT_TRIGGER_MAX_BYTES
        ),
        manifest_min_secs=manifest_min_secs,
        manifest_max_secs=manifest_max_secs,
        trigger_max_secs=decode_seconds(
            _KEY_CDC_TRIGGER_MAX_SECS,
            cfg.get(_KEY_CDC_TRIGGER_MAX_SECS, DEFAULT_TRIGGER_MAX_SECS),
        ),
        poll_idle_secs=cfg.get(_KEY_CDC_POLL_IDLE_SECS, DEFAULT_POLL_IDLE_SECS),
        poll_max_secs=cfg.get(_KEY_CDC_POLL_MAX_SECS, DEFAULT_POLL_MAX_SECS),
        max_large_text_length=int(
            cfg.get(_KEY_CDC_MAX_LARGE_TEXT_LENGTH, DEFAULT_MAX_LARGE_TEXT_LENGTH)
        ),
        initial_load=src.initial_load,
        fetch_size=int(cfg.get(_KEY_CDC_FETCH_SIZE, DEFAULT_FETCH_SIZE)),
        chunk_size=int(cfg.get(_KEY_CDC_CHUNK_SIZE, DEFAULT_CHUNK_SIZE)),
        logging_level=int(cfg.get(_KEY_CDC_LOGGING_LEVEL, logging.ERROR)),
    )


class MySQLCdcStreamSrcPlugin(CdcPlugin[MySQLCdcStreamSrcConn, MySQLCdcStreamSrc]):
    """Drives a :class:`MySQLCdcStreamSrc` via the dialect-agnostic
    :class:`~tabsdatak.conn.common.sql.cdc._runner.CdcPlugin` loop, backed
    by a :class:`~tabsdatak.conn.mysql._cdc_impl.BinlogIngester`."""

    def build_run_config(
        self,
        stream_src: MySQLCdcStreamSrc,
    ) -> CdcRunConfig:
        return _build_cdc_run_config(stream_src)

    def build_ingester(
        self,
        conn: MySQLCdcStreamSrcConn,
        stream_src: MySQLCdcStreamSrc,
    ) -> BinlogIngester:
        # noinspection PyProtectedMember
        from tabsdatak.conn.mysql._cdc_impl import BinlogIngester

        poll_max_secs = (stream_src.src_cfg or {}).get(
            _KEY_CDC_POLL_MAX_SECS, DEFAULT_POLL_MAX_SECS
        )
        return BinlogIngester(
            conn=conn,
            tables=list(stream_src.tables),
            start_from=_to_internal_start_from(stream_src.start_from),
            server_id=conn.server_id,
            poll_max_secs=poll_max_secs,
        )


MYSQL_CDC_STREAM_SRC = StreamSrcDef(
    conn=MySQLCdcStreamSrcConn,
    type_="mysql-cdc",
    system="MySQL",
    src_version="v1",
    src=MySQLCdcStreamSrc,
    cardinality=lambda src: len(src.tables),
    plugin=MySQLCdcStreamSrcPlugin,
    publisher_src=CdcStageSrc,
    explorer=MySQLCdcConnectionExplorer,
    icon="conn/mysql/resources/mysql-cdc.svg",
)

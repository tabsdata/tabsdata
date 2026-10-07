#
# Copyright 2026 Tabsdata Inc.
#

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
    DEFAULT_INITIAL_LOAD_MAX_WINDOW_CHANGES,
    DEFAULT_MAX_LARGE_TEXT_LENGTH,
)
from tabsdatak.conn.common.sql.cdc.typing import (
    CdcFormat as _CommonCdcFormat,
    TimestampPosition as _CommonTimestampPosition,
)
from tabsdatak.conn.db2 import (
    CdcSrcCfgKey,
    Db2CdcStartFromSpec,
    Db2CdcStreamSrc,
    Db2CdcStreamSrcConn,
    TimestampPosition,
)

# noinspection PyProtectedMember
from tabsdatak.conn.db2._explorer import Db2CdcConnectionExplorer
from tabsdatak.spi import StreamSrcDef

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.db2._cdc_impl import SqlReplicaIngester

# Allowed CDC src_cfg keys -- the Literal annotation gates typos at
# schema-construction time. ``_build_cdc_run_config`` reads from
# src_cfg and falls back to common-sql defaults.
_KEY_CDC_POLL_MAX_SECS: CdcSrcCfgKey = "tabsdata.cdc.poll.max_secs"
_KEY_CDC_FETCH_SIZE: CdcSrcCfgKey = "tabsdata.cdc.fetch_size"
_KEY_CDC_CHUNK_SIZE: CdcSrcCfgKey = "tabsdata.cdc.chunk_size"
_KEY_CDC_LOGGING_LEVEL: CdcSrcCfgKey = "db2.cdc.logging_level"
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
_KEY_CDC_INITIAL_LOAD_MAX_WINDOW_CHANGES: CdcSrcCfgKey = (
    "tabsdata.cdc.initial_load.max_window_changes"
)


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
        initial_load_max_window_changes=int(
            cfg.get(
                _KEY_CDC_INITIAL_LOAD_MAX_WINDOW_CHANGES,
                DEFAULT_INITIAL_LOAD_MAX_WINDOW_CHANGES,
            )
        ),
        logging_level=int(cfg.get(_KEY_CDC_LOGGING_LEVEL, logging.ERROR)),
    )


class Db2CdcStreamSrcPlugin(CdcPlugin[Db2CdcStreamSrcConn, Db2CdcStreamSrc]):
    """Drives a :class:`Db2CdcStreamSrc` via the dialect-agnostic
    :class:`~tabsdatak.conn.common.sql.cdc._runner.CdcPlugin` loop, backed
    by a :class:`~tabsdatak.conn.db2._cdc_impl.SqlReplicaIngester`."""

    def build_run_config(
        self,
        stream_src: Db2CdcStreamSrc,
    ) -> CdcRunConfig:
        return _build_cdc_run_config(stream_src)

    def build_ingester(
        self,
        conn: Db2CdcStreamSrcConn,
        stream_src: Db2CdcStreamSrc,
    ) -> SqlReplicaIngester:
        # noinspection PyProtectedMember
        from tabsdatak.conn.db2._cdc_impl import SqlReplicaIngester

        src_cfg = stream_src.src_cfg or {}
        poll_max_secs = src_cfg.get(
            _KEY_CDC_POLL_MAX_SECS,
            DEFAULT_POLL_MAX_SECS,
        )
        fetch_size = src_cfg.get(_KEY_CDC_FETCH_SIZE, DEFAULT_FETCH_SIZE)
        return SqlReplicaIngester(
            conn=conn,
            tables=list(stream_src.tables),
            start_from=_to_internal_start_from(stream_src.start_from),
            poll_max_secs=poll_max_secs,
            fetch_size=fetch_size,
        )


DB2_CDC_STREAM_SRC = StreamSrcDef(
    conn=Db2CdcStreamSrcConn,
    type_="db2-cdc",
    system="IBM Db2",
    src_version="v1",
    src=Db2CdcStreamSrc,
    cardinality=lambda src: len(src.tables),
    plugin=Db2CdcStreamSrcPlugin,
    publisher_src=CdcStageSrc,
    explorer=Db2CdcConnectionExplorer,
    icon="conn/db2/resources/db2-cdc.svg",
)

#
# Copyright 2026 Tabsdata Inc.
#

"""Postgres CDC plugin glue.

Mirrors the layering of the batch side (``_plugin.py`` / ``_impl.py``):
the plugin lives here, the dialect / ingester / offset machinery
sits one layer below in ``_cdc_impl.py``.

* :class:`PostgresCdcStreamSrcPlugin` -- wires
  :class:`PostgresCdcStreamSrc` to the dialect-agnostic ``run_cdc``
  loop.
* :data:`POSTGRES_CDC_STREAM_SRC` -- entry-point ``StreamSrcDef``.
* ``_to_internal_start_from`` -- maps the postgres-public
  ``start_from`` shapes onto the runner-internal representation
  (``LsnPosition`` passes through, ``"head"`` / ``"tail"`` are
  forwarded as-is and resolved against the live database in
  ``Wal2JsonIngester.init_stream``).
* ``_build_cdc_run_config`` -- translates ``src_cfg`` into
  ``CdcRunConfig``, filling in common-sql defaults for missing keys.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

# noinspection PyProtectedMember
# noinspection PyProtectedMember
from tabsdatak.api import BufferThresholds, PollThresholds
from tabsdatak.conn.common._stream.constants import (
    DEFAULT_BUFFER_MAX_BYTES,
    DEFAULT_BUFFER_MAX_ROWS,
    DEFAULT_BUFFER_MAX_SECS,
    DEFAULT_POLL_IDLE_SECS,
    DEFAULT_POLL_MAX_SECS,
)
from tabsdatak.conn.common.sql.cdc._runner import CdcPlugin, CdcRunConfig
from tabsdatak.conn.common.sql.cdc._stage_plugin import CdcStageSrc
from tabsdatak.conn.common.sql.cdc.constants import (
    DEFAULT_CHUNK_SIZE,
    DEFAULT_FETCH_SIZE,
    DEFAULT_MAX_LARGE_TEXT_LENGTH,
)
from tabsdatak.conn.common.sql.cdc.typing import (
    FULL_START,
    CdcFormat as _CommonCdcFormat,
)
from tabsdatak.conn.postgres import (
    CdcSrcCfgKey,
    PostgresCdcStartFromSpec,
    PostgresCdcStreamSrc,
    PostgresCdcStreamSrcConn,
)

# noinspection PyProtectedMember
from tabsdatak.conn.postgres._explorer import PostgresCdcConnectionExplorer
from tabsdatak.spi import StreamSrcDef

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from tabsdatak.conn.postgres._cdc_impl import Wal2JsonIngester

# Allowed CDC src_cfg keys -- the Literal annotation gates typos at
# schema-construction time. ``_build_cdc_run_config`` reads from
# src_cfg and falls back to common-sql defaults.
_KEY_CDC_POLL_MAX_SECS: CdcSrcCfgKey = "tabsdata.cdc.poll.max_secs"
_KEY_CDC_LOGGING_LEVEL: CdcSrcCfgKey = "postgres.cdc.logging_level"
_KEY_CDC_WAL2JSON_FORMAT_VERSION: CdcSrcCfgKey = "postgres.cdc.wal2json_format_version"
_KEY_CDC_UNCHANGED_LARGE_TEXT_AS_NULL: CdcSrcCfgKey = (
    "postgres.cdc.unchanged_large_text_as_null"
)
_KEY_CDC_ACCEPT_GAP: CdcSrcCfgKey = "postgres.cdc.accept_gap"
_KEY_CDC_BUFFER_MAX_ROWS: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_rows"
_KEY_CDC_BUFFER_MAX_BYTES: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_bytes"
_KEY_CDC_BUFFER_MAX_SECS: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_secs"
_KEY_CDC_POLL_IDLE_SECS: CdcSrcCfgKey = "tabsdata.cdc.poll.idle_secs"
_KEY_CDC_MAX_LARGE_TEXT_LENGTH: CdcSrcCfgKey = "tabsdata.cdc.max_large_text_length"
_KEY_CDC_FETCH_SIZE: CdcSrcCfgKey = "tabsdata.cdc.fetch_size"
_KEY_CDC_CHUNK_SIZE: CdcSrcCfgKey = "tabsdata.cdc.chunk_size"


def _to_internal_start_from(spec: PostgresCdcStartFromSpec):
    """Translate the user-facing ``start_from`` into the shape the
    ingester / runner consume. Postgres has no public types that
    are distinct from the internal ones (no ``TimestampPosition``,
    no driver-specific position): ``LsnPosition`` / ``"head"`` /
    ``"tail"`` all pass through as-is. ``"full"`` gives no position: the
    initial load fixes where change capture starts. The function exists for
    symmetry with the mysql plugin so future divergence (e.g.
    a wall-clock position) has a single conversion site."""
    if spec == FULL_START:
        return None
    return spec


def _build_cdc_run_config(
    src: PostgresCdcStreamSrc,
) -> CdcRunConfig:
    """Translate the user-facing ``src_cfg`` mapping into a runner
    ``CdcRunConfig``. Missing keys fall back to common-sql defaults."""
    cfg = dict(src.src_cfg or {})
    return CdcRunConfig(
        cdc_format=_CommonCdcFormat(
            columns_format=src.columns_format,
            flatten_columns=src.flatten_columns,
        ),
        buffer=BufferThresholds(
            max_rows=cfg.get(_KEY_CDC_BUFFER_MAX_ROWS, DEFAULT_BUFFER_MAX_ROWS),
            max_bytes=cfg.get(_KEY_CDC_BUFFER_MAX_BYTES, DEFAULT_BUFFER_MAX_BYTES),
            max_secs=cfg.get(_KEY_CDC_BUFFER_MAX_SECS, DEFAULT_BUFFER_MAX_SECS),
        ),
        poll=PollThresholds(
            max_secs=cfg.get(_KEY_CDC_POLL_MAX_SECS, DEFAULT_POLL_MAX_SECS),
            idle_secs=cfg.get(_KEY_CDC_POLL_IDLE_SECS, DEFAULT_POLL_IDLE_SECS),
        ),
        max_large_text_length=int(
            cfg.get(_KEY_CDC_MAX_LARGE_TEXT_LENGTH, DEFAULT_MAX_LARGE_TEXT_LENGTH)
        ),
        initial_load=src.start_from == FULL_START,
        fetch_size=int(cfg.get(_KEY_CDC_FETCH_SIZE, DEFAULT_FETCH_SIZE)),
        chunk_size=int(cfg.get(_KEY_CDC_CHUNK_SIZE, DEFAULT_CHUNK_SIZE)),
        logging_level=int(cfg.get(_KEY_CDC_LOGGING_LEVEL, logging.ERROR)),
    )


class PostgresCdcStreamSrcPlugin(
    CdcPlugin[PostgresCdcStreamSrcConn, PostgresCdcStreamSrc]
):
    """Drives a :class:`PostgresCdcStreamSrc` via the dialect-agnostic
    :class:`~tabsdatak.conn.common.sql.cdc._runner.CdcPlugin` loop, backed
    by a :class:`~tabsdatak.conn.postgres._cdc_impl.Wal2JsonIngester`."""

    def build_run_config(
        self,
        stream_src: PostgresCdcStreamSrc,
    ) -> CdcRunConfig:
        return _build_cdc_run_config(stream_src)

    def build_ingester(
        self,
        conn: PostgresCdcStreamSrcConn,
        stream_src: PostgresCdcStreamSrc,
    ) -> Wal2JsonIngester:
        # noinspection PyProtectedMember
        from tabsdatak.conn.postgres._cdc_impl import (
            DEFAULT_WAL2JSON_FORMAT_VERSION,
            Wal2JsonIngester,
            accepted_gap,
        )

        src_cfg = stream_src.src_cfg or {}
        poll_max_secs = src_cfg.get(
            _KEY_CDC_POLL_MAX_SECS,
            DEFAULT_POLL_MAX_SECS,
        )
        wal2json_format = src_cfg.get(
            _KEY_CDC_WAL2JSON_FORMAT_VERSION,
            DEFAULT_WAL2JSON_FORMAT_VERSION,
        )
        unchanged_large_text_as_null = bool(
            src_cfg.get(_KEY_CDC_UNCHANGED_LARGE_TEXT_AS_NULL, False)
        )
        return Wal2JsonIngester(
            conn=conn,
            tables=list(stream_src.tables),
            start_from=_to_internal_start_from(stream_src.start_from),
            replication_slot=stream_src.replication_slot,
            replication_slot_behavior=stream_src.replication_slot_behavior,
            poll_max_secs=poll_max_secs,
            publication_name=stream_src.publication_name,
            wal2json_format_version=int(wal2json_format),
            unchanged_large_text_as_null=unchanged_large_text_as_null,
            accepted_gap=accepted_gap(src_cfg.get(_KEY_CDC_ACCEPT_GAP)),
        )


POSTGRES_CDC_STREAM_SRC = StreamSrcDef(
    conn=PostgresCdcStreamSrcConn,
    type_="postgres-cdc",
    system="PostgreSQL",
    src_version="v1",
    src=PostgresCdcStreamSrc,
    cardinality=lambda src: len(src.tables),
    plugin=PostgresCdcStreamSrcPlugin,
    publisher_src=CdcStageSrc,
    explorer=PostgresCdcConnectionExplorer,
    icon="conn/postgres/resources/postgresql-cdc.svg",
)

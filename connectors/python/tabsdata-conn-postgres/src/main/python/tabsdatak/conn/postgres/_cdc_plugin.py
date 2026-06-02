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
from typing import Any, cast

from tabsdatak.conn.common.sql.cdc._runner import CdcPlugin, CdcRunConfig
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
)
from tabsdatak.conn.postgres import (
    CdcSrcCfgKey,
    PostgresCdcStartFromSpec,
    PostgresCdcStreamSrc,
    PostgresCdcStreamSrcConn,
)

# noinspection PyProtectedMember
from tabsdatak.conn.postgres._cdc_impl import (
    DEFAULT_WAL2JSON_FORMAT_VERSION,
    Wal2JsonIngester,
)
from tabsdatak.spi import (
    Explorer,
    StreamSrcDef,
)


# Allowed CDC src_cfg keys -- the Literal annotation gates typos at
# schema-construction time. ``_build_cdc_run_config`` reads from
# src_cfg and falls back to common-sql defaults.
_KEY_CDC_BLOCKING_TIMEOUT: CdcSrcCfgKey = "postgres.cdc.blocking_timeout_seconds"
_KEY_CDC_LOGGING_LEVEL: CdcSrcCfgKey = "postgres.cdc.logging_level"
_KEY_CDC_WAL2JSON_FORMAT_VERSION: CdcSrcCfgKey = "postgres.cdc.wal2json_format_version"
_KEY_CDC_BUFFER_MAX_ROWS: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_rows"
_KEY_CDC_BUFFER_MAX_BYTES: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_bytes"
_KEY_CDC_BUFFER_MAX_SECS: CdcSrcCfgKey = "tabsdata.cdc.buffer.max_secs"
_KEY_CDC_TRIGGER_MAX_ROWS: CdcSrcCfgKey = "tabsdata.cdc.trigger.max_rows"
_KEY_CDC_TRIGGER_MAX_BYTES: CdcSrcCfgKey = "tabsdata.cdc.trigger.max_bytes"
_KEY_CDC_TRIGGER_MAX_SECS: CdcSrcCfgKey = "tabsdata.cdc.trigger.max_secs"
_KEY_CDC_POLL_INTERVAL: CdcSrcCfgKey = "tabsdata.cdc.poll_interval_seconds"


def _to_internal_start_from(spec: PostgresCdcStartFromSpec):
    """Translate the user-facing ``start_from`` into the shape the
    ingester / runner consume. Postgres has no public types that
    are distinct from the internal ones (no ``TimestampPosition``,
    no driver-specific position): ``LsnPosition`` / ``"head"`` /
    ``"tail"`` all pass through as-is. The function exists for
    symmetry with the mysql plugin so future divergence (e.g.
    a wall-clock position) has a single conversion site."""
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
        buffer_max_rows=cfg.get(_KEY_CDC_BUFFER_MAX_ROWS, DEFAULT_BUFFER_MAX_ROWS),
        buffer_max_bytes=cfg.get(_KEY_CDC_BUFFER_MAX_BYTES, DEFAULT_BUFFER_MAX_BYTES),
        buffer_max_secs=cfg.get(_KEY_CDC_BUFFER_MAX_SECS, DEFAULT_BUFFER_MAX_SECS),
        trigger_max_rows=cfg.get(_KEY_CDC_TRIGGER_MAX_ROWS, DEFAULT_TRIGGER_MAX_ROWS),
        trigger_max_bytes=cfg.get(
            _KEY_CDC_TRIGGER_MAX_BYTES, DEFAULT_TRIGGER_MAX_BYTES
        ),
        trigger_max_secs=cfg.get(_KEY_CDC_TRIGGER_MAX_SECS, DEFAULT_TRIGGER_MAX_SECS),
        poll_interval_seconds=cfg.get(
            _KEY_CDC_POLL_INTERVAL, DEFAULT_POLL_INTERVAL_SECONDS
        ),
        logging_level=int(cfg.get(_KEY_CDC_LOGGING_LEVEL, logging.ERROR)),
    )


class PostgresCdcStreamSrcPlugin(CdcPlugin[PostgresCdcStreamSrc]):
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
        stream_src: PostgresCdcStreamSrc,
    ) -> Wal2JsonIngester:
        conn = cast(PostgresCdcStreamSrcConn, stream_src.conn)
        src_cfg = stream_src.src_cfg or {}
        blocking_timeout = src_cfg.get(
            _KEY_CDC_BLOCKING_TIMEOUT,
            DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        )
        wal2json_format = src_cfg.get(
            _KEY_CDC_WAL2JSON_FORMAT_VERSION,
            DEFAULT_WAL2JSON_FORMAT_VERSION,
        )
        return Wal2JsonIngester(
            conn=conn,
            tables=list(stream_src.tables),
            start_from=_to_internal_start_from(stream_src.start_from),
            replication_slot=stream_src.replication_slot,
            replication_slot_behavior=stream_src.replication_slot_behavior,
            blocking_timeout_seconds=blocking_timeout,
            publication_name=stream_src.publication_name,
            wal2json_format_version=int(wal2json_format),
        )


class PostgresCdcExplorer(Explorer[PostgresCdcStreamSrcConn]):
    def metadata(self, conn: PostgresCdcStreamSrcConn) -> Any:
        # TODO
        pass

    def data(self, conn: PostgresCdcStreamSrcConn, query: str) -> Any:
        # TODO
        pass


POSTGRES_CDC_STREAM_SRC = StreamSrcDef(
    conn=PostgresCdcStreamSrcConn,
    src_type="postgres-cdc",
    src_version="v1",
    src=PostgresCdcStreamSrc,
    cardinality=lambda src: len(src.tables),
    uses_manifest=True,
    plugin=PostgresCdcStreamSrcPlugin,
    explorer=PostgresCdcExplorer,
)

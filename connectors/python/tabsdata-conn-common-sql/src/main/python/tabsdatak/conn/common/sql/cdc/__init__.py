#
# Copyright 2026 Tabsdata Inc.
#

"""Shared, dialect-agnostic CDC (change-data-capture) machinery.

Building blocks every SQL CDC connector (db2 / mysql / postgres) reuses
so the change-capture loop, staging and output shape stay identical
across dialects. Not end-user API: a flow author wires a connector's own
`<Db>CdcStreamSrc` into a `@stream_publisher` and never touches this
module directly. It is the SPI a connector's CDC plugin builds on.

Public surface (see `__all__`):

* `CdcConn` -- abstract CDC connection base.
* `CdcRunConfig` / `CdcPlugin` -- the per-flow run knobs and the
  abstract `StreamSrcPlugin` that drives the capture loop.
* `CdcStageSrc` / `CdcStageSrcPlugin` / `CDC_STAGE_SRC` -- the
  stream-stage publisher that de-duplicates and offset-filters staged
  change rows.
* `QualifiedTableSpec` / `QualifiedTablesSpec` /
  `RelativePositionSpec` / `TimestampPosition` / `CdcFormat` /
  `CdcColumnsFormatSpec` -- the shared table / position / output-format
  specs each connector re-expresses locally.
"""

import importlib.metadata

from tabsdatak.conn.common.sql.cdc._connection import CdcConn
from tabsdatak.conn.common.sql.cdc._runner import CdcPlugin, CdcRunConfig
from tabsdatak.conn.common.sql.cdc._stage_plugin import (
    CDC_STAGE_SRC,
    CdcStageSrc,
    CdcStageSrcPlugin,
)
from tabsdatak.conn.common.sql.cdc.typing import (
    CdcColumnsFormatSpec,
    CdcFormat,
    QualifiedTableSpec,
    QualifiedTablesSpec,
    RelativePositionSpec,
    TimestampPosition,
)

__all__ = [
    # Connections
    "CdcConn",
    # Runner
    "CdcPlugin",
    "CdcRunConfig",
    # Stream-stage publisher
    "CDC_STAGE_SRC",
    "CdcStageSrc",
    "CdcStageSrcPlugin",
    # Specs
    "QualifiedTableSpec",
    "QualifiedTablesSpec",
    "RelativePositionSpec",
    "TimestampPosition",
    "CdcFormat",
    "CdcColumnsFormatSpec",
]

# noinspection PyBroadException
try:
    __version__ = importlib.metadata.version("tabsdata-conn-common-sql")
except Exception:
    __version__ = "unknown"

version = __version__

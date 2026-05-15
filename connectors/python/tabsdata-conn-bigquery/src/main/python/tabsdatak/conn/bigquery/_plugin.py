from __future__ import annotations

from tabsdatak.conn.bigquery import BigQueryConn, BigQueryDest
from tabsdatak.spi import (
    DestDef,
    DestPlugin,
    Explorer,
)


class _BigQueryDestPlugin(DestPlugin[BigQueryDest]): ...


class _BigQueryExplorer(Explorer[BigQueryConn]): ...


BIGQUERY_DEST = DestDef(
    conn=BigQueryConn,
    dest_type="bigquery",
    dest_version="v1",
    dest=BigQueryDest,
    cardinality=lambda dest: len(dest.tables) if dest.tables is not None else 0,
    plugin=_BigQueryDestPlugin,
    explorer=_BigQueryExplorer,
)

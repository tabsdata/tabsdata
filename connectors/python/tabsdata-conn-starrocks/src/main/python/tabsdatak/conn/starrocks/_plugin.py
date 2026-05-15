from __future__ import annotations

from tabsdatak.conn.starrocks import StarRocksConn, StarRocksDest
from tabsdatak.spi import (
    DestDef,
    DestPlugin,
    Explorer,
)


class _StarRocksDestPlugin(DestPlugin[StarRocksDest]): ...


class _StarRocksExplorer(Explorer[StarRocksConn]): ...


STARROCKS_DEST = DestDef(
    conn=StarRocksConn,
    dest_type="starrocks",
    dest_version="v1",
    dest=StarRocksDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=_StarRocksDestPlugin,
    explorer=_StarRocksExplorer,
)

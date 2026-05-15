from __future__ import annotations

from tabsdatak.conn.clickhouse import ClickHouseConn, ClickHouseDest
from tabsdatak.spi import (
    DestDef,
    DestPlugin,
    Explorer,
)


class _ClickHouseDestPlugin(DestPlugin[ClickHouseDest]): ...


class _ClickHouseExplorer(Explorer[ClickHouseConn]): ...


CLICKHOUSE_DEST = DestDef(
    conn=ClickHouseConn,
    dest_type="clickhouse",
    dest_version="v1",
    dest=ClickHouseDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=_ClickHouseDestPlugin,
    explorer=_ClickHouseExplorer,
)

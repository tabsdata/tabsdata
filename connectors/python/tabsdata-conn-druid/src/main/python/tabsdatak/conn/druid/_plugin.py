from __future__ import annotations

from tabsdatak.conn.druid import DruidConn, DruidDest
from tabsdatak.spi import (
    DestDef,
    DestPlugin,
    Explorer,
)


class _DruidDestPlugin(DestPlugin[DruidDest]): ...


class _DruidExplorer(Explorer[DruidConn]): ...


DRUID_DEST = DestDef(
    conn=DruidConn,
    dest_type="druid",
    dest_version="v1",
    dest=DruidDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=_DruidDestPlugin,
    explorer=_DruidExplorer,
)

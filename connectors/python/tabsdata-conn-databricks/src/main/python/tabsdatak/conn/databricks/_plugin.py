from __future__ import annotations

from tabsdatak.conn.databricks import (
    DatabricksConn,
    DatabricksDest,
    DatabricksSrc,
)
from tabsdatak.spi import (
    DestDef,
    DestPlugin,
    Explorer,
    SrcDef,
    SrcPlugin,
    TableMode,
)


class _DatabricksSrcPlugin(SrcPlugin[DatabricksSrc]): ...


class _DatabricksDestPlugin(DestPlugin[DatabricksDest]): ...


class _DatabricksExplorer(Explorer[DatabricksConn]): ...


DATABRICKS_SRC = SrcDef(
    conn=DatabricksConn,
    src_type="databricks",
    src_version="v1",
    src=DatabricksSrc,
    table_mode=TableMode.SINGLE,
    cardinality=lambda src: len(src.queries),
    plugin=_DatabricksSrcPlugin,
    explorer=_DatabricksExplorer,
)

DATABRICKS_DEST = DestDef(
    conn=DatabricksConn,
    dest_type="databricks",
    dest_version="v1",
    dest=DatabricksDest,
    cardinality=lambda dest: len(dest.tables),
    plugin=_DatabricksDestPlugin,
    explorer=_DatabricksExplorer,
)

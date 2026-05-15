from __future__ import annotations

from tabsdatak.conn.mongodb import MongoDBConn, MongoDBDest
from tabsdatak.spi import (
    DestDef,
    DestPlugin,
    Explorer,
)


class _MongoDBDestPlugin(DestPlugin[MongoDBDest]): ...


class _MongoDBExplorer(Explorer[MongoDBConn]): ...


MONGODB_DEST = DestDef(
    conn=MongoDBConn,
    dest_type="mongodb",
    dest_version="v1",
    dest=MongoDBDest,
    cardinality=lambda dest: len(dest.collections_with_ids),
    plugin=_MongoDBDestPlugin,
    explorer=_MongoDBExplorer,
)

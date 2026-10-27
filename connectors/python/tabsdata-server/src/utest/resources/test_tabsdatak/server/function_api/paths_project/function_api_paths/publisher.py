#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrc
from tabsdatak.tableframe.functions import concat

SOURCE = LocalFileSrc(paths=["a.parquet", "b.parquet"])


@publisher(source=SOURCE, output_tables=["concat_all", "first_only", "skipped"])
def publish_paths(
    a: TableFramesSpec, b: TableFramesSpec
) -> tuple[TableFrameSpec, TableFrameSpec, TableFrameSpec]:
    return concat([*a, *b], how="vertical_relaxed"), a[0], None

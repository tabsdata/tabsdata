#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrc
from tabsdatak.tableframe.functions import concat


@publisher(
    source=LocalFileSrc(paths=["persons.csv"]),
    output_tables=["persons"],
)
def qs_pub(inputs: TableFramesSpec) -> TableFrameSpec:
    return concat(inputs)

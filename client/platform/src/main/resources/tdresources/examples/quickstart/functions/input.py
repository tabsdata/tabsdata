#
# Copyright 2026 Tabsdata Inc.
#

# Quickstart publisher: read input-data/persons.csv into the `persons` table.

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrc


@publisher(
    source=LocalFileSrc(paths=["persons.csv"]),
    output_tables=["persons"],
)
def qs_pub(inputs: TableFramesSpec) -> TableFrameSpec:
    if inputs is None or len(inputs) == 0:
        return None
    return inputs[0]

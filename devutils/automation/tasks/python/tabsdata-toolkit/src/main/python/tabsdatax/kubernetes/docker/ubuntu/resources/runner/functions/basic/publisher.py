#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrc


@publisher(
    source=LocalFileSrc(paths=["persons.csv"]),
    output_tables=["persons"],
)
def persons_publisher(persons: TableFramesSpec) -> TableFrameSpec:
    assert persons is not None
    return persons[0]

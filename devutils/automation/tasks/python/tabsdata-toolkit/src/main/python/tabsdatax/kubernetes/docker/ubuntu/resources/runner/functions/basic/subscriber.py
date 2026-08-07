#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak.api import TableFrameSpec, subscriber
from tabsdatak.conn.localfile import LocalFileDest


@subscriber(
    input_tables=["spanish", "french"],
    destination=LocalFileDest(paths=["spanish.jsonl", "french.jsonl"]),
)
def persons_subscriber(
    spanish: TableFrameSpec,
    french: TableFrameSpec,
) -> tuple[TableFrameSpec, TableFrameSpec]:
    return spanish, french

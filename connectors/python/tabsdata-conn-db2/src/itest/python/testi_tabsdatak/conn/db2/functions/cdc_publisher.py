#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak.api import TableFrameSpec, TableFramesSpec, stream_publisher
from tabsdatak.conn.db2 import Db2CdcStreamSrc

PUBLISHED_TABLE = "db2_cdc_published"

SOURCE_TABLE = "TD_IL_INSTANCE"
TRACKED_TABLE = "ASN.TD_T__TD_IL_INSTANCE"

SEEDED_ROWS = [
    {"id": index, "name": f"seed-{index}", "age": index} for index in range(1, 6)
]

SRC_CFG = {
    "tabsdata.cdc.buffer.max_rows": 1,
    "tabsdata.cdc.buffer.max_secs": 1,
    "tabsdata.cdc.poll_interval_seconds": 1,
    "db2.cdc.blocking_timeout_seconds": 3,
}


@stream_publisher(
    source=Db2CdcStreamSrc(
        tables=[TRACKED_TABLE],
        initial_load=True,
        src_cfg=SRC_CFG,
    ),
    flush_interval_mins=1,
    output_tables=[PUBLISHED_TABLE],
)
def pub_stream_publisher_initial_load_from_db2cdc_domain(
    inputs: TableFramesSpec,
) -> TableFrameSpec:
    frames = [frame for frame in (inputs or []) if frame is not None]
    return frames[0] if frames else None

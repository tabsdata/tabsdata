#
# Copyright 2026 Tabsdata Inc.
#

# noinspection PyProtectedMember
from tabsdatak._api import unsafe_seconds
from tabsdatak.api import TableFrameSpec, TableFramesSpec, stream_publisher
from tabsdatak.conn.mysql import MySQLCdcStreamSrc

PUBLISHED_TABLE = "mysql_cdc_published"

TRACKED_TABLE = "td_il_instance"

SEEDED_ROWS = [{"id": index, "name": f"seed-{index}"} for index in range(1, 6)]

SRC_CFG = {
    "tabsdata.cdc.buffer.max_rows": 1,
    "tabsdata.cdc.buffer.max_secs": 1,
    "tabsdata.cdc.poll.idle_secs": 1,
    "tabsdata.cdc.poll.max_secs": 2,
}


@stream_publisher(
    source=MySQLCdcStreamSrc(
        tables=[TRACKED_TABLE],
        initial_load=True,
        src_cfg=SRC_CFG,
    ),
    batch_min_secs=unsafe_seconds(1),
    batch_max_secs=unsafe_seconds(1),
    batch_max_rows=1,
    output_tables=[PUBLISHED_TABLE],
)
def pub_stream_publisher_initial_load_from_mysqlcdc_domain(
    inputs: TableFramesSpec,
) -> TableFrameSpec:
    frames = [frame for frame in (inputs or []) if frame is not None]
    return frames[0] if frames else None

#
# Copyright 2026 Tabsdata Inc.
#

# noinspection PyProtectedMember
from tabsdatak._api import unsafe_seconds
from tabsdatak.api import TableFrameSpec, TableFramesSpec, stream_publisher
from tabsdatak.conn.postgres import PostgresCdcStreamSrc

PUBLISHED_TABLE = "postgres_cdc_published"

TRACKED_TABLE = "public.td_il_instance"
REPLICATION_SLOT = "td_il_instance_slot"

SEEDED_ROWS = [{"id": index, "name": f"seed-{index}"} for index in range(1, 6)]

SRC_CFG = {
    "tabsdata.cdc.buffer.max_rows": 1,
    "tabsdata.cdc.buffer.max_secs": 1,
    "tabsdata.cdc.poll.idle_secs": 1,
    "tabsdata.cdc.poll.max_secs": 2,
}


@stream_publisher(
    source=PostgresCdcStreamSrc(
        tables=[TRACKED_TABLE],
        replication_slot=REPLICATION_SLOT,
        replication_slot_behavior="reuse",
        initial_load=True,
        src_cfg=SRC_CFG,
    ),
    batch_min_secs=unsafe_seconds(1),
    batch_max_secs=unsafe_seconds(1),
    batch_max_rows=1,
    output_tables=[PUBLISHED_TABLE],
)
def pub_stream_publisher_initial_load_from_postgrescdc_domain(
    inputs: TableFramesSpec,
) -> TableFrameSpec:
    frames = [frame for frame in (inputs or []) if frame is not None]
    return frames[0] if frames else None

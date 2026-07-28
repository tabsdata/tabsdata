#
# Copyright 2026 Tabsdata Inc.
#

"""Publishers used by test_tabsdata_src.py and test_tabsdata_dest.py"""

import polars as pl

from tabsdatak.api import TableFrameSpec, TrxCtx, publisher
from tabsdatak.conn.db2 import Db2Src
from tabsdatak.conn.dev import Data, DataGenSrc

PUBLISHED_TABLE = "db2_published"
SOURCE_COLLECTION = "db2_source"

WATERMARK_SEED = 10000
WATERMARK_ADVANCE_TO = 10050

GENERATED_FRAME = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["alice", "bob", "carol"],
})


@publisher(
    source=Db2Src(queries=["SELECT * FROM departments"]),
    output_tables=[PUBLISHED_TABLE],
)
def pub_publisher_reads_table_from_db2_domain(
    frame: TableFrameSpec,
) -> TableFrameSpec:
    return frame


@publisher(
    source=Db2Src(
        queries=[
            "SELECT emp_no, first_name FROM employees WHERE emp_no > :wm "
            "ORDER BY emp_no"
        ],
        initial_values={"wm": WATERMARK_SEED},
    ),
    output_tables=[PUBLISHED_TABLE],
)
def pub_publisher_initial_values_advance_across_runs(
    frame: TableFrameSpec, ctx: TrxCtx
) -> TableFrameSpec:
    ctx.set_attr("wm", WATERMARK_ADVANCE_TO)
    return frame


@publisher(
    source=DataGenSrc(data=[Data(GENERATED_FRAME)]),
    output_tables=[PUBLISHED_TABLE],
)
def pub_subscriber_writes_table_to_db2_domain(
    frame: TableFrameSpec,
) -> TableFrameSpec:
    return frame

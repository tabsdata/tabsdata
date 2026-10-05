#
# Copyright 2026 Tabsdata Inc.
#

import time

import polars as pl

from tabsdatak.api import CronTrigger, TableFrameSpec, publisher
from tabsdatak.conn.dev import Data, DataGenSrc

SLEEP_SECONDS = 3600

_SOURCE = DataGenSrc(data=[Data(pl.DataFrame({"tick": [1]}))])


@publisher(source=_SOURCE, output_tables=["slept"])
def sleeper(frame: TableFrameSpec) -> TableFrameSpec:
    time.sleep(SLEEP_SECONDS)
    return frame


@publisher(source=_SOURCE, output_tables=["quick"])
def quick(frame: TableFrameSpec) -> TableFrameSpec:
    return frame


@publisher(
    source=_SOURCE,
    output_tables=["ticked"],
    trigger_by=CronTrigger(mask="*/1 * * * *"),
)
def ticker(frame: TableFrameSpec) -> TableFrameSpec:
    return frame

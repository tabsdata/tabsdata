#
# Copyright 2026 Tabsdata Inc.
#

"""Functions for ``test_execution_settlement.py``: one publisher feeding two
transformers, each followed by a subscriber, so that one execution spans
three transactions (one per collection: sources, transformers, destinations)
and the transformers' transaction holds two function runs. The transformers
take a few seconds, so a test can cancel one of their runs before it ends.
``failing`` stands in for a transformer that breaks: it works for a minute,
then raises.

Collections are named here because transformers and subscribers name their
input tables by collection; each test runs in its own project, so the fixed
names collide with nothing.
"""

import time

import polars as pl

from tabsdatak.api import TableFrameSpec, publisher, subscriber, transformer
from tabsdatak.conn.dev import Data, DataGenSrc, TrashDest

SOURCES = "settlement_sources"
TRANSFORMERS = "settlement_transformers"
DESTINATIONS = "settlement_destinations"

PUBLISHER_SECONDS = 10
TRANSFORMER_SECONDS = 15
FAILING_SECONDS = 60

_SOURCE = DataGenSrc(data=[Data(pl.DataFrame({"tick": [1]}))])


@publisher(source=_SOURCE, output_tables=["ticks"])
def ticks(frame: TableFrameSpec) -> TableFrameSpec:
    time.sleep(PUBLISHER_SECONDS)
    return frame


@transformer(input_tables=[f"{SOURCES}/ticks"], output_tables=["left"])
def left(frame: TableFrameSpec) -> TableFrameSpec:
    time.sleep(TRANSFORMER_SECONDS)
    return frame


@transformer(input_tables=[f"{SOURCES}/ticks"], output_tables=["right"])
def right(frame: TableFrameSpec) -> TableFrameSpec:
    time.sleep(TRANSFORMER_SECONDS)
    return frame


@transformer(input_tables=[f"{SOURCES}/ticks"], output_tables=["broken"])
def failing(frame: TableFrameSpec) -> TableFrameSpec:
    time.sleep(FAILING_SECONDS)
    raise RuntimeError("failing on purpose after working for a minute")


@subscriber(destination=TrashDest(), input_tables=[f"{TRANSFORMERS}/left"])
def after_left(frame: TableFrameSpec) -> TableFrameSpec:
    return frame


@subscriber(destination=TrashDest(), input_tables=[f"{TRANSFORMERS}/right"])
def after_right(frame: TableFrameSpec) -> TableFrameSpec:
    return frame


@subscriber(destination=TrashDest(), input_tables=[f"{TRANSFORMERS}/broken"])
def after_failing(frame: TableFrameSpec) -> TableFrameSpec:
    return frame

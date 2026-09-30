#
# Copyright 2026 Tabsdata Inc.
#

"""Publishers whose data-quality summaries drive the trend transformers that
``test_dq_trend_lifecycle.py`` exercises.

One published table, one boolean classifier, and a ``Summary`` per variant --
the tests care about what registration derives from the declaration, not about
the numbers, so the generated frame is as small as it can be. ``DataGenSrc``
keeps the project self-contained: no external system feeds these.
"""

import polars as pl

from tabsdatak.api import TableFrameSpec, publisher
from tabsdatak.conn.dev import Data, DataGenSrc
from tabsdatak.dataquality import DataQuality, IsPositive, Summary, SummaryTrend

PUBLISHED_TABLE = "trend_source"

# The names registration derives, asserted by the tests rather than recomputed
# there -- if the derivation changes, these are what has to change with it.
SUMMARY_TABLE = f"{PUBLISHED_TABLE}_dq_summary"
DEFAULT_TREND_TABLE = f"{PUBLISHED_TABLE}_dq_summary_trends"
CUSTOM_TREND_TABLE = "trend_source_custom_trends"

GENERATED_FRAME = pl.DataFrame({"amount": [1, -2, 3]})

_SOURCE = DataGenSrc(data=[Data(GENERATED_FRAME)])


def _action(trend: SummaryTrend | None) -> DataQuality:
    return DataQuality(
        table=PUBLISHED_TABLE,
        classifiers=[IsPositive(["amount"])],
        operators=[Summary(trend=trend)],
    )


@publisher(
    source=_SOURCE,
    output_tables=[PUBLISHED_TABLE],
    on_tables=[_action(SummaryTrend())],
)
def pub_with_default_trend(frame: TableFrameSpec) -> TableFrameSpec:
    return frame


@publisher(
    source=_SOURCE,
    output_tables=[PUBLISHED_TABLE],
    on_tables=[_action(SummaryTrend(table=CUSTOM_TREND_TABLE))],
)
def pub_with_custom_trend(frame: TableFrameSpec) -> TableFrameSpec:
    return frame


@publisher(
    source=_SOURCE,
    output_tables=[PUBLISHED_TABLE],
    on_tables=[_action(SummaryTrend(days=1, weeks=2, months=3))],
)
def pub_with_custom_retention(frame: TableFrameSpec) -> TableFrameSpec:
    return frame


@publisher(
    source=_SOURCE,
    output_tables=[PUBLISHED_TABLE],
    on_tables=[_action(None)],
)
def pub_without_trend(frame: TableFrameSpec) -> TableFrameSpec:
    return frame


@publisher(source=_SOURCE, output_tables=[PUBLISHED_TABLE])
def pub_without_data_quality(frame: TableFrameSpec) -> TableFrameSpec:
    return frame

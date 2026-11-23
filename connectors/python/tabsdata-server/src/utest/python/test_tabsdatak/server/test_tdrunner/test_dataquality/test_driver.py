#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.dataquality import DataQuality, IsPositive, Summary
from tabsdatak.dataquality._symbols import TableType
from tabsdatak.server._tdrunner import dataquality
from tabsdatak.tableframe import TableFrame

pytestmark = pytest.mark.dq


def _summary_action(table: str = "out") -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=[IsPositive(["a"])],
        operators=[Summary()],
    )


def test_has_data_quality_detects_dataquality_actions():
    assert dataquality.driver.has_data_quality([_summary_action()]) is True
    assert dataquality.driver.has_data_quality([]) is False
    assert dataquality.driver.has_data_quality(None) is False


def test_run_data_quality_produces_derived_tables():
    frame = TableFrame.from_dict({"a": [1, -2, 3]})
    result = dataquality.driver.run_data_quality(
        collection="c",
        output_tables=["out"],
        output_frames=[frame],
        on_tables=[_summary_action()],
    )
    assert len(result.output_frames) == 1
    assert len(result.derived) == 1
    derived = result.derived[0]
    assert derived.name == "out_dq_summary"
    assert derived.table_type is TableType.DQSummary
    assert isinstance(derived.frame, TableFrame)


def test_run_data_quality_derives_tables_without_data_from_a_none_output():
    result = dataquality.driver.run_data_quality(
        collection="c",
        output_tables=["none", "out"],
        output_frames=[None, TableFrame.from_dict({"a": [1, -2, 3]})],
        on_tables=[_summary_action("none"), _summary_action("out")],
    )
    assert [(derived.name, derived.frame is None) for derived in result.derived] == [
        ("none_dq_summary", True),
        ("out_dq_summary", False),
    ]

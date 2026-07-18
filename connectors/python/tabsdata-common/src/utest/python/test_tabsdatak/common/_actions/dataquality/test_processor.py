#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.common._actions.dataquality.processor import DataQualityProcessor
from tabsdatak.dataquality import DataQuality, IsPositive, Summary
from tabsdatak.dataquality._symbols import TableType
from tabsdatak.tableframe import TableFrame

pytestmark = pytest.mark.dq


def _summary_action(table: str = "out") -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=[IsPositive("a")],
        operators=[Summary()],
    )


def test_from_actions_builds_a_processor():
    processor = DataQualityProcessor.from_actions(
        collection="c", output_tables=["out"], actions=[_summary_action()]
    )
    assert processor.collection == "c"


def test_table_names_derives_summary_output():
    processor = DataQualityProcessor.from_actions(
        collection="c", output_tables=["out"], actions=[_summary_action()]
    )
    _, output_infos = processor.table_names()
    assert [info.table for info in output_infos] == ["out_dq_summary"]
    assert output_infos[0].kind.type is TableType.DQSummary


def test_tables_data_produces_derived_frame():
    processor = DataQualityProcessor.from_actions(
        collection="c", output_tables=["out"], actions=[_summary_action()]
    )
    frame = TableFrame.from_dict({"a": [1, -2, 3]})
    frames = [frame]
    derived: list = []
    processor.tables_data([], frames, derived)
    assert len(derived) == 1
    assert isinstance(derived[0].derived_tf, TableFrame)


def test_no_dataquality_actions_yields_no_derived_tables():
    processor = DataQualityProcessor.from_actions(
        collection="c", output_tables=["out"], actions=[]
    )
    _, output_infos = processor.table_names()
    assert output_infos == []

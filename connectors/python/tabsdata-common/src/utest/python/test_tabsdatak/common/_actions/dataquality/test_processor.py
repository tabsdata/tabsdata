#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.common._actions.dataquality.processor import DataQualityProcessor
from tabsdatak.dataquality import (
    AnyFailed,
    DataQuality,
    Enrich,
    Fail,
    IsPositive,
    RowCountThreshold,
    Summary,
)
from tabsdatak.dataquality._symbols import TableType
from tabsdatak.tableframe import TableFrame

pytestmark = pytest.mark.dq


def _summary_action(table: str = "out") -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=[IsPositive(["a"])],
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


def _enrich_action(table: str = "out") -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=[IsPositive(["a"])],
        operators=[Enrich(to_table=f"{table}_enriched"), Summary(table=f"{table}_sum")],
    )


def _run(frames: list, actions: list, tables: list[str]) -> list:
    processor = DataQualityProcessor.from_actions(
        collection="c", output_tables=tables, actions=actions
    )
    derived: list = []
    processor.tables_data([], list(frames), derived)
    return derived


@pytest.mark.parametrize(
    "frames",
    [[None], [[]], [[None]], [[None, None]]],
    ids=["none", "empty-list", "list-of-none", "list-of-nones"],
)
def test_a_table_with_no_data_derives_nothing(frames):
    assert _run(frames, [_summary_action()], ["out"]) == []


@pytest.mark.parametrize(
    "operators",
    [
        [Summary()],
        [Enrich(to_table="out_enriched")],
        [Enrich(to_table="out_enriched"), Summary(table="out_sum")],
    ],
    ids=["summary", "enrich", "enrich-and-summary"],
)
def test_no_data_derives_nothing_whatever_the_producing_operators(operators):
    action = DataQuality(
        table="out", classifiers=[IsPositive(["a"])], operators=operators
    )
    assert _run([None], [action], ["out"]) == []


def test_a_non_producing_operator_alone_already_tolerates_no_data():
    action = DataQuality(
        table="out",
        classifiers=[IsPositive(["a"])],
        operators=[Fail(AnyFailed(), RowCountThreshold(0))],
    )
    assert _run([None], [action], ["out"]) == []


def test_an_empty_table_does_not_fail_the_table_beside_it():
    derived = _run(
        [TableFrame.from_dict({"a": [1, -2, 3]}), None],
        [_enrich_action("one"), _enrich_action("two")],
        ["one", "two"],
    )
    assert [record.derived_tf is not None for record in derived] == [True, True]


def test_every_table_empty_derives_nothing():
    derived = _run(
        [None, None],
        [_enrich_action("one"), _enrich_action("two")],
        ["one", "two"],
    )
    assert derived == []


def test_a_table_holding_data_among_empty_slots_is_still_processed():
    derived = _run(
        [[None, TableFrame.from_dict({"a": [1, -2, 3]})]],
        [_summary_action()],
        ["out"],
    )
    assert len(derived) == 1

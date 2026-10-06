#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from tabsdatak.common._actions.dataquality.processor import DataQualityProcessor
from tabsdatak.common._actions.dataquality.trend import cleanup_trend_tfr_mod
from tabsdatak.dataquality import (
    AnyFailed,
    DataQuality,
    Enrich,
    Fail,
    IsPositive,
    RowCountThreshold,
    Summary,
    SummaryTrend,
)
from tabsdatak.dataquality._error import DataQualityErrorCode
from tabsdatak.dataquality._symbols import TableType
from tabsdatak.dataquality._tableinfo import TableKind
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


def _processor(*actions, output_tables=("out",)):
    return DataQualityProcessor.from_actions(
        collection="c", output_tables=list(output_tables), actions=list(actions)
    )


def _action(table: str = "out", *operators) -> DataQuality:
    return DataQuality(
        table=table,
        classifiers=[IsPositive(["a"])],
        operators=list(operators),
    )


@pytest.fixture
def transformers():
    """Build trend transformers, releasing each generated module afterwards."""
    built = []

    def build(processor):
        infos = processor.build_trend_transformers_info()
        built.extend(infos)
        return infos

    yield build

    for info in built:
        cleanup_trend_tfr_mod(info.project_root)


def test_table_name_for_trends_suffixes_the_target():
    info = DataQualityProcessor.table_name_for_trends("c", "out", 0, False, "c", "out")
    assert info.table == "out_dq_summary_trends"
    assert info.kind is TableKind.DQ_SUMMARY_TRENDS


def test_table_name_for_trends_takes_a_custom_name_verbatim():
    info = DataQualityProcessor.table_name_for_trends(
        "c", "my_trend", 0, True, "c", "out"
    )
    assert info.table == "my_trend"


def test_trend_table_names_defaults_to_the_target():
    processor = _processor(_action("out", Summary()))
    assert [i.table for i in processor.trend_table_names()] == ["out_dq_summary_trends"]


def test_trend_table_names_uses_a_declared_trend_table():
    processor = _processor(_action("out", Summary(trend=SummaryTrend(table="t"))))
    assert [i.table for i in processor.trend_table_names()] == ["t"]


def test_trend_table_names_skips_a_summary_that_opted_out():
    processor = _processor(_action("out", Summary(trend=None)))
    assert processor.trend_table_names() == []


def test_trend_table_names_ignores_non_summary_operators():
    processor = _processor(_action("out", Enrich("enriched")))
    assert processor.trend_table_names() == []


def test_trend_table_names_is_empty_without_output_actions():
    assert _processor().trend_table_names() == []


def test_table_names_validates_trend_tables_alongside_the_rest():
    # Two summaries whose trend tables resolve to one name would otherwise
    # collide silently at registration, so they are validated together.
    processor = _processor(
        _action(
            "out",
            Summary(table="one", trend=SummaryTrend(table="clash")),
            Summary(table="two", trend=SummaryTrend(table="clash")),
        )
    )
    with pytest.raises(Exception) as exc:
        processor.table_names()
    assert exc.value.api_error is DataQualityErrorCode.DQPE_4


def test_build_trend_transformers_info_reads_the_summary_and_its_own_table(
    transformers,
):
    processor = _processor(_action("out", Summary()))
    (built,) = transformers(processor)

    assert built.info.output_tables == ["out_dq_summary_trends"]
    assert built.info.input_tables == [
        "c/out_dq_summary@HEAD",
        "out_dq_summary_trends@HEAD",
    ]
    assert built.info.trigger_by is None
    assert built.info.on_tables is None


def test_build_trend_transformers_info_follows_a_custom_summary_table(transformers):
    processor = _processor(_action("out", Summary(table="my_summary")))
    (built,) = transformers(processor)
    assert built.info.input_tables[0] == "c/my_summary@HEAD"


def test_build_trend_transformers_info_names_the_child_after_its_table(transformers):
    processor = _processor(_action("out", Summary()))
    (built,) = transformers(processor)
    assert built.function_name == "out_dq_summary_trends_trend"
    assert built.table_types == {"out_dq_summary_trends": "DQST"}


def test_build_trend_transformers_info_follows_a_custom_trend_table(transformers):
    processor = _processor(
        _action("out", Summary(trend=SummaryTrend(table="my_trend")))
    )
    (built,) = transformers(processor)
    assert built.info.output_tables == ["my_trend"]
    assert built.info.input_tables[1] == "my_trend@HEAD"


def test_build_trend_transformers_info_skips_a_summary_that_opted_out(transformers):
    processor = _processor(_action("out", Summary(trend=None)))
    assert transformers(processor) == []


def test_build_trend_transformers_info_ignores_non_summary_operators(transformers):
    processor = _processor(_action("out", Enrich("enriched")))
    assert transformers(processor) == []


def test_build_trend_transformers_info_is_empty_without_output_actions(transformers):
    assert transformers(_processor()) == []


def test_build_trend_transformers_info_builds_one_per_summary(transformers):
    processor = _processor(
        _action(
            "out",
            Summary(table="one", trend=SummaryTrend(table="one_trend")),
            Summary(table="two", trend=SummaryTrend(table="two_trend")),
        )
    )
    built = transformers(processor)
    assert [b.info.output_tables[0] for b in built] == ["one_trend", "two_trend"]


def test_build_trend_transformers_info_bakes_the_declared_retention_in(transformers):
    processor = _processor(
        _action("out", Summary(trend=SummaryTrend(days=1, weeks=2, months=3)))
    )
    (built,) = transformers(processor)
    assert built.info.func.__module__.startswith("dq_trend_1d_2w_3m_")


def test_build_trend_transformers_info_owns_the_directory_it_generated(transformers):
    processor = _processor(_action("out", Summary()))
    (built,) = transformers(processor)
    assert isinstance(built.project_root, TemporaryDirectory)
    assert Path(built.project_root.name).is_dir()

#
# Copyright 2026 Tabsdata Inc.
#

import polars as pl
import pytest

import tabsdatak.dataquality as dq
from tabsdatak.common._actions.dataquality.processor import DataQualityProcessor
from tabsdatak.common._actions.dataquality.record import DerivedTableFrame
from tabsdatak.dataquality._tableinfo import TableKind
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe._core.hooks import tableframe_of

pytestmark = pytest.mark.dq

USER_COLUMNS = ["id", "value", "score"]

ALL = {0, 1, 2, 3, 4, 5, 6}
POSITIVE = {0, 4}
NOT_POSITIVE = {1, 2, 5}
NULL_ROW = {3}
NAN_ROW = {6}
LOW_SCORE = {0, 1, 2, 3}
HIGH_SCORE = {4, 5, 6}


def dataset():
    return tableframe_of(
        pl.LazyFrame({
            "id": [0, 1, 2, 3, 4, 5, 6],
            "value": [10.0, -5.0, 0.0, None, 3.0, -1.0, float("nan")],
            "score": [1, 2, 3, 4, 5, 6, 7],
        })
    )


def is_positive(**kwargs) -> dq.IsPositive:
    return dq.IsPositive(["value"], prefix="", **kwargs)


def low_score(**kwargs) -> dq.IsBetween:
    return dq.IsBetween(
        ["score"], min_val=1, max_val=4, closed_on="both", prefix="", **kwargs
    )


def score_category() -> dq.ScaleCategorizer:
    return dq.ScaleCategorizer(
        ["score"], scale=dq.LinearScale(0.0, 7.0, bins=7), prefix=""
    )


def harvest(operator, classifiers=None):
    action = dq.DataQuality(
        table="orders",
        classifiers=classifiers if classifiers is not None else [is_positive()],
        operators=[operator],
    )
    dtf, schema, cols, tags = DataQualityProcessor.compute([dataset()], action)
    return DataQualityProcessor.harvest(dtf, schema, cols, tags, None, operator)


def rows(dtf: DerivedTableFrame) -> pl.DataFrame:
    return dtf.derived_tf._lf.collect()


def ids(dtf: DerivedTableFrame) -> set[int]:
    return set(rows(dtf)["id"].to_list())


def columns(dtf: DerivedTableFrame) -> list[str]:
    return [name for name in rows(dtf).columns if not name.startswith("$td.")]


# --- Filter: drops the rows its criteria selects ------------------------------


def test_filter_drops_the_rows_the_criteria_selects():
    result = harvest(dq.Filter(dq.AnyFailed()))
    assert ids(result.output) == POSITIVE
    assert result.derived is None


def test_filter_redirects_the_dropped_rows_to_the_table():
    result = harvest(dq.Filter(dq.AnyFailed(), to_table="rejected"))
    assert ids(result.output) == POSITIVE
    assert ids(result.derived) == NOT_POSITIVE | NULL_ROW | NAN_ROW


def test_filter_with_all_ok_drops_the_passing_rows():
    result = harvest(dq.Filter(dq.AllOk(), to_table="passed"))
    assert ids(result.output) == NOT_POSITIVE | NULL_ROW | NAN_ROW
    assert ids(result.derived) == POSITIVE


@pytest.mark.parametrize(
    "criteria",
    [
        dq.AnyFailed(),
        dq.AllOk(),
        dq.AnyFailed(none_is_ok=True),
        dq.AllOk(nan_is_ok=True),
    ],
    ids=["any-failed", "all-ok", "any-failed-none-ok", "all-ok-nan-ok"],
)
def test_filter_kept_and_dropped_rows_partition_the_table(criteria):
    result = harvest(dq.Filter(criteria, to_table="dropped"))
    kept, dropped = ids(result.output), ids(result.derived)
    assert kept | dropped == ALL
    assert kept & dropped == set()


@pytest.mark.parametrize(
    "none_is_ok,nan_is_ok,kept",
    [
        (False, False, POSITIVE),
        (True, False, POSITIVE | NULL_ROW),
        (False, True, POSITIVE | NAN_ROW),
        (True, True, POSITIVE | NULL_ROW | NAN_ROW),
    ],
)
def test_filter_none_is_ok_and_nan_is_ok_decide_the_null_and_nan_rows(
    none_is_ok, nan_is_ok, kept
):
    criteria = dq.AnyFailed(none_is_ok=none_is_ok, nan_is_ok=nan_is_ok)
    result = harvest(dq.Filter(criteria, to_table="rejected"))
    assert ids(result.output) == kept
    assert ids(result.derived) == ALL - kept


def test_filter_criteria_tags_scope_the_verdicts_consulted():
    classifiers = [is_positive(tags=["sign"]), low_score(tags=["range"])]
    result = harvest(
        dq.Filter(dq.AnyFailed(tags=["range"]), to_table="rejected"), classifiers
    )
    assert ids(result.output) == LOW_SCORE
    assert ids(result.derived) == HIGH_SCORE


def test_filter_with_a_category_criteria_drops_the_rows_in_the_bins():
    criteria = dq.InBins(column_name="score_category", bins={1, 2})
    result = harvest(
        dq.Filter(criteria, to_table="in_bins", include_quality_columns="all"),
        [score_category()],
    )
    kept, dropped = ids(result.output), ids(result.derived)
    assert dropped and kept
    assert set(rows(result.derived)["score_category"].to_list()) <= {1, 2}
    assert kept | dropped == ALL
    assert kept & dropped == set()


def test_filter_dropping_every_row_leaves_an_empty_table_with_the_schema():
    result = harvest(
        dq.Filter(dq.AnyFailed(), to_table="rejected"),
        [dq.IsNegative(["score"], prefix="")],
    )
    assert rows(result.output).height == 0
    assert columns(result.output) == USER_COLUMNS
    assert ids(result.derived) == ALL


def test_filter_dropping_no_row_keeps_the_table_intact():
    result = harvest(
        dq.Filter(dq.AnyFailed(), to_table="rejected"),
        [dq.IsPositive(["score"], prefix="")],
    )
    assert ids(result.output) == ALL
    assert rows(result.derived).height == 0


# --- Select: copies the rows its criteria selects ------------------------------


def test_select_copies_the_selected_rows_and_leaves_the_table_untouched():
    result = harvest(dq.Select(dq.AnyFailed(), to_table="to_review"))
    assert result.output is None
    assert ids(result.derived) == NOT_POSITIVE | NULL_ROW | NAN_ROW


def test_select_with_all_ok_copies_the_passing_rows():
    result = harvest(dq.Select(dq.AllOk(), to_table="passed"))
    assert result.output is None
    assert ids(result.derived) == POSITIVE


def test_select_criteria_tags_scope_the_verdicts_consulted():
    classifiers = [is_positive(tags=["sign"]), low_score(tags=["range"])]
    result = harvest(
        dq.Select(dq.AnyFailed(tags=["sign"]), to_table="to_review"), classifiers
    )
    assert ids(result.derived) == NOT_POSITIVE | NULL_ROW | NAN_ROW


def test_select_of_nothing_yields_an_empty_table_with_the_schema():
    result = harvest(
        dq.Select(dq.AnyFailed(), to_table="to_review"),
        [dq.IsPositive(["score"], prefix="")],
    )
    assert rows(result.derived).height == 0
    assert columns(result.derived) == USER_COLUMNS


@pytest.mark.parametrize(
    "criteria",
    [
        dq.AnyFailed(),
        dq.AllOk(),
        dq.AnyFailed(none_is_ok=True, nan_is_ok=True),
        dq.InBins(column_name="score_category", bins={1, 2}),
        dq.NotInBins(column_name="score_category", bins={1, 2}),
    ],
    ids=["any-failed", "all-ok", "any-failed-lenient", "in-bins", "not-in-bins"],
)
def test_select_copies_exactly_the_rows_filter_drops(criteria):
    classifiers = [is_positive(), score_category()]
    selected = harvest(dq.Select(criteria, to_table="out"), classifiers)
    filtered = harvest(dq.Filter(criteria, to_table="out"), classifiers)
    assert ids(selected.derived) == ids(filtered.derived)
    assert ids(filtered.output) == ALL - ids(selected.derived)


# --- include_quality_columns: only to_table carries verdict columns ------------


def quality_columns_cases():
    bool_criteria = dq.AnyFailed(tags=["range"])
    category_criteria = dq.InBins(column_name="score_category", bins={1, 2})
    return [
        pytest.param(bool_criteria, "none", [], id="none"),
        pytest.param(bool_criteria, "criteria", ["score_is_between"], id="criteria"),
        pytest.param(
            bool_criteria,
            "all",
            ["score_category", "score_is_between", "value_is_positive"],
            id="all",
        ),
        pytest.param(
            category_criteria, "criteria", ["score_category"], id="criteria-category"
        ),
    ]


@pytest.mark.parametrize("criteria,policy,quality_columns", quality_columns_cases())
def test_filter_carries_the_quality_columns_only_into_the_redirect_table(
    criteria, policy, quality_columns
):
    classifiers = [
        is_positive(tags=["sign"]),
        low_score(tags=["range"]),
        score_category(),
    ]
    result = harvest(
        dq.Filter(criteria, to_table="rejected", include_quality_columns=policy),
        classifiers,
    )
    assert columns(result.output) == USER_COLUMNS
    assert columns(result.derived) == USER_COLUMNS + quality_columns


@pytest.mark.parametrize("criteria,policy,quality_columns", quality_columns_cases())
def test_select_carries_the_quality_columns_into_the_table(
    criteria, policy, quality_columns
):
    classifiers = [
        is_positive(tags=["sign"]),
        low_score(tags=["range"]),
        score_category(),
    ]
    result = harvest(
        dq.Select(criteria, to_table="to_review", include_quality_columns=policy),
        classifiers,
    )
    assert columns(result.derived) == USER_COLUMNS + quality_columns


# --- Processor: the target table is rewritten, the redirect table registered ----


def run(operators):
    action = dq.DataQuality(
        table="out", classifiers=[dq.IsPositive(["a"])], operators=operators
    )
    processor = DataQualityProcessor.from_actions(
        collection="c", output_tables=["out"], actions=[action]
    )
    frames = [TableFrame.from_dict({"a": [1, -2, 3]})]
    derived: list = []
    processor.tables_data([], frames, derived)
    return processor, frames, derived


def values(tf) -> list:
    return tf._lf.collect()["a"].to_list()


def test_the_processor_rewrites_the_target_and_registers_the_dropped_rows():
    processor, frames, derived = run([dq.Filter(dq.AnyFailed(), to_table="out_bad")])
    assert values(frames[0]) == [1, 3]
    assert [values(record.derived_tf) for record in derived] == [[-2]]
    _, output_infos = processor.table_names()
    assert [info.table for info in output_infos] == ["out_bad"]
    assert output_infos[0].kind is TableKind.DQ_FILTER


def test_the_processor_discards_the_dropped_rows_without_a_redirect_table():
    processor, frames, derived = run([dq.Filter(dq.AnyFailed())])
    assert values(frames[0]) == [1, 3]
    assert derived == []
    assert processor.table_names() == ([], [])


def test_the_processor_leaves_the_target_untouched_under_select():
    processor, frames, derived = run([dq.Select(dq.AnyFailed(), to_table="out_bad")])
    assert values(frames[0]) == [1, -2, 3]
    assert [values(record.derived_tf) for record in derived] == [[-2]]
    _, output_infos = processor.table_names()
    assert output_infos[0].kind is TableKind.DQ_SELECT

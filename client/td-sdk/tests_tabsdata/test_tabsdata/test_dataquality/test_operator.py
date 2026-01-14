#
# Copyright 2025 Tabs Data Inc.
#

import pytest

from tabsdata.dataquality._operator import (
    AllOk,
    AnyFailed,
    Enrich,
    Fail,
    Filter,
    InBins,
    NotInBins,
    PercentThreshold,
    RowCountThreshold,
    Select,
    Summary,
)
from tabsdata.exceptions import (
    ErrorCode,
    ParameterValidationError,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_summary_class():
    summary = Summary(table="table_name", tags=["tag1", "tag2"])
    assert summary.table == "table_name"
    assert summary.tags == ["tag1", "tag2"]


def test_summary_class_wrong_table():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        Summary(table=3)
    assert e.value.error_code == ErrorCode.PVDQOP11


def test_summary_class_wrong_tags():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        Summary(tags=3)
    assert e.value.error_code == ErrorCode.PVDQOP11


def test_summary_class_wrong_tags_list():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        Summary(tags=[3])
    assert e.value.error_code == ErrorCode.PVDQOP11


def test_filter_out_class():
    criteria = AnyFailed(tags=["tag1", "tag2"])
    filter_out = Filter(to_table="table_name", criteria=criteria)
    assert filter_out.to_table == "table_name"
    assert filter_out.criteria == criteria
    assert filter_out.include_quality_columns == "none"
    assert filter_out.tags == ["tag1", "tag2"]


def test_filter_out_wrong_criteria_type():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        Filter(criteria="wrong_type")
        assert e.value.error_code == ErrorCode.DQCE63


def test_filter_out_wrong_to_table_type():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        Filter(to_table=3)
    assert e.value.error_code == ErrorCode.PVDQOP9


def test_select_class():
    criteria = AnyFailed(tags=["tag1", "tag2"])
    selector = Select(
        criteria,
        to_table="table_name",
        include_quality_columns="criteria",
    )
    assert selector.to_table == "table_name"
    assert selector.criteria == criteria
    assert selector.include_quality_columns == "criteria"
    assert selector.tags == ["tag1", "tag2"]


def test_selector_wrong_criteria_type():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        Select("table_name", criteria="wrong_type")
        assert e.value.error_code == ErrorCode.DQCE65


def test_selector_wrong_to_table_type():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        Select(to_table=3)
    assert e.value.error_code == ErrorCode.PVDQOP10


def test_threshold_count_class():
    thr = RowCountThreshold(3)
    assert thr.row_count == 3


def test_threshold_one_class():
    thr = RowCountThreshold(1)
    assert thr.row_count == 1


def test_threshold_percent_class():
    thr = PercentThreshold(59)
    assert thr.percent == float(59)


def test_threshold_count_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        RowCountThreshold("wrong_type")
    assert e.value.error_code == ErrorCode.PVDQOP6


def test_threshold_count_wrong_value():
    with pytest.raises(ParameterValidationError) as e:
        RowCountThreshold(-3)
    assert e.value.error_code == ErrorCode.PVDQOP6


def test_threshold_count_none():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        RowCountThreshold(None)
    assert e.value.error_code == ErrorCode.PVDQOP6


def test_threshold_percent_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        PercentThreshold("wrong_type")
    assert e.value.error_code == ErrorCode.PVDQOP7


def test_threshold_percent_wrong_value():
    with pytest.raises(ParameterValidationError) as e:
        PercentThreshold(200)
    assert e.value.error_code == ErrorCode.PVDQOP7


def test_threshold_percent_none():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        PercentThreshold(None)
    assert e.value.error_code == ErrorCode.PVDQOP7


def test_fail_class():
    criteria = AllOk(tags=["tag1", "tag2"])
    thr = PercentThreshold(59)
    fail = Fail(criteria, thr)
    assert fail.threshold == thr
    assert fail.criteria == criteria
    assert fail.tags == ["tag1", "tag2"]


def test_fail_class_wrong_threshold_type():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        Fail(threshold="wrong_type")
    assert e.value.error_code == ErrorCode.PVDQOP8


def test_fail_class_wrong_criteria_type():
    with pytest.raises(ParameterValidationError) as e:
        thr = PercentThreshold(59)
        # noinspection PyTypeChecker
        Fail(threshold=thr, criteria="wrong_type")
    assert e.value.error_code == ErrorCode.PVDQOP8


def test_enrich_class():
    enrich = Enrich(to_table="table_name", tags=["tag1", "tag2"])
    assert enrich.to_table == "table_name"
    assert enrich.tags == ["tag1", "tag2"]


def test_enrich_class_defaults():
    enrich = Enrich()
    assert enrich.to_table is None
    assert enrich.tags == None


def test_enrich_wrong_to_table_type():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        Enrich(to_table=3)
    assert e.value.error_code == ErrorCode.PVDQOP9


def test_all_ok_criteria():
    criteria = AllOk()
    assert criteria.none_is_ok is False
    assert criteria.nan_is_ok is False
    assert criteria.tags is None

    criteria = AllOk(none_is_ok=True, nan_is_ok=True, tags=["tag1"])
    assert criteria.none_is_ok is True
    assert criteria.nan_is_ok is True
    assert criteria.tags == ["tag1"]


def test_all_ok_criteria_wrong_tags():
    with pytest.raises(ParameterValidationError) as e:
        AllOk(tags=1)
    assert e.value.error_code == ErrorCode.PVDQOP1


def test_any_failed_criteria():
    criteria = AnyFailed()
    assert criteria.none_is_ok is False
    assert criteria.nan_is_ok is False
    assert criteria.tags is None

    criteria = AnyFailed(none_is_ok=True, nan_is_ok=True, tags=["tag1"])
    assert criteria.none_is_ok is True
    assert criteria.nan_is_ok is True
    assert criteria.tags == ["tag1"]


def test_any_failed_criteria_wrong_tags():
    with pytest.raises(ParameterValidationError) as e:
        AnyFailed(tags=1)
    assert e.value.error_code == ErrorCode.PVDQOP1


def test_in_bins_criteria():
    criteria = InBins("col1", bins=[0, 1, 1])
    assert criteria.column_name == "col1"
    assert criteria.bins == {0, 1}

    criteria = InBins(
        "col2",
        bins=0,
    )
    assert criteria.column_name == "col2"
    assert criteria.bins == {0}


def test_in_bins_criteria_special_bins():
    criteria = InBins("col1", bins=["none", "nan", "overflow", "underflow", 0])
    assert criteria.column_name == "col1"
    assert criteria.bins == {"none", "nan", "overflow", "underflow", 0}


def test_in_bins_criteria_wrong_column():
    with pytest.raises(ParameterValidationError) as e:
        InBins(column_name=123, bins=0)
    assert e.value.error_code == ErrorCode.PVDQOP14


def test_in_bins_criteria_wrong_bins():
    with pytest.raises(ParameterValidationError) as e:
        InBins(column_name="col1", bins=123)
    assert e.value.error_code == ErrorCode.PVDQOP14


def test_not_in_bins_criteria():
    criteria = NotInBins("col1", bins=[0, 1, 1])
    assert criteria.column_name == "col1"
    assert criteria.bins == {1, 0}

    criteria = NotInBins(
        "col2",
        bins=1,
    )
    assert criteria.column_name == "col2"
    assert criteria.bins == {1}


def test_not_in_bins_criteria_special_bins():
    criteria = NotInBins("col1", bins=["none", "nan", "overflow", "underflow", 0])
    assert criteria.column_name == "col1"
    assert criteria.bins == {"none", "nan", "overflow", "underflow", 0}


def test_not_in_bins_criteria_wrong_column():
    with pytest.raises(ParameterValidationError) as e:
        NotInBins(column_name=123, bins="b1")
    assert e.value.error_code == ErrorCode.PVDQOP15


def test_not_in_bins_criteria_wrong_bins():
    with pytest.raises(ParameterValidationError) as e:
        NotInBins(column_name="col1", bins=123)
    assert e.value.error_code == ErrorCode.PVDQOP15

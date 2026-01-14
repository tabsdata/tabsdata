#
# Copyright 2025 Tabs Data Inc.
#

import pytest

from tabsdata.dataquality._operator import (
    Criteria,
    Enrich,
    Fail,
    Filter,
    PercentThreshold,
    RowCountThreshold,
    Select,
    Summary,
)
from tabsdata.exceptions import (
    ConstructorError,
    ErrorCode,
    ParameterValidationError,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_criteria_all_ok_class():
    criteria = Criteria.all_ok()
    assert criteria.criteria_type == "all_ok"
    assert criteria.column_names is None
    assert criteria.bins is None


def test_criteria_any_failed_class():
    criteria = Criteria.any_failed()
    assert criteria.criteria_type == "any_failed"
    assert criteria.column_names is None
    assert criteria.bins is None


def test_criteria_by_bin_class():
    criteria = Criteria.in_bins("names", [1, 2, 3])
    assert criteria.criteria_type == "in_bins"
    assert criteria.column_names == ["names"]
    assert criteria.bins == [1, 2, 3]


def test_criteria_by_not_in_bin_class():
    criteria = Criteria.not_in_bins("names", [1, 2, 3])
    assert criteria.criteria_type == "not_in_bins"
    assert criteria.column_names == ["names"]
    assert criteria.bins == [1, 2, 3]


def test_criteria_wrong_instantiation():
    with pytest.raises(ConstructorError) as e:
        Criteria()
    assert e.value.error_code == ErrorCode.PVDQOP1


def test_criteria_wrong_bin_type():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        Criteria.in_bins("name", 3)
    assert e.value.error_code == ErrorCode.PVDQOP3


def test_criteria_in_bins_with_multiple_columns():
    criteria = Criteria.in_bins(["col1", "col2"], [1, 2, 3])
    assert criteria.criteria_type == "in_bins"
    assert criteria.column_names == ["col1", "col2"]
    assert criteria.bins == [1, 2, 3]


def test_criteria_not_in_bins_with_multiple_columns():
    criteria = Criteria.not_in_bins(["col1", "col2"], [1, 2, 3])
    assert criteria.criteria_type == "not_in_bins"
    assert criteria.column_names == ["col1", "col2"]
    assert criteria.bins == [1, 2, 3]


def test_criteria_in_bins_invalid_column_type():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        Criteria.in_bins(123, [1, 2, 3])
    assert e.value.error_code == ErrorCode.PVDQOP3


def test_criteria_not_in_bins_invalid_column_type():
    with pytest.raises(ParameterValidationError) as e:
        # noinspection PyTypeChecker
        Criteria.not_in_bins(456, [1, 2, 3])
    assert e.value.error_code == ErrorCode.PVDQOP4


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
    criteria = Criteria.any_failed()
    filter_out = Filter(
        to_table="table_name", criteria=criteria, enrich=True, tags=["tag1", "tag2"]
    )
    assert filter_out.to_table == "table_name"
    assert filter_out.criteria == criteria
    assert filter_out.enrich is True
    assert filter_out.tags == ["tag1", "tag2"]


def test_filter_out_class_defaults():
    filter_out = Filter()
    assert filter_out.to_table is None
    assert filter_out.criteria.criteria_type == "any_failed"
    assert filter_out.enrich is False
    assert filter_out.tags == None


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
    criteria = Criteria.any_failed()
    selector = Select(
        to_table="table_name", criteria=criteria, enrich=True, tags=["tag1", "tag2"]
    )
    assert selector.to_table == "table_name"
    assert selector.criteria == criteria
    assert selector.enrich is True
    assert selector.tags == ["tag1", "tag2"]


def test_select_class_defaults():
    selector = Select("table_name")
    assert selector.to_table == "table_name"
    assert selector.criteria.criteria_type == "all_ok"
    assert selector.enrich is False
    assert selector.tags == None


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
    criteria = Criteria.all_ok()
    thr = PercentThreshold(59)
    fail = Fail(threshold=thr, criteria=criteria, tags=["tag1", "tag2"])
    assert fail.threshold == thr
    assert fail.criteria == criteria
    assert fail.tags == ["tag1", "tag2"]


def test_fail_class_defaults():
    thr = PercentThreshold(59)
    fail = Fail(threshold=thr)
    assert fail.threshold == thr
    assert fail.criteria.criteria_type == "any_failed"
    assert fail.tags == None


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

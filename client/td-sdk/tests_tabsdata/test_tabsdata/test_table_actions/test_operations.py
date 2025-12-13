#
# Copyright 2025 Tabs Data Inc.
#

import pytest

from tabsdata.dataquality._operations import (
    Criteria,
    Fail,
    Filter,
    Select,
    Summary,
    Threshold,
)
from tabsdata.exceptions import DataQualityConfigurationError, ErrorCode

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_criteria_all_ok_class():
    criteria = Criteria.all_ok()
    assert criteria.criteria_type == "all_ok"
    assert criteria.column_names is None
    assert criteria.buckets is None


def test_criteria_any_failed_class():
    criteria = Criteria.any_failed()
    assert criteria.criteria_type == "any_failed"
    assert criteria.column_names is None
    assert criteria.buckets is None


def test_criteria_by_bucket_class():
    criteria = Criteria.in_buckets("names", [1, 2, 3])
    assert criteria.criteria_type == "in_buckets"
    assert criteria.column_names == ["names"]
    assert criteria.buckets == [1, 2, 3]


def test_criteria_by_not_in_bucket_class():
    criteria = Criteria.not_in_buckets("names", [1, 2, 3])
    assert criteria.criteria_type == "not_in_buckets"
    assert criteria.column_names == ["names"]
    assert criteria.buckets == [1, 2, 3]


def test_criteria_wrong_instantiation():
    with pytest.raises(DataQualityConfigurationError) as e:
        Criteria()
    assert e.value.error_code == ErrorCode.DQCE58


def test_criteria_wrong_bucket_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Criteria.in_buckets("name", 3)
    assert e.value.error_code == ErrorCode.DQCE61


def test_criteria_in_buckets_with_multiple_columns():
    criteria = Criteria.in_buckets(["col1", "col2"], [1, 2, 3])
    assert criteria.criteria_type == "in_buckets"
    assert criteria.column_names == ["col1", "col2"]
    assert criteria.buckets == [1, 2, 3]


def test_criteria_not_in_buckets_with_multiple_columns():
    criteria = Criteria.not_in_buckets(["col1", "col2"], [1, 2, 3])
    assert criteria.criteria_type == "not_in_buckets"
    assert criteria.column_names == ["col1", "col2"]
    assert criteria.buckets == [1, 2, 3]


def test_criteria_in_buckets_invalid_column_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Criteria.in_buckets(123, [1, 2, 3])
    assert e.value.error_code == ErrorCode.DQCE60


def test_criteria_not_in_buckets_invalid_column_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Criteria.not_in_buckets(456, [1, 2, 3])
    assert e.value.error_code == ErrorCode.DQCE60


def test_criteria_in_buckets_with_string_buckets():
    criteria = Criteria.in_buckets("status", ["ACTIVE", "PENDING", "FAILED"])
    assert criteria.criteria_type == "in_buckets"
    assert criteria.column_names == ["status"]
    assert criteria.buckets == ["ACTIVE", "PENDING", "FAILED"]


def test_criteria_not_in_buckets_with_string_buckets():
    criteria = Criteria.not_in_buckets("status", ["INACTIVE", "DELETED"])
    assert criteria.criteria_type == "not_in_buckets"
    assert criteria.column_names == ["status"]
    assert criteria.buckets == ["INACTIVE", "DELETED"]


def test_summary_class():
    summary = Summary(table="table_name", tags=["tag1", "tag2"])
    assert summary.table == "table_name"
    assert summary.tags == ["tag1", "tag2"]


def test_summary_class_wrong_table():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Summary(table=3)
    assert e.value.error_code == ErrorCode.DQCE46


def test_summary_class_wrong_tags():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Summary(tags=3)
    assert e.value.error_code == ErrorCode.DQCE45


def test_summary_class_wrong_tags_list():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Summary(tags=[3])
    assert e.value.error_code == ErrorCode.DQCE44


def test_filter_out_class():
    criteria = Criteria.any_failed()
    filter_out = Filter(to_table="table_name", criteria=criteria, tags=["tag1", "tag2"])
    assert filter_out.to_table == "table_name"
    assert filter_out.criteria == criteria
    assert filter_out.tags == ["tag1", "tag2"]


def test_filter_out_class_defaults():
    filter_out = Filter()
    assert filter_out.to_table is None
    assert filter_out.criteria.criteria_type == "any_failed"
    assert filter_out.tags == []


def test_filter_out_wrong_criteria_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Filter(criteria="wrong_type")
        assert e.value.error_code == ErrorCode.DQCE63


def test_filter_out_wrong_to_table_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Filter(to_table=3)
    assert e.value.error_code == ErrorCode.DQCE62


def test_select_class():
    criteria = Criteria.any_failed()
    selector = Select(to_table="table_name", criteria=criteria, tags=["tag1", "tag2"])
    assert selector.to_table == "table_name"
    assert selector.criteria == criteria
    assert selector.tags == ["tag1", "tag2"]


def test_select_class_defaults():
    selector = Select("table_name")
    assert selector.to_table == "table_name"
    assert selector.criteria.criteria_type == "all_ok"
    assert selector.tags == []


def test_selector_wrong_criteria_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Select("table_name", criteria="wrong_type")
        assert e.value.error_code == ErrorCode.DQCE65


def test_selector_wrong_to_table_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Select(to_table=3)
    assert e.value.error_code == ErrorCode.DQCE64


def test_threshold_count_class():
    thr = Threshold.count(3)
    assert thr.absolute == 3
    assert thr.relative is None


def test_threshold_one_class():
    thr = Threshold.one()
    assert thr.absolute == 1
    assert thr.relative is None


def test_threshold_percent_class():
    thr = Threshold.percent(59)
    assert thr.absolute is None
    assert thr.relative == float(59)


def test_threshold_wrong_instantiation():
    with pytest.raises(DataQualityConfigurationError) as e:
        Threshold()
    assert e.value.error_code == ErrorCode.DQCE67


def test_threshold_count_wrong_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Threshold.count("wrong_type")
    assert e.value.error_code == ErrorCode.DQCE68


def test_threshold_count_wrong_value():
    with pytest.raises(DataQualityConfigurationError) as e:
        Threshold.count(-3)
    assert e.value.error_code == ErrorCode.DQCE69


def test_threshold_count_none():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Threshold.count(None)
    assert e.value.error_code == ErrorCode.DQCE70


def test_threshold_percent_wrong_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Threshold.percent("wrong_type")
    assert e.value.error_code == ErrorCode.DQCE73


def test_threshold_percent_wrong_value():
    with pytest.raises(DataQualityConfigurationError) as e:
        Threshold.percent(200)
    assert e.value.error_code == ErrorCode.DQCE72


def test_threshold_percent_none():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Threshold.percent(None)
    assert e.value.error_code == ErrorCode.DQCE71


def test_fail_class():
    criteria = Criteria.all_ok()
    thr = Threshold.percent(59)
    fail = Fail(threshold=thr, criteria=criteria, tags=["tag1", "tag2"])
    assert fail.threshold == thr
    assert fail.criteria == criteria
    assert fail.tags == ["tag1", "tag2"]


def test_fail_class_defaults():
    thr = Threshold.percent(59)
    fail = Fail(threshold=thr)
    assert fail.threshold == thr
    assert fail.criteria.criteria_type == "any_failed"
    assert fail.tags == []


def test_fail_class_wrong_threshold_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        # noinspection PyTypeChecker
        Fail(threshold="wrong_type")
    assert e.value.error_code == ErrorCode.DQCE74


def test_fail_class_wrong_criteria_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        thr = Threshold.percent(59)
        # noinspection PyTypeChecker
        Fail(threshold=thr, criteria="wrong_type")
    assert e.value.error_code == ErrorCode.DQCE66

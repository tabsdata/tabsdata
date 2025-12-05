#
# Copyright 2025 Tabs Data Inc.
#

import pytest

from tabsdata.dataquality._classifiers import Matches
from tabsdata.dataquality._data_quality import DataQuality
from tabsdata.dataquality._operations import Fail, Summary, Threshold
from tabsdata.exceptions import DataQualityConfigurationError, ErrorCode

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_data_quality_class():
    classifier = Matches(regex="hi")
    table = "my_table"
    thr = Threshold.one()
    operation = Fail(thr)
    dq = DataQuality(table, classifier, operation)
    assert dq.table == table
    assert dq.classify == [classifier]
    assert dq.operations == [operation]


def test_data_quality_class_defaults():
    classifier = Matches(regex="hi")
    table = "my_table"
    dq = DataQuality(table, classifier)
    assert dq.table == table
    assert dq.classify == [classifier]
    assert len(dq.operations) == 1
    assert isinstance(dq.operations[0], Summary)


def test_data_quality_wrong_table_type():
    classifier = Matches(regex="hi")
    with pytest.raises(DataQualityConfigurationError) as e:
        DataQuality(3, classifier)
    assert e.value.error_code == ErrorCode.DQCE75


def test_data_quality_wrong_classifier_type():
    table = "my_table"
    with pytest.raises(DataQualityConfigurationError) as e:
        DataQuality(table, 3)
    assert e.value.error_code == ErrorCode.DQCE76


def test_data_quality_wrong_classifier_list_type():
    table = "my_table"
    with pytest.raises(DataQualityConfigurationError) as e:
        DataQuality(table, [3])
    assert e.value.error_code == ErrorCode.DQCE77


def test_data_quality_wrong_operation_type():
    table = "my_table"
    classifier = Matches(regex="hi")
    with pytest.raises(DataQualityConfigurationError) as e:
        DataQuality(table, classifier, 3)
    assert e.value.error_code == ErrorCode.DQCE78


def test_data_quality_wrong_operation_list_type():
    table = "my_table"
    classifier = Matches(regex="hi")
    with pytest.raises(DataQualityConfigurationError) as e:
        DataQuality(table, classifier, [3])
    assert e.value.error_code == ErrorCode.DQCE79

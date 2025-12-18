#
# Copyright 2025 Tabs Data Inc.
#

import pytest

from tabsdata.dataquality._classifiers import Matches
from tabsdata.dataquality._data_quality import DataQuality
from tabsdata.dataquality._operations import Fail, RowCountThreshold, Summary
from tabsdata.exceptions import (
    DataQualityConfigurationError,
    ErrorCode,
    ParameterValidationError,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_data_quality_class():
    classifier = Matches(regex="hi")
    table = "my_table"
    thr = RowCountThreshold(1)
    operation = Fail(thr)
    dq = DataQuality(table, classifier, operation)
    assert dq.table == table
    assert dq.classifiers == [classifier]
    assert dq.operations == [operation]


def test_data_quality_wrong_table_type():
    classifier = Matches(regex="hi")
    with pytest.raises(ParameterValidationError) as e:
        DataQuality(3, classifier, Summary())
    assert e.value.error_code == ErrorCode.PVDQ1


def test_data_quality_wrong_classifier_type():
    table = "my_table"
    with pytest.raises(ParameterValidationError) as e:
        DataQuality(table, 3, Summary())
    assert e.value.error_code == ErrorCode.PVDQ1


def test_data_quality_wrong_classifier_list_type():
    table = "my_table"
    with pytest.raises(ParameterValidationError) as e:
        DataQuality(table, [3], Summary())
    assert e.value.error_code == ErrorCode.PVDQ1


def test_data_quality_wrong_operation_type():
    table = "my_table"
    classifier = Matches(regex="hi")
    with pytest.raises(ParameterValidationError) as e:
        DataQuality(table, classifier, 3)
    assert e.value.error_code == ErrorCode.PVDQ1


def test_data_quality_wrong_operation_list_type():
    table = "my_table"
    classifier = Matches(regex="hi")
    with pytest.raises(ParameterValidationError) as e:
        DataQuality(table, classifier, [3])
    assert e.value.error_code == ErrorCode.PVDQ1

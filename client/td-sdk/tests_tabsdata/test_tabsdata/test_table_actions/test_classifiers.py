#
# Copyright 2025 Tabs Data Inc.
#

import pytest

import tabsdata.tableframe.dtypes as td_dtypes
from tabsdata._table_actions.classifiers import (
    DoesNotMatch,
    HasColumns,
    HasColumnTypes,
    HasLen,
    IsIn,
    IsInRange,
    IsNan,
    IsNegative,
    IsNegativeOrZero,
    IsNotIn,
    IsNotNan,
    IsNotNull,
    IsNotNullNorNan,
    IsNotZero,
    IsNull,
    IsNullOrNan,
    IsPositive,
    IsPositiveOrZero,
    IsZero,
    Matches,
)
from tabsdata.exceptions import DataQualityConfigurationError, ErrorCode

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_does_not_match_classifier():
    classifier = DoesNotMatch(cols="column_a", regex="^test_.*", tags="tag1")
    assert classifier.cols == ["column_a"]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == ["tag1"]
    assert not classifier.ignore_nulls


def test_does_not_match_classifier_multiple_tags():
    classifier = DoesNotMatch(
        cols=["column_a", "column_b"], regex="^test_.*", tags=["tag1", "tag2"]
    )
    assert classifier.cols == ["column_a", "column_b"]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == ["tag1", "tag2"]
    assert not classifier.ignore_nulls


def test_does_not_match_classifier_ignore_nulls():
    classifier = DoesNotMatch(cols="column_a", regex="^test_.*", ignore_nulls=True)
    assert classifier.cols == ["column_a"]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == []
    assert classifier.ignore_nulls


def test_does_not_match_classifier_no_tags():
    classifier = DoesNotMatch(cols="column_a", regex="^test_.*")
    assert classifier.cols == ["column_a"]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == []
    assert not classifier.ignore_nulls


def test_does_not_match_classifier_no_columns():
    classifier = DoesNotMatch(regex="^test_.*")
    assert classifier.cols is None
    assert classifier.regex == "^test_.*"
    assert classifier.tags == []
    assert not classifier.ignore_nulls


def test_does_not_match_classifier_column_tuple():
    classifier = DoesNotMatch(
        cols=("column_a", "column_b"), regex="^test_.*", tags="tag1"
    )
    assert classifier.cols == [("column_a", "column_b")]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == ["tag1"]
    assert not classifier.ignore_nulls


def test_does_not_match_classifier_multiple_column_tuples():
    classifier = DoesNotMatch(
        cols=[("column_a", "column_b"), ("column_c", "column_d")],
        regex="^test_.*",
        tags="tag1",
    )
    assert classifier.cols == [("column_a", "column_b"), ("column_c", "column_d")]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == ["tag1"]
    assert not classifier.ignore_nulls


def test_wrong_cols_type_raises_error():
    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(cols=123, regex="^test_.*")
    assert e.value.error_code == ErrorCode.DQCE9


def test_wrong_cols_tuple_raises_error():
    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(cols=("1", "2", "3"), regex="^test_.*")
    assert e.value.error_code == ErrorCode.DQCE3

    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(cols=("1", 3), regex="^test_.*")
    assert e.value.error_code == ErrorCode.DQCE4

    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(cols=(1, "3"), regex="^test_.*")
    assert e.value.error_code == ErrorCode.DQCE4


def test_wrong_cols_list_raises_error():
    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(cols=[123], regex="^test_.*")
    assert e.value.error_code == ErrorCode.DQCE7

    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(cols=[("1", 2)], regex="^test_.*")
    assert e.value.error_code == ErrorCode.DQCE6

    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(cols=[(1, "2")], regex="^test_.*")
    assert e.value.error_code == ErrorCode.DQCE6

    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(cols=[("1", "2", "3")], regex="^test_.*")
    assert e.value.error_code == ErrorCode.DQCE5

    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(cols=[123, "valid_column"], regex="^test_.*")
    assert e.value.error_code == ErrorCode.DQCE7

    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(
            cols=[("valid_col1", "valid_col2"), ("invalid_col", 3)], regex="^test_.*"
        )
    assert e.value.error_code == ErrorCode.DQCE6

    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(
            cols=[("valid_col1", "valid_col2"), "valid_col3"], regex="^test_.*"
        )
    assert e.value.error_code == ErrorCode.DQCE8

    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(
            cols=["hi", ("valid_col1", "valid_col2"), "valid_col3"], regex="^test_.*"
        )
    assert e.value.error_code == ErrorCode.DQCE8


def test_wrong_tags_type_raises_error():
    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(cols="column_a", regex="^test_.*", tags=123)
    assert e.value.error_code == ErrorCode.DQCE2

    with pytest.raises(DataQualityConfigurationError) as e:
        DoesNotMatch(cols="column_a", regex="^test_.*", tags=[123])
    assert e.value.error_code == ErrorCode.DQCE1


def test_has_columns_classifier():
    classifier = HasColumns(cols=["column_a", "column_b"], tags="tag1")
    assert classifier.cols == ["column_a", "column_b"]
    assert classifier.tags == ["tag1"]
    assert classifier.mode == "at_least"


def test_has_columns_classifier_single_column():
    classifier = HasColumns(cols="column_a", tags="tag1")
    assert classifier.cols == ["column_a"]
    assert classifier.tags == ["tag1"]
    assert classifier.mode == "at_least"


def test_has_columns_classifier_exact_mode():
    classifier = HasColumns(cols=["column_a", "column_b"], tags="tag1", mode="exact")
    assert classifier.cols == ["column_a", "column_b"]
    assert classifier.tags == ["tag1"]
    assert classifier.mode == "exact"


def test_has_columns_classifier_wrong_mode_raises_error():
    with pytest.raises(DataQualityConfigurationError) as e:
        HasColumns(cols=["column_a", "column_b"], mode="invalid_mode")
    assert e.value.error_code == ErrorCode.DQCE35


def test_has_columns_classifier_wrong_cols_type_raises_error():
    with pytest.raises(DataQualityConfigurationError) as e:
        HasColumns(cols=123, tags="tag1")
    assert e.value.error_code == ErrorCode.DQCE34

    with pytest.raises(DataQualityConfigurationError) as e:
        HasColumns(cols=[123, "column_b"], tags="tag1")
    assert e.value.error_code == ErrorCode.DQCE33


def test_has_column_types_classifier():
    classifier = HasColumnTypes(
        cols=[
            (
                "column_a",
                td_dtypes.Boolean,
            ),
            (
                "column_b",
                td_dtypes.String,
            ),
        ],
        tags="tag1",
    )
    assert classifier.cols == [
        (
            "column_a",
            td_dtypes.Boolean,
        ),
        (
            "column_b",
            td_dtypes.String,
        ),
    ]
    assert classifier.tags == ["tag1"]


def test_has_column_types_classifier_3_elements():
    classifier = HasColumnTypes(
        cols=[
            (
                "column_a",
                td_dtypes.Boolean,
            ),
            (
                "column_b",
                td_dtypes.String,
            ),
            (
                "column_b",
                td_dtypes.String,
            ),
        ],
        tags="tag1",
    )
    assert classifier.cols == [
        (
            "column_a",
            td_dtypes.Boolean,
        ),
        (
            "column_b",
            td_dtypes.String,
        ),
        (
            "column_b",
            td_dtypes.String,
        ),
    ]
    assert classifier.tags == ["tag1"]


def test_has_column_types_classifier_single_column():
    classifier = HasColumnTypes(
        cols=(
            "column_a",
            td_dtypes.Boolean,
        ),
        tags="tag1",
    )
    assert classifier.cols == [
        (
            "column_a",
            td_dtypes.Boolean,
        )
    ]
    assert classifier.tags == ["tag1"]


def test_has_column_types_classifier_wrong_cols_type_raises_error():
    with pytest.raises(DataQualityConfigurationError) as e:
        HasColumnTypes(cols=123)
    assert e.value.error_code == ErrorCode.DQCE42

    with pytest.raises(DataQualityConfigurationError) as e:
        HasColumnTypes(cols=[("column_a",)])
    assert e.value.error_code == ErrorCode.DQCE39

    with pytest.raises(DataQualityConfigurationError) as e:
        HasColumnTypes(cols=[("column_a", "not_a_dtype")])
    assert e.value.error_code == ErrorCode.DQCE41

    with pytest.raises(DataQualityConfigurationError) as e:
        HasColumnTypes(
            cols=[
                123,
                (
                    "column_b",
                    td_dtypes.String,
                ),
            ]
        )
    assert e.value.error_code == ErrorCode.DQCE38

    with pytest.raises(DataQualityConfigurationError) as e:
        HasColumnTypes(cols=[("column_a", td_dtypes.Boolean, "hi")])
    assert e.value.error_code == ErrorCode.DQCE39

    with pytest.raises(DataQualityConfigurationError) as e:
        HasColumnTypes(cols=[(3, td_dtypes.Boolean)])
    assert e.value.error_code == ErrorCode.DQCE40

    with pytest.raises(DataQualityConfigurationError) as e:
        HasColumnTypes(cols=[("hi", 3)])
    assert e.value.error_code == ErrorCode.DQCE41


def test_has_column_types_classifier_wrong_cols_tuple_raises_error():
    with pytest.raises(DataQualityConfigurationError) as e:
        HasColumnTypes(cols=("column_a",))
    assert e.value.error_code == ErrorCode.DQCE43

    with pytest.raises(DataQualityConfigurationError) as e:
        HasColumnTypes(cols=("column_a", "not_a_dtype"))
    assert e.value.error_code == ErrorCode.DQCE37

    with pytest.raises(DataQualityConfigurationError) as e:
        HasColumnTypes(cols=(3, td_dtypes.Boolean))
    assert e.value.error_code == ErrorCode.DQCE36


def test_has_len_classifier():
    classifier = HasLen(min=5, max=10)
    assert classifier.min == 5
    assert classifier.max == 10


def test_has_len_classifier_only_min():
    classifier = HasLen(min=5)
    assert classifier.min == 5
    assert classifier.max == float("inf")


def test_has_len_classifier_only_max():
    classifier = HasLen(max=10)
    assert classifier.min == float("-inf")
    assert classifier.max == 10


def test_is_in_classifier():
    classifier = IsIn(set=[1, 2, 3])
    assert classifier.set == [1, 2, 3]


def test_is_in_classifier_wrong_set_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        IsIn(set=3)
    assert e.value.error_code == ErrorCode.DQCE30


def test_is_in_range_classifier():
    classifier = IsInRange(min=5, max=10)
    assert classifier.min == 5
    assert classifier.max == 10
    assert classifier.closed == "both"


def test_is_in_range_classifier_valid_closed_options():
    for closed_option in IsInRange.VALID_CLOSED_OPTIONS:
        classifier = IsInRange(min=5, max=10, closed=closed_option)
        assert classifier.closed == closed_option


def test_is_in_range_classifier_wrong_closed_option():
    with pytest.raises(DataQualityConfigurationError) as e:
        IsInRange(min=5, max=10, closed="invalid_option")
    assert e.value.error_code == ErrorCode.DQCE27


def test_is_in_range_classifier_no_values():
    classifier = IsInRange()
    assert classifier.min == float("-inf")
    assert classifier.max == float("inf")


def test_is_in_range_classifier_wrong_min_max_types():
    with pytest.raises(DataQualityConfigurationError) as e:
        IsInRange(min="5", max=10)
    assert e.value.error_code == ErrorCode.DQCE25

    with pytest.raises(DataQualityConfigurationError) as e:
        IsInRange(min=5, max="10")
    assert e.value.error_code == ErrorCode.DQCE26


def test_is_nan_classifier():
    classifier = IsNan()
    assert classifier.cols is None
    assert classifier.tags == []
    assert classifier.ignore_nulls is False


def test_is_negative_classifier():
    classifier = IsNegative(cols="column_a", tags=["tag1", "tag2"])
    assert classifier.cols == ["column_a"]
    assert classifier.tags == ["tag1", "tag2"]
    assert classifier.ignore_nulls is False


def test_is_negative_or_zero_classifier():
    classifier = IsNegativeOrZero(cols=["column_a", "column_b"], tags="tag1")
    assert classifier.cols == ["column_a", "column_b"]
    assert classifier.tags == ["tag1"]
    assert classifier.ignore_nulls is False


def test_is_not_in_classifier():
    classifier = IsNotIn(set=["a", "b", "c"])
    assert classifier.set == ["a", "b", "c"]


def test_is_not_in_classifier_wrong_set_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        IsNotIn(set=3)
    assert e.value.error_code == ErrorCode.DQCE32


def test_is_not_nan_classifier():
    classifier = IsNotNan(cols="column_a", tags="tag1", ignore_nulls=True)
    assert classifier.cols == ["column_a"]
    assert classifier.tags == ["tag1"]
    assert classifier.ignore_nulls is True


def test_is_not_null_classifier():
    classifier = IsNotNull(cols=["column_a", "column_b"], tags=["tag1", "tag2"])
    assert classifier.cols == ["column_a", "column_b"]
    assert classifier.tags == ["tag1", "tag2"]


def test_is_not_null_nor_nan_classifier():
    classifier = IsNotNullNorNan(cols="column_a")
    assert classifier.cols == ["column_a"]
    assert classifier.tags == []


def test_is_not_zero_classifier():
    classifier = IsNotZero(cols=["column_a"], tags="tag1")
    assert classifier.cols == ["column_a"]
    assert classifier.tags == ["tag1"]
    assert classifier.ignore_nulls is False


def test_is_null_classifier():
    classifier = IsNull(cols="column_a", tags=["tag1", "tag2"])
    assert classifier.cols == ["column_a"]
    assert classifier.tags == ["tag1", "tag2"]


def test_is_null_or_nan_classifier():
    classifier = IsNullOrNan(cols=["column_a", "column_b"], tags="tag1")
    assert classifier.cols == ["column_a", "column_b"]
    assert classifier.tags == ["tag1"]


def test_is_positive_classifier():
    classifier = IsPositive(cols="column_a", tags="tag1")
    assert classifier.cols == ["column_a"]
    assert classifier.tags == ["tag1"]
    assert classifier.ignore_nulls is False


def test_is_positive_or_zero_classifier():
    classifier = IsPositiveOrZero(cols=["column_a", "column_b"], tags=["tag1", "tag2"])
    assert classifier.cols == ["column_a", "column_b"]
    assert classifier.tags == ["tag1", "tag2"]
    assert classifier.ignore_nulls is False


def test_is_positive_or_zero_classifier_wrong_ignore_nulls_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        IsPositiveOrZero(cols=["column_a", "column_b"], ignore_nulls="not_a_bool")
    assert e.value.error_code == ErrorCode.DQCE15


def test_is_zero_classifier():
    classifier = IsZero(cols="column_a", tags="tag1")
    assert classifier.cols == ["column_a"]
    assert classifier.tags == ["tag1"]
    assert classifier.ignore_nulls is False


def test_matches_classifier():
    classifier = Matches(cols="column_a", regex="^test_.*", tags="tag1")
    assert classifier.cols == ["column_a"]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == ["tag1"]
    assert not classifier.ignore_nulls


def test_matches_classifier_no_tags():
    classifier = Matches(cols="column_a", regex="^test_.*")
    assert classifier.cols == ["column_a"]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == []
    assert not classifier.ignore_nulls


def test_matches_classifier_ignore_nulls():
    classifier = Matches(cols="column_a", regex="^test_.*", ignore_nulls=True)
    assert classifier.cols == ["column_a"]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == []
    assert classifier.ignore_nulls


def test_matches_classifier_no_columns():
    classifier = Matches(regex="^test_.*")
    assert classifier.cols is None
    assert classifier.regex == "^test_.*"
    assert classifier.tags == []
    assert not classifier.ignore_nulls


def test_matches_classifier_wrong_regex_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        Matches(cols="column_a", regex=123)
    assert e.value.error_code == ErrorCode.DQCE19


def test_matches_classifier_wrong_ignore_nulls_type():
    with pytest.raises(DataQualityConfigurationError) as e:
        Matches(cols="column_a", regex="^test_.*", ignore_nulls="not_a_bool")
    assert e.value.error_code == ErrorCode.DQCE18

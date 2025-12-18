#
# Copyright 2025 Tabs Data Inc.
#
import math
import sys
from typing import get_args

import pytest

import tabsdata.tableframe.dtypes as td_dtypes
from tabsdata.dataquality._classifiers import (
    Categorizer,
    DoesNotMatch,
    ExponentialScale,
    HasColumns,
    HasColumnTypes,
    HasLen,
    IdentityScale,
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
    LinearScale,
    LogarithmicScale,
    Matches,
    MonomialScale,
    RangeClosedOnSpec,
    Scale,
)
from tabsdata.exceptions import (
    ConstructorError,
    ErrorCode,
    ParameterValidationError,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_does_not_match_classifier():
    classifier = DoesNotMatch(column_names="column_a", regex="^test_.*", tags="tag1")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == ["tag1"]
    assert not classifier.ignore_nulls


def test_does_not_match_classifier_multiple_tags():
    classifier = DoesNotMatch(
        column_names=["column_a", "column_b"], regex="^test_.*", tags=["tag1", "tag2"]
    )
    assert classifier.column_names == [("column_a", None), ("column_b", None)]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == ["tag1", "tag2"]
    assert not classifier.ignore_nulls


def test_does_not_match_classifier_ignore_nulls():
    classifier = DoesNotMatch(
        column_names="column_a", regex="^test_.*", ignore_nulls=True
    )
    assert classifier.column_names == [("column_a", None)]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == []
    assert classifier.ignore_nulls


def test_does_not_match_classifier_no_tags():
    classifier = DoesNotMatch(column_names="column_a", regex="^test_.*")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == []
    assert not classifier.ignore_nulls


def test_does_not_match_classifier_no_columns():
    classifier = DoesNotMatch(regex="^test_.*")
    assert classifier.column_names is None
    assert classifier.regex == "^test_.*"
    assert classifier.tags == []
    assert not classifier.ignore_nulls


def test_does_not_match_classifier_column_tuple():
    classifier = DoesNotMatch(
        column_names=("column_a", "column_b"), regex="^test_.*", tags="tag1"
    )
    assert classifier.column_names == [("column_a", "column_b")]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == ["tag1"]
    assert not classifier.ignore_nulls


def test_does_not_match_classifier_multiple_column_tuples():
    classifier = DoesNotMatch(
        column_names=[("column_a", "column_b"), ("column_c", "column_d")],
        regex="^test_.*",
        tags="tag1",
    )
    assert classifier.column_names == [
        ("column_a", "column_b"),
        ("column_c", "column_d"),
    ]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == ["tag1"]
    assert not classifier.ignore_nulls


def test_wrong_cols_type_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=123, regex="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3


def test_wrong_cols_tuple_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=("1", "2", "3"), regex="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=("1", 3), regex="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=(1, "3"), regex="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3


def test_wrong_cols_list_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[123], regex="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[("1", 2)], regex="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[(1, "2")], regex="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[("1", "2", "3")], regex="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[123, "valid_column"], regex="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(
            column_names=[("valid_col1", "valid_col2"), ("invalid_col", 3)],
            regex="^test_.*",
        )
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(
            column_names=[("valid_col1", "valid_col2"), "valid_col3"], regex="^test_.*"
        )
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(
            column_names=["hi", ("valid_col1", "valid_col2"), "valid_col3"],
            regex="^test_.*",
        )
    assert e.value.error_code == ErrorCode.PVDQCL3


def test_wrong_tags_type_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names="column_a", regex="^test_.*", tags=123)
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names="column_a", regex="^test_.*", tags=[123])
    assert e.value.error_code == ErrorCode.PVDQCL3


def test_has_columns_classifier():
    classifier = HasColumns(column_names=["column_a", "column_b"], tags="tag1")
    assert classifier.column_names == [("column_a", None), ("column_b", None)]
    assert classifier.tags == ["tag1"]
    assert classifier.mode == "at_least"


def test_has_columns_classifier_single_column():
    classifier = HasColumns(column_names="column_a", tags="tag1")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.tags == ["tag1"]
    assert classifier.mode == "at_least"


def test_has_columns_classifier_exact_mode():
    classifier = HasColumns(
        column_names=["column_a", "column_b"], tags="tag1", mode="exact"
    )
    assert classifier.column_names == [("column_a", None), ("column_b", None)]
    assert classifier.tags == ["tag1"]
    assert classifier.mode == "exact"


def test_has_columns_classifier_wrong_mode_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        HasColumns(column_names=["column_a", "column_b"], mode="invalid_mode")
    assert e.value.error_code == ErrorCode.PVDQCL4


def test_has_columns_classifier_wrong_cols_type_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        HasColumns(column_names=123, tags="tag1")
    assert e.value.error_code == ErrorCode.PVDQCL4

    with pytest.raises(ParameterValidationError) as e:
        HasColumns(column_names=[123, "column_b"], tags="tag1")
    assert e.value.error_code == ErrorCode.PVDQCL4


def test_has_column_types_classifier():
    classifier = HasColumnTypes(
        column_types=[
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
    assert classifier.column_names == [
        ("column_a", None),
        ("column_b", None),
    ]
    assert classifier.column_types == [
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
        column_types=[
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
    assert classifier.column_types == [
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
        column_types=(
            "column_a",
            td_dtypes.Boolean,
        ),
        tags="tag1",
    )
    assert classifier.column_types == [
        (
            "column_a",
            td_dtypes.Boolean,
        )
    ]
    assert classifier.tags == ["tag1"]


def test_has_column_types_classifier_wrong_cols_type_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        HasColumnTypes(column_types=123)
    assert e.value.error_code == ErrorCode.PVDQCL5

    with pytest.raises(ParameterValidationError) as e:
        HasColumnTypes(column_types=[("column_a",)])
    assert e.value.error_code == ErrorCode.PVDQCL5

    with pytest.raises(ParameterValidationError) as e:
        HasColumnTypes(column_types=[("column_a", "not_a_dtype")])
    assert e.value.error_code == ErrorCode.PVDQCL6

    with pytest.raises(ParameterValidationError) as e:
        HasColumnTypes(
            column_types=[
                123,
                (
                    "column_b",
                    td_dtypes.String,
                ),
            ]
        )
    assert e.value.error_code == ErrorCode.PVDQCL5

    with pytest.raises(ParameterValidationError) as e:
        HasColumnTypes(column_types=[("column_a", td_dtypes.Boolean, "hi")])
    assert e.value.error_code == ErrorCode.PVDQCL5

    with pytest.raises(ParameterValidationError) as e:
        HasColumnTypes(column_types=[(3, td_dtypes.Boolean)])
    assert e.value.error_code == ErrorCode.PVDQCL5

    with pytest.raises(ParameterValidationError) as e:
        HasColumnTypes(column_types=[("hi", 3)])
    assert e.value.error_code == ErrorCode.PVDQCL6


def test_has_column_types_classifier_wrong_cols_tuple_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        HasColumnTypes(column_types=("column_a",))
    assert e.value.error_code == ErrorCode.PVDQCL5

    with pytest.raises(ParameterValidationError) as e:
        HasColumnTypes(column_types=("column_a", "not_a_dtype"))
    assert e.value.error_code == ErrorCode.PVDQCL6

    with pytest.raises(ParameterValidationError) as e:
        HasColumnTypes(column_types=(3, td_dtypes.Boolean))
    assert e.value.error_code == ErrorCode.PVDQCL5


def test_has_len_classifier():
    classifier = HasLen(min=5, max=10)
    assert classifier.min == 5
    assert classifier.max == 10


def test_has_len_classifier_only_min():
    classifier = HasLen(min=5)
    assert classifier.min == 5
    assert classifier.max == sys.maxsize


def test_has_len_classifier_only_max():
    classifier = HasLen(max=10)
    assert classifier.min == 0
    assert classifier.max == 10


def test_is_in_classifier():
    classifier = IsIn(set=[1, 2, 3])
    assert classifier.set == [1, 2, 3]


def test_is_in_classifier_wrong_set_type():
    with pytest.raises(ParameterValidationError) as e:
        IsIn(set=3)
    assert e.value.error_code == ErrorCode.PVDQCL9


def test_is_in_range_classifier():
    classifier = IsInRange(min=5, max=10)
    assert classifier.min == 5
    assert classifier.max == 10
    assert classifier.closed == "both"


def test_is_in_range_classifier_valid_closed_options():
    valid_closed_options = get_args(RangeClosedOnSpec)
    for closed_option in valid_closed_options:
        classifier = IsInRange(min=5, max=10, closed=closed_option)
        assert classifier.closed == closed_option


def test_is_in_range_classifier_wrong_closed_option():
    with pytest.raises(ParameterValidationError) as e:
        IsInRange(min=5, max=10, closed="invalid_option")
    assert e.value.error_code == ErrorCode.PVDQCL10


def test_is_in_range_classifier_no_values():
    classifier = IsInRange()
    assert classifier.min == float("-inf")
    assert classifier.max == float("inf")


def test_is_in_range_classifier_wrong_min_max_types():
    with pytest.raises(ParameterValidationError) as e:
        IsInRange(min="5", max=10)
    assert e.value.error_code == ErrorCode.PVDQCL10

    with pytest.raises(ParameterValidationError) as e:
        IsInRange(min=5, max="10")
    assert e.value.error_code == ErrorCode.PVDQCL10


def test_is_nan_classifier():
    classifier = IsNan()
    assert classifier.column_names is None
    assert classifier.tags == []
    assert classifier.ignore_nulls is False


def test_is_negative_classifier():
    classifier = IsNegative(column_names="column_a", tags=["tag1", "tag2"])
    assert classifier.column_names == [("column_a", None)]
    assert classifier.tags == ["tag1", "tag2"]
    assert classifier.ignore_nulls is False


def test_is_negative_or_zero_classifier():
    classifier = IsNegativeOrZero(column_names=["column_a", "column_b"], tags="tag1")
    assert classifier.column_names == [("column_a", None), ("column_b", None)]
    assert classifier.tags == ["tag1"]
    assert classifier.ignore_nulls is False


def test_is_not_in_classifier():
    classifier = IsNotIn(set=["a", "b", "c"])
    assert classifier.set == ["a", "b", "c"]


def test_is_not_in_classifier_wrong_set_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNotIn(set=3)
    assert e.value.error_code == ErrorCode.PVDQCL10


def test_is_not_nan_classifier():
    classifier = IsNotNan(column_names="column_a", tags="tag1", ignore_nulls=True)
    assert classifier.column_names == [("column_a", None)]
    assert classifier.tags == ["tag1"]
    assert classifier.ignore_nulls is True


def test_is_not_null_classifier():
    classifier = IsNotNull(column_names=["column_a", "column_b"], tags=["tag1", "tag2"])
    assert classifier.column_names == [("column_a", None), ("column_b", None)]
    assert classifier.tags == ["tag1", "tag2"]


def test_is_not_null_nor_nan_classifier():
    classifier = IsNotNullNorNan(column_names="column_a")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.tags == []


def test_is_not_zero_classifier():
    classifier = IsNotZero(column_names=["column_a"], tags="tag1")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.tags == ["tag1"]
    assert classifier.ignore_nulls is False


def test_is_null_classifier():
    classifier = IsNull(column_names="column_a", tags=["tag1", "tag2"])
    assert classifier.column_names == [("column_a", None)]
    assert classifier.tags == ["tag1", "tag2"]


def test_is_null_or_nan_classifier():
    classifier = IsNullOrNan(column_names=["column_a", "column_b"], tags="tag1")
    assert classifier.column_names == [("column_a", None), ("column_b", None)]
    assert classifier.tags == ["tag1"]


def test_is_positive_classifier():
    classifier = IsPositive(column_names="column_a", tags="tag1")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.tags == ["tag1"]
    assert classifier.ignore_nulls is False


def test_is_positive_or_zero_classifier():
    classifier = IsPositiveOrZero(
        column_names=["column_a", "column_b"], tags=["tag1", "tag2"]
    )
    assert classifier.column_names == [("column_a", None), ("column_b", None)]
    assert classifier.tags == ["tag1", "tag2"]
    assert classifier.ignore_nulls is False


def test_is_positive_or_zero_classifier_wrong_ignore_nulls_type():
    with pytest.raises(ParameterValidationError) as e:
        IsPositiveOrZero(
            column_names=["column_a", "column_b"], ignore_nulls="not_a_bool"
        )
    assert e.value.error_code == ErrorCode.PVDQCL20


def test_is_zero_classifier():
    classifier = IsZero(column_names="column_a", tags="tag1")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.tags == ["tag1"]
    assert classifier.ignore_nulls is False


def test_matches_classifier():
    classifier = Matches(column_names="column_a", regex="^test_.*", tags="tag1")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == ["tag1"]
    assert not classifier.ignore_nulls


def test_matches_classifier_no_tags():
    classifier = Matches(column_names="column_a", regex="^test_.*")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == []
    assert not classifier.ignore_nulls


def test_matches_classifier_ignore_nulls():
    classifier = Matches(column_names="column_a", regex="^test_.*", ignore_nulls=True)
    assert classifier.column_names == [("column_a", None)]
    assert classifier.regex == "^test_.*"
    assert classifier.tags == []
    assert classifier.ignore_nulls


def test_matches_classifier_no_columns():
    classifier = Matches(regex="^test_.*")
    assert classifier.column_names is None
    assert classifier.regex == "^test_.*"
    assert classifier.tags == []
    assert not classifier.ignore_nulls


def test_matches_classifier_wrong_regex_type():
    with pytest.raises(ParameterValidationError) as e:
        Matches(column_names="column_a", regex=123)
    assert e.value.error_code == ErrorCode.PVDQCL22


def test_matches_classifier_wrong_ignore_nulls_type():
    with pytest.raises(ParameterValidationError) as e:
        Matches(column_names="column_a", regex="^test_.*", ignore_nulls="not_a_bool")
    assert e.value.error_code == ErrorCode.PVDQCL22


def test_scale_identity():
    continents = IdentityScale(
        1,
        7,
    )
    assert continents.scale_range == (1, 7)
    assert continents.bins == 6


def test_scale_linear():
    scale = LinearScale(
        1,
        7,
    )
    assert scale.scale_range == (1, 7)
    assert scale.bins == 100

    scale = LinearScale(1, 7, 2)
    assert scale.scale_range == (1, 7)
    assert scale.bins == 2


def test_scale_monomial():
    scale = MonomialScale(1, 7, 2)
    assert scale.scale_range == (1, 7)
    assert scale.power == 2
    assert scale.bins == 100

    scale = MonomialScale(1, 7, 2, 3)
    assert scale.scale_range == (1, 7)
    assert scale.power == 2
    assert scale.bins == 3


def test_scale_logarithmic():
    scale = LogarithmicScale(
        1,
        7,
    )
    assert scale.scale_range == (1, 7)
    assert scale.base == math.e
    assert scale.bins == 100

    scale = LogarithmicScale(1, 7, 2, 3)
    assert scale.scale_range == (1, 7)
    assert scale.base == 2
    assert scale.bins == 3


def test_scale_exponential():
    scale = ExponentialScale(1, 7)
    assert scale.scale_range == (1, 7)
    assert scale.base == math.e
    assert scale.bins == 100

    scale = ExponentialScale(1, 7, 2, 4)
    assert scale.scale_range == (1, 7)
    assert scale.bins == 4
    assert scale.base == 2


def test_scale_identity_validation():
    with pytest.raises(ParameterValidationError) as e:
        IdentityScale(7, 1)
    assert e.value.error_code == ErrorCode.PVDQCL23

    with pytest.raises(ParameterValidationError) as e:
        IdentityScale(1, 7, bins=0)
    assert e.value.error_code == ErrorCode.PVDQCL2


def test_scale_lineal_validation():
    with pytest.raises(ParameterValidationError) as e:
        LinearScale(7, 1)
    assert e.value.error_code == ErrorCode.PVDQCL23

    with pytest.raises(ParameterValidationError) as e:
        LinearScale(1, 7, bins=0)
    assert e.value.error_code == ErrorCode.PVDQCL24


def test_scale_monomial_validation():
    with pytest.raises(ParameterValidationError) as e:
        MonomialScale(7, 1, 2)
    assert e.value.error_code == ErrorCode.PVDQCL23

    with pytest.raises(ParameterValidationError) as e:
        MonomialScale(1, 7, 0)
    assert e.value.error_code == ErrorCode.PVDQCL25

    with pytest.raises(ParameterValidationError) as e:
        MonomialScale(1, 7, 2, 0)
    assert e.value.error_code == ErrorCode.PVDQCL25


def test_scale_logarithmic_validation():
    with pytest.raises(ParameterValidationError) as e:
        LogarithmicScale(7, 1)
    assert e.value.error_code == ErrorCode.PVDQCL23

    with pytest.raises(ParameterValidationError) as e:
        LogarithmicScale(1, 7, base=0)
    assert e.value.error_code == ErrorCode.PVDQCL26

    with pytest.raises(ParameterValidationError) as e:
        LogarithmicScale(1, 7, bins=0)
    assert e.value.error_code == ErrorCode.PVDQCL26


def test_scale_exponential_validation():
    with pytest.raises(ParameterValidationError) as e:
        ExponentialScale(7, 1)
    assert e.value.error_code == ErrorCode.PVDQCL23

    with pytest.raises(ParameterValidationError) as e:
        ExponentialScale(1, 7, base=0)
    assert e.value.error_code == ErrorCode.PVDQCL27

    with pytest.raises(ParameterValidationError) as e:
        ExponentialScale(1, 7, bins=0)
    assert e.value.error_code == ErrorCode.PVDQCL27


def test_categorizer_class():
    scale = LinearScale(
        1,
        7,
    )
    categorizer = Categorizer(scale=scale)
    assert categorizer.scale == scale


def test_categorizer_wrong_scale_type_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        Categorizer(scale="hi")
    assert e.value.error_code == ErrorCode.PVDQCL2

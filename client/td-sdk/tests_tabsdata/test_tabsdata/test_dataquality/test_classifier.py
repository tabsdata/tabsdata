#
# Copyright 2025 Tabs Data Inc.
#
import math
import sys
from typing import get_args

import pytest

import tabsdata.tableframe.dtypes as td_dtypes
from tabsdata import lit
from tabsdata.dataquality._classifier import (
    Categorizer,
    DoesNotMatch,
    ExponentialScale,
    HasLength,
    IdentityScale,
    IsBetween,
    IsIn,
    IsNan,
    IsNegative,
    IsNegativeOrZero,
    IsNotBetween,
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
    ErrorCode,
    ParameterValidationError,
)

# noinspection PyUnresolvedReferences
from . import pytestmark  # noqa: F401


def test_does_not_match_classifier():
    classifier = DoesNotMatch(column_names="column_a", pattern="^test_.*", tags="tag1")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.pattern == "^test_.*"
    assert classifier.tags == ["tag1"]


def test_does_not_match_classifier_multiple_tags():
    classifier = DoesNotMatch(
        column_names=["column_a", "column_b"], pattern="^test_.*", tags=["tag1", "tag2"]
    )
    assert classifier.column_names == [("column_a", None), ("column_b", None)]
    assert classifier.pattern == "^test_.*"
    assert classifier.tags == ["tag1", "tag2"]


def test_does_not_match_classifier_no_tags():
    classifier = DoesNotMatch(column_names="column_a", pattern="^test_.*")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.pattern == "^test_.*"
    assert classifier.tags == []


def test_does_not_match_classifier_no_columns():
    classifier = DoesNotMatch(pattern="^test_.*")
    assert classifier.column_names is None
    assert classifier.pattern == "^test_.*"
    assert classifier.tags == []


def test_does_not_match_classifier_column_tuple():
    classifier = DoesNotMatch(
        column_names=("column_a", "column_b"), pattern="^test_.*", tags="tag1"
    )
    assert classifier.column_names == [("column_a", "column_b")]
    assert classifier.pattern == "^test_.*"
    assert classifier.tags == ["tag1"]


def test_does_not_match_classifier_multiple_column_tuples():
    classifier = DoesNotMatch(
        column_names=[("column_a", "column_b"), ("column_c", "column_d")],
        pattern="^test_.*",
        tags="tag1",
    )
    assert classifier.column_names == [
        ("column_a", "column_b"),
        ("column_c", "column_d"),
    ]
    assert classifier.pattern == "^test_.*"
    assert classifier.tags == ["tag1"]


def test_does_not_match_classifier_invalid_pattern():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(pattern="*")
    assert e.value.error_code == ErrorCode.PVDQCL35


def test_wrong_cols_type_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=123, pattern="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3


def test_wrong_cols_tuple_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=("1", "2", "3"), pattern="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=("1", 3), pattern="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=(1, "3"), pattern="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3


def test_wrong_cols_list_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[123], pattern="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[("1", 2)], pattern="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[(1, "2")], pattern="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[("1", "2", "3")], pattern="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[123, "valid_column"], pattern="^test_.*")
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(
            column_names=[("valid_col1", "valid_col2"), ("invalid_col", 3)],
            pattern="^test_.*",
        )
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(
            column_names=[("valid_col1", "valid_col2"), "valid_col3"],
            pattern="^test_.*",
        )
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(
            column_names=["hi", ("valid_col1", "valid_col2"), "valid_col3"],
            pattern="^test_.*",
        )
    assert e.value.error_code == ErrorCode.PVDQCL3


def test_wrong_tags_type_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names="column_a", pattern="^test_.*", tags=123)
    assert e.value.error_code == ErrorCode.PVDQCL3

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names="column_a", pattern="^test_.*", tags=[123])
    assert e.value.error_code == ErrorCode.PVDQCL3


def test_has_len_classifier():
    classifier = HasLength(min_len=5, max_len=10)
    assert classifier.min == 5
    assert classifier.max == 10


def test_has_len_classifier_only_min():
    classifier = HasLength(min_len=5)
    assert classifier.min == 5
    assert classifier.max == sys.maxsize


def test_has_len_classifier_only_max():
    classifier = HasLength(max_len=10)
    assert classifier.min == 0
    assert classifier.max == 10


def test_is_in_classifier():
    classifier = IsIn(values=[1, 2, 3, None])
    assert classifier.values == {1, 2, 3, None}


def test_is_in_classifier_NaN():
    classifier = IsIn(values=[1, 2, 3, float("Nan"), float("Nan")])
    assert len(classifier.values) == 4


def test_is_in_classifier_wrong_set_type():
    with pytest.raises(ParameterValidationError) as e:
        IsIn(values=3)
    assert e.value.error_code == ErrorCode.PVDQCL9


def test_is_between_classifier():
    classifier = IsBetween(min_len=5, max_len=10)
    assert classifier.min == 5
    assert classifier.max == 10
    assert classifier.closed == "both"

    classifier = IsBetween(min_len=5, max_len=5)
    assert classifier.min == 5
    assert classifier.max == 5
    assert classifier.closed == "both"


def test_is_between_classifier_valid_closed_options():
    valid_closed_options = get_args(RangeClosedOnSpec)
    for closed_option in valid_closed_options:
        classifier = IsBetween(min_len=5, max_len=10, closed=closed_option)
        assert classifier.closed == closed_option


def test_is_between_classifier_wrong_closed_option():
    with pytest.raises(ParameterValidationError) as e:
        IsBetween(min_len=5, max_len=10, closed="invalid_option")
    assert e.value.error_code == ErrorCode.PVDQCL10


def test_is_between_classifier_no_values():
    classifier = IsBetween()
    assert classifier.min is None
    assert classifier.max is None


def test_is_between_classifier_wrong_min_max_types():
    with pytest.raises(ParameterValidationError) as e:
        IsBetween(min_len="5", max_len=10)
    assert e.value.error_code == ErrorCode.PVDQCL10

    with pytest.raises(ParameterValidationError) as e:
        IsBetween(min_len=5, max_len="10")
    assert e.value.error_code == ErrorCode.PVDQCL10


def test_is_between_classifier_wrong_min_max():
    with pytest.raises(ParameterValidationError) as e:
        IsBetween(min_len=10, max_len=9)
    assert e.value.error_code == ErrorCode.PVDQCL10


def test_is_not_between_classifier():
    classifier = IsNotBetween(min_len=5, max_len=10)
    assert classifier.min == 5
    assert classifier.max == 10
    assert classifier.closed == "both"

    classifier = IsNotBetween(min_len=5, max_len=5)
    assert classifier.min == 5
    assert classifier.max == 5
    assert classifier.closed == "both"


def test_is_not_between_classifier_valid_closed_options():
    valid_closed_options = get_args(RangeClosedOnSpec)
    for closed_option in valid_closed_options:
        classifier = IsNotBetween(min_len=5, max_len=10, closed=closed_option)
        assert classifier.closed == closed_option


def test_is_not_between_classifier_wrong_closed_option():
    with pytest.raises(ParameterValidationError) as e:
        IsNotBetween(min_len=5, max_len=10, closed="invalid_option")
    assert e.value.error_code == ErrorCode.PVDQCL10


def test_is_not_between_classifier_no_values():
    classifier = IsNotBetween()
    assert classifier.min is None
    assert classifier.max is None


def test_is_not_between_classifier_wrong_min_max_types():
    with pytest.raises(ParameterValidationError) as e:
        IsNotBetween(min_len="5", max_len=10)
    assert e.value.error_code == ErrorCode.PVDQCL10

    with pytest.raises(ParameterValidationError) as e:
        IsNotBetween(min_len=5, max_len="10")
    assert e.value.error_code == ErrorCode.PVDQCL10


def test_is_not_between_classifier_wrong_min_max():
    with pytest.raises(ParameterValidationError) as e:
        IsNotBetween(min_len=10, max_len=9)
    assert e.value.error_code == ErrorCode.PVDQCL10


def test_is_nan_classifier():
    classifier = IsNan()
    assert classifier.column_names is None
    assert classifier.tags == []


def test_is_negative_classifier():
    classifier = IsNegative(column_names="column_a", tags=["tag1", "tag2"])
    assert classifier.column_names == [("column_a", None)]
    assert classifier.tags == ["tag1", "tag2"]


def test_is_negative_or_zero_classifier():
    classifier = IsNegativeOrZero(column_names=["column_a", "column_b"], tags="tag1")
    assert classifier.column_names == [("column_a", None), ("column_b", None)]
    assert classifier.tags == ["tag1"]


def test_is_not_in_classifier():
    classifier = IsNotIn(values=["a", "b", "c", "a"])
    assert classifier.values == {"a", "b", "c"}


def test_is_not_in_classifier_wrong_set_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNotIn(values=3)
    assert e.value.error_code == ErrorCode.PVDQCL10


def test_is_not_nan_classifier():
    classifier = IsNotNan(column_names="column_a", tags="tag1")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.tags == ["tag1"]


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


def test_is_positive_or_zero_classifier():
    classifier = IsPositiveOrZero(
        column_names=["column_a", "column_b"], tags=["tag1", "tag2"]
    )
    assert classifier.column_names == [("column_a", None), ("column_b", None)]
    assert classifier.tags == ["tag1", "tag2"]


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


def test_matches_classifier():
    classifier = Matches(column_names="column_a", pattern="^test_.*", tags="tag1")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.pattern == "^test_.*"
    assert classifier.tags == ["tag1"]


def test_matches_classifier_no_tags():
    classifier = Matches(column_names="column_a", pattern="^test_.*")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.pattern == "^test_.*"
    assert classifier.tags == []


def test_matches_classifier_ignore_nulls():
    classifier = Matches(column_names="column_a", pattern="^test_.*")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.pattern == "^test_.*"
    assert classifier.tags == []


def test_matches_classifier_no_columns():
    classifier = Matches(pattern="^test_.*")
    assert classifier.column_names is None
    assert classifier.pattern == "^test_.*"
    assert classifier.tags == []


def test_matches_classifier_wrong_patttern_type():
    with pytest.raises(ParameterValidationError) as e:
        Matches(column_names="column_a", pattern=123)
    assert e.value.error_code == ErrorCode.PVDQCL22


def test_matches_classifier_invalid_pattern():
    with pytest.raises(ParameterValidationError) as e:
        Matches(pattern="*")
    assert e.value.error_code == ErrorCode.PVDQCL36


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


def test_matches_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        Matches(pattern="^test_.*", on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL22

    classifier = Matches(pattern="^test_.*", on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = Matches(pattern="^test_.*", on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        Matches(pattern="^test_.*", on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL22

    classifier = Matches(pattern="^test_.*", on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = Matches(pattern="^test_.*", on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_does_not_match_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(pattern="^test_.*", on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL3

    classifier = DoesNotMatch(pattern="^test_.*", on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = DoesNotMatch(pattern="^test_.*", on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(pattern="^test_.*", on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL3

    classifier = DoesNotMatch(pattern="^test_.*", on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = DoesNotMatch(pattern="^test_.*", on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_has_length_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        HasLength(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL7

    classifier = HasLength(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = HasLength(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        HasLength(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL7

    classifier = HasLength(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = HasLength(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_in_on_missing_column():
    with pytest.raises(ParameterValidationError) as e:
        IsIn(values=[1], on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL9

    classifier = IsIn(values=[1], on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsIn(values=[1], on_missing_column="fail")
    assert classifier.on_missing_column == "fail"


def test_is_not_in_on_missing_column():
    with pytest.raises(ParameterValidationError) as e:
        IsNotIn(values=[1], on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL10

    classifier = IsNotIn(values=[1], on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNotIn(values=[1], on_missing_column="fail")
    assert classifier.on_missing_column == "fail"


def test_is_between_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsBetween(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL10

    classifier = IsBetween(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsBetween(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsBetween(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL10

    classifier = IsBetween(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsBetween(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_not_between_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNotBetween(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL12

    classifier = IsNotBetween(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNotBetween(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNotBetween(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL12

    classifier = IsNotBetween(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNotBetween(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_nan_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNan(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL14

    classifier = IsNan(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNan(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNan(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL14

    classifier = IsNan(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNan(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_negative_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNegative(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL14

    classifier = IsNegative(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNegative(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNegative(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL14

    classifier = IsNegative(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNegative(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_negative_or_zero_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNegativeOrZero(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL14

    classifier = IsNegativeOrZero(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNegativeOrZero(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNegativeOrZero(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL14

    classifier = IsNegativeOrZero(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNegativeOrZero(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_not_nan_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNotNan(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL14

    classifier = IsNotNan(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNotNan(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNotNan(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL14

    classifier = IsNotNan(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNotNan(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_not_null_on_missing_column():
    with pytest.raises(ParameterValidationError) as e:
        IsNotNull(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL15

    classifier = IsNotNull(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNotNull(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"


def test_is_not_null_nor_nan_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNotNullNorNan(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL15

    classifier = IsNotNullNorNan(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNotNullNorNan(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNotNullNorNan(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL15

    classifier = IsNotNullNorNan(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNotNullNorNan(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_not_zero_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNotZero(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL16

    classifier = IsNotZero(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNotZero(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNotZero(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL16

    classifier = IsNotZero(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNotZero(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_null_on_missing_column():
    with pytest.raises(ParameterValidationError) as e:
        IsNull(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL17

    classifier = IsNull(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNull(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"


def test_is_null_or_nan_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNullOrNan(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL18

    classifier = IsNullOrNan(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNullOrNan(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNullOrNan(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL18

    classifier = IsNullOrNan(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNullOrNan(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_positive_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsPositive(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL19

    classifier = IsPositive(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsPositive(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsPositive(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL19

    classifier = IsPositive(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsPositive(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_positive_or_zero_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsPositiveOrZero(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL20

    classifier = IsPositiveOrZero(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsPositiveOrZero(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsPositiveOrZero(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL20

    classifier = IsPositiveOrZero(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsPositiveOrZero(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_zero_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsZero(on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL21

    classifier = IsZero(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsZero(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsZero(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL21

    classifier = IsZero(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsZero(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_categorizer_on_missing_column_and_on_wrong_type():
    scale = LinearScale(1, 7)
    with pytest.raises(ParameterValidationError) as e:
        Categorizer(scale=scale, on_missing_column="error")
    assert e.value.error_code == ErrorCode.PVDQCL2

    classifier = Categorizer(scale=scale, on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = Categorizer(scale=scale, on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        Categorizer(scale=scale, on_wrong_type="error")
    assert e.value.error_code == ErrorCode.PVDQCL2

    classifier = Categorizer(scale=scale, on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = Categorizer(scale=scale, on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"

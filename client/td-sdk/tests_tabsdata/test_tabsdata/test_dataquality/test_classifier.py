#
# Copyright 2025 Tabs Data Inc.
#
import math
import sys
from typing import cast, get_args

import pytest

import tabsdata.tableframe.dtypes as td_dtypes
from tabsdata import lit
from tabsdata.api.tabsdata_server import Collection
from tabsdata.dataquality._classifier import (
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
    ScaleCategorizer,
)
from tabsdata.exceptions import (
    ErrorCode,
    ParameterValidationError,
    ParameterValueError,
    TabsDataException,
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
    assert classifier.tags is None


def test_does_not_match_classifier_no_columns():
    classifier = DoesNotMatch(pattern="^test_.*")
    assert classifier.column_names is None
    assert classifier.pattern == "^test_.*"
    assert classifier.tags is None


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
    with pytest.raises(ParameterValueError) as e:
        DoesNotMatch(pattern="*")
    assert e.value.error_code == ErrorCode.DQC146


def test_wrong_cols_type_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=123, pattern="^test_.*")
    assert e.value.error_code == ErrorCode.DQC144


def test_wrong_cols_tuple_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=("1", "2", "3"), pattern="^test_.*")
    assert e.value.error_code == ErrorCode.DQC144

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=("1", 3), pattern="^test_.*")
    assert e.value.error_code == ErrorCode.DQC144

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=(1, "3"), pattern="^test_.*")
    assert e.value.error_code == ErrorCode.DQC144


def test_wrong_cols_list_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[123], pattern="^test_.*")
    assert e.value.error_code == ErrorCode.DQC144

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[("1", 2)], pattern="^test_.*")
    assert e.value.error_code == ErrorCode.DQC144

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[(1, "2")], pattern="^test_.*")
    assert e.value.error_code == ErrorCode.DQC144

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[("1", "2", "3")], pattern="^test_.*")
    assert e.value.error_code == ErrorCode.DQC144

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names=[123, "valid_column"], pattern="^test_.*")
    assert e.value.error_code == ErrorCode.DQC144

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(
            column_names=[("valid_col1", "valid_col2"), ("invalid_col", 3)],
            pattern="^test_.*",
        )
    assert e.value.error_code == ErrorCode.DQC144

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(
            column_names=[("valid_col1", "valid_col2"), "valid_col3"],
            pattern="^test_.*",
        )
    assert e.value.error_code == ErrorCode.DQC144

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(
            column_names=["hi", ("valid_col1", "valid_col2"), "valid_col3"],
            pattern="^test_.*",
        )
    assert e.value.error_code == ErrorCode.DQC144


def test_wrong_tags_type_raises_error():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names="column_a", pattern="^test_.*", tags=123)
    assert e.value.error_code == ErrorCode.DQC144

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(column_names="column_a", pattern="^test_.*", tags=[123])
    assert e.value.error_code == ErrorCode.DQC144


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
    assert e.value.error_code == ErrorCode.DQC142


def test_is_between_classifier():
    classifier = IsBetween(min_val=5, max_val=10)
    assert classifier.min == 5
    assert classifier.max == 10
    assert classifier.closed_on == "both"

    classifier = IsBetween(min_val=5, max_val=5)
    assert classifier.min == 5
    assert classifier.max == 5
    assert classifier.closed_on == "both"


def test_is_between_classifier_valid_closed_options():
    valid_closed_options = get_args(RangeClosedOnSpec)
    for closed_option in valid_closed_options:
        classifier = IsBetween(min_val=5, max_val=10, closed_on=closed_option)
        assert classifier.closed_on == closed_option


def test_is_between_classifier_wrong_closed_option():
    with pytest.raises(ParameterValidationError) as e:
        IsBetween(min_val=5, max_val=10, closed="invalid_option")
    assert e.value.error_code == ErrorCode.DQC135


def test_is_between_classifier_no_values():
    classifier = IsBetween()
    assert classifier.min is None
    assert classifier.max is None


def test_is_between_classifier_wrong_min_max_types():
    with pytest.raises(ParameterValueError) as e:
        IsBetween(min_val="5", max_val=10)
    assert e.value.error_code == ErrorCode.DQC136

    with pytest.raises(ParameterValueError) as e:
        IsBetween(min_val=5, max_val="10")
    assert e.value.error_code == ErrorCode.DQC136


def test_is_between_classifier_wrong_min_max():
    with pytest.raises(ParameterValueError) as e:
        IsBetween(min_val=10, max_val=9)
    assert e.value.error_code == ErrorCode.DQC136


def test_is_not_between_classifier():
    classifier = IsNotBetween(min_val=5, max_val=10)
    assert classifier.min == 5
    assert classifier.max == 10
    assert classifier.closed_on == "both"

    classifier = IsNotBetween(min_val=5, max_val=5)
    assert classifier.min == 5
    assert classifier.max == 5
    assert classifier.closed_on == "both"


def test_is_not_between_classifier_valid_closed_options():
    valid_closed_options = get_args(RangeClosedOnSpec)
    for closed_option in valid_closed_options:
        classifier = IsNotBetween(min_val=5, max_val=10, closed_on=closed_option)
        assert classifier.closed_on == closed_option


def test_is_not_between_classifier_wrong_closed_option():
    with pytest.raises(ParameterValidationError) as e:
        IsNotBetween(min_val=5, max_val=10, closed="invalid_option")
    assert e.value.error_code == ErrorCode.DQC135


def test_is_not_between_classifier_no_values():
    classifier = IsNotBetween()
    assert classifier.min is None
    assert classifier.max is None


def test_is_not_between_classifier_wrong_min_max_types():
    with pytest.raises(ParameterValueError) as e:
        IsNotBetween(min_val="5", max_val=10)
    assert e.value.error_code == ErrorCode.DQC136

    with pytest.raises(ParameterValueError) as e:
        IsNotBetween(min_val=5, max_val="10")
    assert e.value.error_code == ErrorCode.DQC136


def test_is_not_between_classifier_wrong_min_max():
    with pytest.raises(ParameterValueError) as e:
        IsNotBetween(min_val=10, max_val=9)
    assert e.value.error_code == ErrorCode.DQC136


def test_is_nan_classifier():
    classifier = IsNan()
    assert classifier.column_names is None
    assert classifier.tags is None


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
    assert e.value.error_code == ErrorCode.DQC142


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
    assert classifier.tags is None


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
    assert e.value.error_code == ErrorCode.DQC132


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
    assert classifier.tags is None


def test_matches_classifier_ignore_nulls():
    classifier = Matches(column_names="column_a", pattern="^test_.*")
    assert classifier.column_names == [("column_a", None)]
    assert classifier.pattern == "^test_.*"
    assert classifier.tags is None


def test_matches_classifier_no_columns():
    classifier = Matches(pattern="^test_.*")
    assert classifier.column_names is None
    assert classifier.pattern == "^test_.*"
    assert classifier.tags is None


def test_matches_classifier_wrong_patttern_type():
    with pytest.raises(ParameterValidationError) as e:
        Matches(column_names="column_a", pattern=123)
    assert e.value.error_code == ErrorCode.DQC144


def test_matches_classifier_invalid_pattern():
    with pytest.raises(ParameterValueError) as e:
        Matches(pattern="*")
    assert e.value.error_code == ErrorCode.DQC146


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
    with pytest.raises(ParameterValueError) as e:
        IdentityScale(7, 1)
    assert e.value.error_code == ErrorCode.DQC107

    with pytest.raises(ParameterValidationError) as e:
        IdentityScale(1, 7, bins=0)
    assert e.value.error_code == ErrorCode.DQC106


def test_scale_lineal_validation():
    with pytest.raises(ParameterValueError) as e:
        LinearScale(7, 1)
    assert e.value.error_code == ErrorCode.DQC107

    with pytest.raises(ParameterValidationError) as e:
        LinearScale(1, 7, bins="x")
    assert e.value.error_code == ErrorCode.DQC110


def test_scale_monomial_validation():
    with pytest.raises(ParameterValueError) as e:
        MonomialScale(7, 1, 2)
    assert e.value.error_code == ErrorCode.DQC107

    with pytest.raises(ParameterValidationError) as e:
        MonomialScale(1, 7, 0)
    assert e.value.error_code == ErrorCode.DQC111

    with pytest.raises(ParameterValidationError) as e:
        MonomialScale(1, 7, 2, "x")
    assert e.value.error_code == ErrorCode.DQC111


def test_scale_logarithmic_validation():
    with pytest.raises(ParameterValueError) as e:
        LogarithmicScale(7, 1)
    assert e.value.error_code == ErrorCode.DQC107

    with pytest.raises(ParameterValidationError) as e:
        LogarithmicScale(1, 7, base=0)
    assert e.value.error_code == ErrorCode.DQC113

    with pytest.raises(ParameterValidationError) as e:
        LogarithmicScale(1, 7, bins="0")
    assert e.value.error_code == ErrorCode.DQC113


def test_scale_exponential_validation():
    with pytest.raises(ParameterValueError) as e:
        ExponentialScale(7, 1)
    assert e.value.error_code == ErrorCode.DQC107

    with pytest.raises(ParameterValidationError) as e:
        ExponentialScale(1, 7, base=0)
    assert e.value.error_code == ErrorCode.DQC115

    with pytest.raises(ParameterValidationError) as e:
        ExponentialScale(1, 7, bins="x")
    assert e.value.error_code == ErrorCode.DQC115


def test_categorizer_class():
    scale = LinearScale(
        1,
        7,
    )
    categorizer = ScaleCategorizer(scale=scale)
    assert categorizer.scale == scale


def test_matches_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        Matches(pattern="^test_.*", on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC144

    classifier = Matches(pattern="^test_.*", on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = Matches(pattern="^test_.*", on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        Matches(pattern="^test_.*", on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC144

    classifier = Matches(pattern="^test_.*", on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = Matches(pattern="^test_.*", on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_does_not_match_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(pattern="^test_.*", on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC144

    classifier = DoesNotMatch(pattern="^test_.*", on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = DoesNotMatch(pattern="^test_.*", on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        DoesNotMatch(pattern="^test_.*", on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC144

    classifier = DoesNotMatch(pattern="^test_.*", on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = DoesNotMatch(pattern="^test_.*", on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_has_length_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        HasLength(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC146

    classifier = HasLength(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = HasLength(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        HasLength(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC146

    classifier = HasLength(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = HasLength(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_in_on_missing_column():
    with pytest.raises(ParameterValidationError) as e:
        IsIn(values=[1], on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC142

    classifier = IsIn(values=[1], on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsIn(values=[1], on_missing_column="fail")
    assert classifier.on_missing_column == "fail"


def test_is_not_in_on_missing_column():
    with pytest.raises(ParameterValidationError) as e:
        IsNotIn(values=[1], on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC142

    classifier = IsNotIn(values=[1], on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNotIn(values=[1], on_missing_column="fail")
    assert classifier.on_missing_column == "fail"


def test_is_between_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsBetween(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC135

    classifier = IsBetween(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsBetween(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsBetween(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC135

    classifier = IsBetween(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsBetween(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_not_between_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNotBetween(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC135

    classifier = IsNotBetween(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNotBetween(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNotBetween(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC135

    classifier = IsNotBetween(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNotBetween(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_nan_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNan(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC125

    classifier = IsNan(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNan(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNan(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC125

    classifier = IsNan(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNan(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_negative_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNegative(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC133

    classifier = IsNegative(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNegative(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNegative(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC133

    classifier = IsNegative(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNegative(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_negative_or_zero_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNegativeOrZero(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC134

    classifier = IsNegativeOrZero(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNegativeOrZero(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNegativeOrZero(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC134

    classifier = IsNegativeOrZero(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNegativeOrZero(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_not_nan_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNotNan(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC126

    classifier = IsNotNan(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNotNan(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNotNan(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC126

    classifier = IsNotNan(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNotNan(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_not_null_on_missing_column():
    with pytest.raises(ParameterValidationError) as e:
        IsNotNull(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC124

    classifier = IsNotNull(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNotNull(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"


def test_is_not_null_nor_nan_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNotNullNorNan(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC128

    classifier = IsNotNullNorNan(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNotNullNorNan(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNotNullNorNan(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC128

    classifier = IsNotNullNorNan(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNotNullNorNan(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_not_zero_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNotZero(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC130

    classifier = IsNotZero(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNotZero(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNotZero(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC130

    classifier = IsNotZero(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNotZero(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_null_on_missing_column():
    with pytest.raises(ParameterValidationError) as e:
        IsNull(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC123

    classifier = IsNull(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNull(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"


def test_is_null_or_nan_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsNullOrNan(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC127

    classifier = IsNullOrNan(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsNullOrNan(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsNullOrNan(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC127

    classifier = IsNullOrNan(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsNullOrNan(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_positive_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsPositive(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC131

    classifier = IsPositive(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsPositive(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsPositive(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC131

    classifier = IsPositive(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsPositive(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_positive_or_zero_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsPositiveOrZero(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC132

    classifier = IsPositiveOrZero(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsPositiveOrZero(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsPositiveOrZero(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC132

    classifier = IsPositiveOrZero(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsPositiveOrZero(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_zero_on_missing_column_and_on_wrong_type():
    with pytest.raises(ParameterValidationError) as e:
        IsZero(on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC129

    classifier = IsZero(on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = IsZero(on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        IsZero(on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC129

    classifier = IsZero(on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = IsZero(on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_categorizer_on_missing_column_and_on_wrong_type():
    scale = LinearScale(1, 7)
    with pytest.raises(ParameterValidationError) as e:
        ScaleCategorizer(scale=scale, on_missing_column="error")
    assert e.value.error_code == ErrorCode.DQC121

    classifier = ScaleCategorizer(scale=scale, on_missing_column="ignore")
    assert classifier.on_missing_column == "ignore"

    classifier = ScaleCategorizer(scale=scale, on_missing_column="fail")
    assert classifier.on_missing_column == "fail"

    with pytest.raises(ParameterValidationError) as e:
        ScaleCategorizer(scale=scale, on_wrong_type="error")
    assert e.value.error_code == ErrorCode.DQC121

    classifier = ScaleCategorizer(scale=scale, on_wrong_type="ignore")
    assert classifier.on_wrong_type == "ignore"

    classifier = ScaleCategorizer(scale=scale, on_wrong_type="fail")
    assert classifier.on_wrong_type == "fail"


def test_is_null_to_dict():
    classifier = IsNull(column_names="col_a", tags="tag1", on_missing_column="fail")
    expected = {
        "type": "IsNull",
        "column_names": [("col_a", None)],
        "on_missing_column": "fail",
        "on_wrong_type": "ignore",  # default
        "tags": ["tag1"],
    }
    assert classifier._to_dict() == expected


def test_is_between_to_dict():
    classifier = IsBetween(
        column_names="col_a",
        min_val=5,
        max_val=10,
        closed_on="both",
        tags=["t1", "t2"],
        on_missing_column="fail",
        on_wrong_type="fail",
        on_wrong_value="fail",
    )
    expected = {
        "type": "IsBetween",
        "column_names": [("col_a", None)],
        "min_len": 5,
        "max_len": 10,
        "closed_on": "both",
        "tags": ["t1", "t2"],
        "on_missing_column": "fail",
        "on_wrong_type": "fail",
        "on_wrong_value": "fail",
    }
    assert classifier._to_dict() == expected


def test_matches_to_dict():
    classifier = Matches(
        column_names=[("c1", "c2")],
        pattern=".*",
        tags="t1",
        on_missing_column="ignore",
        on_wrong_type="fail",
    )
    expected = {
        "type": "Matches",
        "column_names": [("c1", "c2")],
        "pattern": ".*",
        "tags": ["t1"],
        "on_missing_column": "ignore",
        "on_wrong_type": "fail",
    }
    assert classifier._to_dict() == expected


def test_is_in_to_dict():
    classifier = IsIn(
        column_names="c1",
        values=[1, "a", 3.0],
        tags="t1",
        on_missing_column="fail",
        on_wrong_value="fail",
    )
    result_dict = classifier._to_dict()
    result_values = result_dict.pop("values")

    expected_dict = {
        "type": "IsIn",
        "column_names": [("c1", None)],
        "on_missing_column": "fail",
        "on_wrong_type": "ignore",
        "on_wrong_value": "fail",
        "tags": ["t1"],
    }
    assert result_dict == expected_dict
    assert isinstance(result_values, list)
    assert set(result_values) == {1, "a", 3.0}


def test_scale_categorizer_to_dict():
    scale = LinearScale(min_val=0, max_val=100, bins=10)
    classifier = ScaleCategorizer(
        column_names="c1",
        scale=scale,
        tags="t1",
        on_missing_column="fail",
        on_wrong_type="fail",
        on_wrong_scale_value="fail",
    )
    expected = {
        "type": "ScaleCategorizer",
        "column_names": [("c1", None)],
        "on_missing_column": "fail",
        "on_wrong_type": "fail",
        "scale": {
            "type": "LinearScale",
            "min_val": 0,
            "max_val": 100,
            "bins": 10,
            "use_bin_zero": False,
        },
        "tags": ["t1"],
    }
    assert classifier._to_dict() == expected

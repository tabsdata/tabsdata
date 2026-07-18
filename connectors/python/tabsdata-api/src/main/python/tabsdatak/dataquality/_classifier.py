#
# Copyright 2026 Tabsdata Inc.
#

"""
This module defines a set of classifiers used for data quality analysis. Classifiers
are used to evaluate data in columns and categorize or flag them based on specific
rules. These include boolean classifiers for simple checks (e.g., `IsNull`, `IsBetween`)
and categorizers for binning data.

Classifiers are frozen dataclasses that serialize through the `JsonSerde`
envelope; per-field validation is handled by pydantic via the ``Annotated``
type aliases in `_typing`, and cross-field invariants by ``model_validator``
hooks that raise the relevant `DataQualityErrorCode`.
"""

import math
import re
import sys
from abc import ABC
from dataclasses import field
from typing import ClassVar, Union

from pydantic import model_validator

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak.dataquality._error import DataQualityErrorCode
from tabsdatak.dataquality._typing import (
    MAX_BINS,
    BinCountSpec,
    ColumnNamesWithAltSpec,
    LenSpec,
    OnInvalidColumnSpec,
    OnInvalidValueSpec,
    RangeClosedOnSpec,
    RangeValueSpec,
    ScaleBaseSpec,
    ScaleIdentityLimitSpec,
    ScaleLimitSpec,
    ScalePowerSpec,
    TagsSpec,
    ValuesSpec,
)
from tabsdatak.tableframe._engine.gtypes import (
    _ALL_DTYPES,
    _ALL_NON_BOOLEAN_DTYPES,
    _BOOLEAN_DTYPES,
    _FLOAT_DTYPES,
    _NUMERIC_DTYPES,
    _SIGNED_NUMERIC_DTYPES,
    _STRING_DTYPES,
)


def _parameter_value_message(
    fn_name: str,
    param_names: Union[str, list[str]],
    detail: str,
) -> str:
    """Composes a parameter value validation error message."""
    if isinstance(param_names, str):
        param_names = [param_names]
    parameters = ", ".join(str(n) for n in param_names)
    return (
        "Parameter value validation error, "
        f"function: '{fn_name}', "
        f"parameter(s): {parameters}. error: {detail}"
    )


def _check_identity_min_max(
    min_val: int, max_val: int, fn_name: str, error_code: DataQualityErrorCode
) -> None:
    """Validates that ``min_val < max_val`` and the delta is at most MAX_BINS."""
    if min_val >= max_val:
        detail = (
            f"min={min_val}, max={max_val}, the maximum value must be strictly "
            "greater than the minimum value."
        )
        raise error_code.exception(
            message=_parameter_value_message(fn_name, ["min_val", "max_val"], detail)
        )
    if max_val - min_val > MAX_BINS:
        detail = f"min={min_val}, max={max_val}, maximum delta is {MAX_BINS}."
        raise error_code.exception(
            message=_parameter_value_message(fn_name, ["min_val", "max_val"], detail)
        )


def _check_min_max(
    min_val: ScaleLimitSpec,
    max_val: ScaleLimitSpec,
    fn_name: str,
    error_code: DataQualityErrorCode,
) -> None:
    """Validates that ``min_val < max_val``."""
    if min_val >= max_val:
        detail = (
            f"min={min_val}, max={max_val}, the maximum value must be strictly "
            "greater than the minimum value."
        )
        raise error_code.exception(
            message=_parameter_value_message(fn_name, ["min_val", "max_val"], detail)
        )


def _check_bins(
    bins: int, use_bin_zero: bool, fn_name: str, error_code: DataQualityErrorCode
) -> None:
    """Validates the number of bins against the allowed maximum."""
    max_allowed = MAX_BINS if use_bin_zero else MAX_BINS + 1
    if bins > max_allowed:
        detail = (
            f"bins={bins}, use_bin_zero={use_bin_zero}, "
            f"exceeds the maximum allowed bins of {max_allowed}."
        )
        raise error_code.exception(
            message=_parameter_value_message(fn_name, ["bins", "use_bin_zero"], detail)
        )


def _check_range(
    low: RangeValueSpec | None,
    high: RangeValueSpec | None,
    fn_name: str,
    error_code: DataQualityErrorCode,
) -> None:
    """Validates a ``[low, high]`` range: both endpoints, when present, must be
    of the same type and ``low`` must not exceed ``high``."""
    if low is not None and high is not None:
        if type(low) is not type(high):
            detail = f" low={low} and high={high}, values must be of the same type."
            raise error_code.exception(
                message=_parameter_value_message(fn_name, ["min", "max"], detail)
            )
        if low > high:
            detail = f" low={low} is greater than high={high}."
            raise error_code.exception(
                message=_parameter_value_message(fn_name, ["min", "max"], detail)
            )


def _check_regex(
    pattern: str, fn_name: str, error_code: DataQualityErrorCode
) -> None:
    """Validates that ``pattern`` compiles as a regular expression."""
    try:
        re.compile(pattern)
    except re.error as e:
        detail = f"Invalid regex pattern '{pattern}': {e}."
        raise error_code.exception(
            message=_parameter_value_message(fn_name, "pattern", detail)
        )


# ---------------------------------------------------------------------------
# Scales
# ---------------------------------------------------------------------------


class Scale(ABC, _api.JsonSerde):
    """
    Abstract base class for scales used in categorization.

    A scale defines how numerical data is divided into bins.
    """

    __kind__ = "dqScale"


@_api.dataclass(DataQualityErrorCode.DQC_106)
class IdentityScale(Scale):
    """
    :categories: dq-classifier

    An identity scale where each integer value corresponds to a bin.
    """

    __kind__ = "dqIdentityScale"

    min_val: ScaleIdentityLimitSpec
    max_val: ScaleIdentityLimitSpec
    use_bin_zero: bool = False

    @property
    def bins(self) -> int:
        """The number of bins, derived from the (min, max) range."""
        return self.max_val - self.min_val

    @model_validator(mode="after")
    def _validate(self) -> "IdentityScale":
        _check_identity_min_max(
            self.min_val, self.max_val, type(self).__name__, DataQualityErrorCode.DQC_107
        )
        return self


@_api.dataclass(DataQualityErrorCode.DQC_110)
class LinearScale(Scale):
    """
    :categories: dq-classifier

    A linear scale.
    """

    __kind__ = "dqLinearScale"

    min_val: ScaleLimitSpec
    max_val: ScaleLimitSpec
    bins: BinCountSpec = MAX_BINS
    use_bin_zero: bool = False

    @model_validator(mode="after")
    def _validate(self) -> "LinearScale":
        _check_min_max(
            self.min_val, self.max_val, type(self).__name__, DataQualityErrorCode.DQC_107
        )
        _check_bins(
            self.bins, self.use_bin_zero, type(self).__name__, DataQualityErrorCode.DQC_101
        )
        return self


@_api.dataclass(DataQualityErrorCode.DQC_111)
class MonomialScale(Scale):
    """
    :categories: dq-classifier

    A monomial (power-law) scale.
    """

    __kind__ = "dqMonomialScale"

    min_val: ScaleLimitSpec
    max_val: ScaleLimitSpec
    power: ScalePowerSpec
    bins: BinCountSpec = MAX_BINS
    use_bin_zero: bool = False

    @model_validator(mode="after")
    def _validate(self) -> "MonomialScale":
        _check_min_max(
            self.min_val, self.max_val, type(self).__name__, DataQualityErrorCode.DQC_107
        )
        _check_bins(
            self.bins, self.use_bin_zero, type(self).__name__, DataQualityErrorCode.DQC_101
        )
        return self


@_api.dataclass(DataQualityErrorCode.DQC_113)
class LogarithmicScale(Scale):
    """
    :categories: dq-classifier

    A logarithmic scale.
    """

    __kind__ = "dqLogarithmicScale"

    min_val: ScaleLimitSpec
    max_val: ScaleLimitSpec
    base: ScaleBaseSpec = math.e
    bins: BinCountSpec = MAX_BINS
    use_bin_zero: bool = False

    @model_validator(mode="after")
    def _validate(self) -> "LogarithmicScale":
        _check_min_max(
            self.min_val, self.max_val, type(self).__name__, DataQualityErrorCode.DQC_107
        )
        _check_bins(
            self.bins, self.use_bin_zero, type(self).__name__, DataQualityErrorCode.DQC_101
        )
        return self


@_api.dataclass(DataQualityErrorCode.DQC_115)
class ExponentialScale(Scale):
    """
    :categories: dq-classifier

    An exponential scale.
    """

    __kind__ = "dqExponentialScale"

    min_val: ScaleLimitSpec
    max_val: ScaleLimitSpec
    base: ScaleBaseSpec = math.e
    bins: BinCountSpec = MAX_BINS
    use_bin_zero: bool = False

    @model_validator(mode="after")
    def _validate(self) -> "ExponentialScale":
        _check_min_max(
            self.min_val, self.max_val, type(self).__name__, DataQualityErrorCode.DQC_107
        )
        _check_bins(
            self.bins, self.use_bin_zero, type(self).__name__, DataQualityErrorCode.DQC_101
        )
        return self


# ---------------------------------------------------------------------------
# Classifiers
# ---------------------------------------------------------------------------


class Classifier(ABC, _api.JsonSerde):
    """
    Abstract base class for all data quality classifiers.

    A classifier is applied to one or more columns to produce a classification
    result, which is stored in a new column. It supports tags to allow data quality
    operations to selectively act on specific classifiers.
    """

    __kind__ = "dqClassifier"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        """Returns the set of data types supported by this classifier."""
        return _ALL_DTYPES


class BoolClassifier(Classifier, ABC):
    """
    Abstract base class for classifiers that produce a boolean result
    (`True` or `False`).
    """

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        """Returns the set of data types supported by boolean classifiers."""
        return _ALL_DTYPES


class Categorizer(Classifier, ABC):
    """A base class for categorizers."""


@_api.dataclass(DataQualityErrorCode.DQC_154)
class IsTrue(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value is `True`.
    """

    __kind__ = "dqIsTrue"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _BOOLEAN_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_155)
class IsFalse(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value is `False`.
    """

    __kind__ = "dqIsFalse"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _BOOLEAN_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_123)
class IsNull(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value is `NULL`.
    """

    __kind__ = "dqIsNull"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_type: ClassVar[OnInvalidColumnSpec] = "ignore"
    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _ALL_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_124)
class IsNotNull(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value is *not* `NULL`.
    """

    __kind__ = "dqIsNotNull"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_type: ClassVar[OnInvalidColumnSpec] = "ignore"
    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _ALL_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_125)
class IsNan(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value is `NaN` (Not a Number).
    """

    __kind__ = "dqIsNan"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _FLOAT_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_126)
class IsNotNan(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value is *not* `NaN`.
    """

    __kind__ = "dqIsNotNan"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _FLOAT_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_127)
class IsNullOrNan(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value is either `NULL` or `NaN`.
    """

    __kind__ = "dqIsNullOrNan"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _FLOAT_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_128)
class IsNotNullNorNan(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value is neither `NULL` nor `NaN`.
    """

    __kind__ = "dqIsNotNullNorNan"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _FLOAT_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_129)
class IsZero(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a numeric value is zero.
    """

    __kind__ = "dqIsZero"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _NUMERIC_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_130)
class IsNotZero(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a numeric value is not zero.
    """

    __kind__ = "dqIsNotZero"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _NUMERIC_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_131)
class IsPositive(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a numeric value is positive.
    """

    __kind__ = "dqIsPositive"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _SIGNED_NUMERIC_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_132)
class IsPositiveOrZero(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a numeric value is positive or zero.
    """

    __kind__ = "dqIsPositiveOrZero"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _SIGNED_NUMERIC_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_133)
class IsNegative(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a numeric value is negative.
    """

    __kind__ = "dqIsNegative"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _SIGNED_NUMERIC_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_134)
class IsNegativeOrZero(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a numeric value is negative or zero.
    """

    __kind__ = "dqIsNegativeOrZero"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _SIGNED_NUMERIC_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_135)
class BetweenClassifier(BoolClassifier, ABC):
    """A boolean classifier that checks if a value is within a specified range."""

    __kind__ = "dqBetweenClassifier"

    column_names: ColumnNamesWithAltSpec | None = None
    min_val: RangeValueSpec | None = None
    max_val: RangeValueSpec | None = None
    closed_on: RangeClosedOnSpec = "both"
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    on_wrong_value: OnInvalidValueSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _ALL_NON_BOOLEAN_DTYPES

    @model_validator(mode="after")
    def _validate(self) -> "BetweenClassifier":
        _check_range(
            self.min_val, self.max_val, type(self).__name__, DataQualityErrorCode.DQC_136
        )
        return self


@_api.dataclass(DataQualityErrorCode.DQC_135)
class IsBetween(BetweenClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value is *inside* a specified range.
    """

    __kind__ = "dqIsBetween"


@_api.dataclass(DataQualityErrorCode.DQC_135)
class IsNotBetween(BetweenClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value is *outside* a specified range.
    """

    __kind__ = "dqIsNotBetween"


@_api.dataclass(DataQualityErrorCode.DQC_142)
class InClassifier(BoolClassifier, ABC):
    """A boolean classifier that checks if a value is in a specified set of values."""

    __kind__ = "dqInClassifier"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_value: OnInvalidValueSpec = "ignore"
    values: ValuesSpec = field(kw_only=True)
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_type: ClassVar[OnInvalidColumnSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _ALL_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_142)
class IsIn(InClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value is in a specified set of values.
    """

    __kind__ = "dqIsIn"


@_api.dataclass(DataQualityErrorCode.DQC_142)
class IsNotIn(InClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value is *not* in a specified set of values.
    """

    __kind__ = "dqIsNotIn"


@_api.dataclass(DataQualityErrorCode.DQC_144)
class MatchClassifier(BoolClassifier, ABC):
    """A boolean classifier that checks if a value matches a regex pattern."""

    __kind__ = "dqMatchClassifier"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    pattern: str = field(kw_only=True)
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _STRING_DTYPES

    @model_validator(mode="after")
    def _validate(self) -> "MatchClassifier":
        _check_regex(self.pattern, type(self).__name__, DataQualityErrorCode.DQC_145)
        return self


@_api.dataclass(DataQualityErrorCode.DQC_144)
class Matches(MatchClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value matches a regex pattern.
    """

    __kind__ = "dqMatches"


@_api.dataclass(DataQualityErrorCode.DQC_144)
class DoesNotMatch(MatchClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if a value does *not* match a regex pattern.
    """

    __kind__ = "dqDoesNotMatch"


@_api.dataclass(DataQualityErrorCode.DQC_146)
class HasLength(BoolClassifier):
    """
    :categories: dq-classifier

    A boolean classifier that checks if the length of a value is within a given range.
    """

    __kind__ = "dqHasLength"

    column_names: ColumnNamesWithAltSpec | None = None
    min_len: LenSpec = 0
    max_len: LenSpec = sys.maxsize
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _STRING_DTYPES

    @model_validator(mode="after")
    def _validate(self) -> "HasLength":
        _check_range(
            self.min_len, self.max_len, type(self).__name__, DataQualityErrorCode.DQC_147
        )
        return self


@_api.dataclass(DataQualityErrorCode.DQC_121)
class ScaleCategorizer(Categorizer):
    """
    :categories: dq-classifier

    A classifier that bucketizes data into bins based on a given scale.
    """

    __kind__ = "dqScaleCategorizer"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    on_wrong_value: OnInvalidValueSpec = "ignore"
    scale: Scale = field(kw_only=True)
    tags: TagsSpec | None = field(default=None, kw_only=True)

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _NUMERIC_DTYPES

#
# Copyright 2026 Tabsdata Inc.
#

"""
This module defines a set of classifiers used for data quality analysis. Classifiers
are used to evaluate data in columns and categorize or flag them based on specific
rules. These include boolean classifiers for simple checks (e.g., `IsNull`, `IsBetween`)
and categorizers for binning data.

Classifiers are frozen dataclasses that serialize through the `JsonSerde`
envelope; per-field validation is handled by pydantic via the `Annotated`
type aliases in `_typing`, and cross-field invariants by `model_validator`
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
    """Validates that `min_val < max_val` and the delta is at most MAX_BINS."""
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
    """Validates that `min_val < max_val`."""
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
    """Validates a `[low, high]` range: both endpoints, when present, must be
    of the same type and `low` must not exceed `high`."""
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


def _check_regex(pattern: str, fn_name: str, error_code: DataQualityErrorCode) -> None:
    """Validates that `pattern` compiles as a regular expression."""
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
    """Abstract base class for the scales used by `ScaleCategorizer`.

    A scale defines how a numeric range is divided into bins. Concrete
    scales differ only in how the bin edges are spaced.
    """

    __kind__ = "dqScale"


@_api.dataclass(DataQualityErrorCode.DQC_106)
class IdentityScale(Scale):
    """A scale where every integer in `[min_val, max_val)` maps to its own
    bin (used with `ScaleCategorizer`).

    :categories: dq-classifier

    Args:
        min_val: Lowest integer of the scale; a whole number.
        max_val: Highest integer of the scale; a whole number strictly
            greater than `min_val` and no more than 100 above it (the
            span is the bin count).
        use_bin_zero: If `False`, the minimum value belongs to the first
            bin; if `True`, `min_val` gets its own bin 0.
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
            self.min_val,
            self.max_val,
            type(self).__name__,
            DataQualityErrorCode.DQC_107,
        )
        return self


@_api.dataclass(DataQualityErrorCode.DQC_110)
class LinearScale(Scale):
    """A scale that splits `[min_val, max_val]` into equal-width bins (used
    with `ScaleCategorizer`).

    :categories: dq-classifier

    Args:
        min_val: Lowest value of the scale; an int or float.
        max_val: Highest value of the scale; strictly greater than
            `min_val`.
        bins: Number of bins to split the range into; a whole number from
            1 to 100 (default 100).
        use_bin_zero: If `False`, the minimum value belongs to the first
            bin; if `True`, `min_val` gets its own bin 0.
    """

    __kind__ = "dqLinearScale"

    min_val: ScaleLimitSpec
    max_val: ScaleLimitSpec
    bins: BinCountSpec = MAX_BINS
    use_bin_zero: bool = False

    @model_validator(mode="after")
    def _validate(self) -> "LinearScale":
        _check_min_max(
            self.min_val,
            self.max_val,
            type(self).__name__,
            DataQualityErrorCode.DQC_107,
        )
        _check_bins(
            self.bins,
            self.use_bin_zero,
            type(self).__name__,
            DataQualityErrorCode.DQC_101,
        )
        return self


@_api.dataclass(DataQualityErrorCode.DQC_111)
class MonomialScale(Scale):
    """A power-law scale over `[min_val, max_val]` whose bin widths grow as
    a power of the value (used with `ScaleCategorizer`).

    :categories: dq-classifier

    Args:
        min_val: Lowest value of the scale; an int or float.
        max_val: Highest value of the scale; strictly greater than
            `min_val`.
        power: Exponent of the power-law spacing; a float greater than 0.
        bins: Number of bins; a whole number from 1 to 100 (default 100).
        use_bin_zero: If `False`, the minimum value belongs to the first
            bin; if `True`, `min_val` gets its own bin 0.
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
            self.min_val,
            self.max_val,
            type(self).__name__,
            DataQualityErrorCode.DQC_107,
        )
        _check_bins(
            self.bins,
            self.use_bin_zero,
            type(self).__name__,
            DataQualityErrorCode.DQC_101,
        )
        return self


@_api.dataclass(DataQualityErrorCode.DQC_113)
class LogarithmicScale(Scale):
    """A scale whose bin edges are spaced logarithmically over
    `[min_val, max_val]` (used with `ScaleCategorizer`).

    :categories: dq-classifier

    Args:
        min_val: Lowest value of the scale; an int or float.
        max_val: Highest value of the scale; strictly greater than
            `min_val`.
        base: Base of the logarithm; a float greater than 0 (default
            `e`).
        bins: Number of bins; a whole number from 1 to 100 (default 100).
        use_bin_zero: If `False`, the minimum value belongs to the first
            bin; if `True`, `min_val` gets its own bin 0.
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
            self.min_val,
            self.max_val,
            type(self).__name__,
            DataQualityErrorCode.DQC_107,
        )
        _check_bins(
            self.bins,
            self.use_bin_zero,
            type(self).__name__,
            DataQualityErrorCode.DQC_101,
        )
        return self


@_api.dataclass(DataQualityErrorCode.DQC_115)
class ExponentialScale(Scale):
    """A scale whose bin edges are spaced exponentially over
    `[min_val, max_val]` (used with `ScaleCategorizer`).

    :categories: dq-classifier

    Args:
        min_val: Lowest value of the scale; an int or float.
        max_val: Highest value of the scale; strictly greater than
            `min_val`.
        base: Base of the exponent; a float greater than 0 (default
            `e`).
        bins: Number of bins; a whole number from 1 to 100 (default 100).
        use_bin_zero: If `False`, the minimum value belongs to the first
            bin; if `True`, `min_val` gets its own bin 0.
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
            self.min_val,
            self.max_val,
            type(self).__name__,
            DataQualityErrorCode.DQC_107,
        )
        _check_bins(
            self.bins,
            self.use_bin_zero,
            type(self).__name__,
            DataQualityErrorCode.DQC_101,
        )
        return self


# ---------------------------------------------------------------------------
# Classifiers
# ---------------------------------------------------------------------------


class Classifier(ABC, _api.JsonSerde):
    """Abstract base class for all data quality classifiers.

    A classifier evaluates one or more columns and writes a per-row verdict
    into a new column. Collect classifiers in a `DataQuality` action
    (`classifiers=[...]`) alongside the operators that act on their
    verdicts. Concrete classifiers are frozen; build one per rule.

    Concrete classifiers share these construction fields (a few fix
    `on_wrong_type` / `on_wrong_value` and do not expose them):

    Args:
        column_names: Columns to classify, as a list. Each entry is a
            source column name, or a `(source, destination)` tuple naming
            the verdict column (`destination` `None` auto-generates it).
            `None` applies the classifier to every column; always pass a
            list, single values are not coerced.
        on_missing_column: `"ignore"` or `"fail"` when a named column
            is absent.
        on_wrong_type: `"ignore"` or `"fail"` when a column's type is
            not supported by the classifier.
        on_wrong_value: `"ignore"` or `"fail"` when a comparison value
            is incompatible with the column's type.
        tags: Optional list of tags; an operator can target only the
            classifiers carrying a matching tag.
    """

    # devguide-ref: how classifiers/operators compose into a DataQuality check

    __kind__ = "dqClassifier"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        """Returns the set of data types supported by this classifier."""
        return _ALL_DTYPES


class BoolClassifier(Classifier, ABC):
    """Abstract base class for classifiers with a boolean verdict.

    Each row is labelled `True` or `False` (or a reserved sentinel for
    null / nan). Boolean verdicts are consumed by the `AllOk` and
    `AnyFailed` criteria used in `Filter` / `Select` / `Fail`.
    """

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        """Returns the set of data types supported by boolean classifiers."""
        return _ALL_DTYPES


class Categorizer(Classifier, ABC):
    """Abstract base class for classifiers that assign each value to a bin.

    Bin verdicts are consumed by the `InBins` and `NotInBins` criteria.
    """


@_api.dataclass(DataQualityErrorCode.DQC_154)
class IsTrue(BoolClassifier):
    """A boolean classifier that checks whether a value is `True`. Applies
    to boolean columns.

    :categories: dq-classifier
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
    """A boolean classifier that checks whether a value is `False`. Applies
    to boolean columns.

    :categories: dq-classifier
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
    """A boolean classifier that checks whether a value is null. Applies to
    columns of any type.

    :categories: dq-classifier
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
    """A boolean classifier that checks whether a value is not null. Applies
    to columns of any type.

    :categories: dq-classifier
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
    """A boolean classifier that checks whether a value is `NaN` (not a
    number). Applies to floating-point columns.

    :categories: dq-classifier
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
    """A boolean classifier that checks whether a value is not `NaN`.
    Applies to floating-point columns.

    :categories: dq-classifier
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
    """A boolean classifier that checks whether a value is null or `NaN`.
    Applies to floating-point columns.

    :categories: dq-classifier
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
    """A boolean classifier that checks whether a value is neither null nor
    `NaN`. Applies to floating-point columns.

    :categories: dq-classifier
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
    """A boolean classifier that checks whether a numeric value is zero.
    Applies to numeric columns.

    :categories: dq-classifier
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
    """A boolean classifier that checks whether a numeric value is not zero.
    Applies to numeric columns.

    :categories: dq-classifier
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
    """A boolean classifier that checks whether a numeric value is positive.
    Applies to signed numeric columns.

    :categories: dq-classifier
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
    """A boolean classifier that checks whether a numeric value is positive or
    zero. Applies to signed numeric columns.

    :categories: dq-classifier
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
    """A boolean classifier that checks whether a numeric value is negative.
    Applies to signed numeric columns.

    :categories: dq-classifier
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
    """A boolean classifier that checks whether a numeric value is negative or
    zero. Applies to signed numeric columns.

    :categories: dq-classifier
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
            self.min_val,
            self.max_val,
            type(self).__name__,
            DataQualityErrorCode.DQC_136,
        )
        return self


@_api.dataclass(DataQualityErrorCode.DQC_135)
class IsBetween(BetweenClassifier):
    """A boolean classifier that checks whether a value is inside a range.
    Applies to non-boolean columns.

    :categories: dq-classifier

    Args:
        min_val: Lower bound; `None` leaves the low side unbounded.
        max_val: Upper bound; `None` leaves the high side unbounded. When
            both are given they must be the same type and `min_val` must
            not exceed `max_val`.
        closed_on: Which bounds are inclusive: `"none"`, `"lower"`,
            `"upper"` or `"both"` (default `"both"`).

    Examples:
        .. code-block:: python

            IsBetween(["value"], min_val=0.0, max_val=10.0, closed_on="both")
    """

    __kind__ = "dqIsBetween"


@_api.dataclass(DataQualityErrorCode.DQC_135)
class IsNotBetween(BetweenClassifier):
    """A boolean classifier that checks whether a value is outside a range.
    Applies to non-boolean columns.

    :categories: dq-classifier

    Args:
        min_val: Lower bound; `None` leaves the low side unbounded.
        max_val: Upper bound; `None` leaves the high side unbounded. When
            both are given they must be the same type and `min_val` must
            not exceed `max_val`.
        closed_on: Which bounds are inclusive: `"none"`, `"lower"`,
            `"upper"` or `"both"` (default `"both"`).
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
    """A boolean classifier that checks whether a value is in a set of values.
    Applies to columns of any type.

    :categories: dq-classifier

    Args:
        values: Values to test membership against; a non-empty collection
            (at least one). Include `None` to match nulls; duplicates are
            removed.

    Examples:
        .. code-block:: python

            IsIn(["value"], values=[10, 20])
    """

    __kind__ = "dqIsIn"


@_api.dataclass(DataQualityErrorCode.DQC_142)
class IsNotIn(InClassifier):
    """A boolean classifier that checks whether a value is not in a set of
    values. Applies to columns of any type.

    :categories: dq-classifier

    Args:
        values: Values to test membership against; a non-empty collection
            (at least one). Include `None` to match nulls; duplicates are
            removed.
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
    """A boolean classifier that checks whether a value matches a regular
    expression. Applies to string columns.

    :categories: dq-classifier

    Args:
        pattern: Regular expression to match; must be a valid regex.

    Examples:
        .. code-block:: python

            Matches(["value"], pattern=r"[a-z]+")
    """

    __kind__ = "dqMatches"


@_api.dataclass(DataQualityErrorCode.DQC_144)
class DoesNotMatch(MatchClassifier):
    """A boolean classifier that checks whether a value does not match a
    regular expression. Applies to string columns.

    :categories: dq-classifier

    Args:
        pattern: Regular expression to test against; must be a valid regex.
    """

    __kind__ = "dqDoesNotMatch"


@_api.dataclass(DataQualityErrorCode.DQC_146)
class HasLength(BoolClassifier):
    """A boolean classifier that checks whether a value's length is within a
    range. Applies to string columns.

    :categories: dq-classifier

    Args:
        min_len: Minimum length, inclusive; a non-negative whole number
            (default 0).
        max_len: Maximum length, inclusive; a non-negative whole number.
            Must be the same type as `min_len` and not less than it.

    Examples:
        .. code-block:: python

            HasLength(["value"], min_len=2, max_len=5)
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
            self.min_len,
            self.max_len,
            type(self).__name__,
            DataQualityErrorCode.DQC_147,
        )
        return self


@_api.dataclass(DataQualityErrorCode.DQC_121)
class ScaleCategorizer(Categorizer):
    """A classifier that assigns each value to a bin defined by a `Scale`.
    Applies to numeric columns. The verdict column holds the bin index (or
    a reserved sentinel for null / nan / under- / overflow); pair it with
    an `InBins` or `NotInBins` criteria.

    :categories: dq-classifier

    Args:
        scale: The `Scale` (e.g. `LinearScale`, `LogarithmicScale`)
            that maps values to bins.

    Examples:
        .. code-block:: python

            ScaleCategorizer(["value"], scale=LinearScale(0.0, 10.0, bins=5))
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

#
# Copyright 2025 Tabs Data Inc.
#
"""
This module defines a set of classifiers used for data quality analysis. Classifiers
are used to evaluate data in columns and categorize or flag them based on specific
rules. These include boolean classifiers for simple checks (e.g., `IsNull`, `IsBetween`)
and categorizers for binning data.
"""

import math
import re
import sys
from abc import ABC
from typing import AbstractSet, Collection, Optional

from tabsdata.dataquality._typing import (
    MAX_BINS,
    BinsSpec,
    ColumNamesWithAltSpec,
    ColumnNameWithAltSpec,
    LenSpec,
    RangeClosedOnSpec,
    RangeValueSpec,
    ScaleBaseSpec,
    ScaleIdentityLimitSpec,
    ScaleLimitSpec,
    ScalePowerSpec,
    TagSpec,
    TagsSpec,
)
from tabsdata.dataquality._utils import (
    _normalize_to_list,
    _normalize_to_set,
    _td_validate_call,
    _validation_error,
)
from tabsdata.exceptions import (
    ErrorCode,
    ParameterValidationError,
)


class Classifier(ABC):
    """
    Abstract base class for all data quality classifiers.

    A classifier is applied to one or more columns to produce a classification
    result, which is stored in a new column. It supports tags to allow data quality
    operations to selectively act on specific classifiers.
    """

    @_td_validate_call(ErrorCode.PVDQCL1)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the classifier.

        Args:
            column_names: The column(s) to apply the classifier to.
                - A string or list of strings specifies the source columns. The results
                  are stored in columns with auto-generated names.
                - A tuple `(source, destination)` or a list of such tuples allows
                  specifying custom names for the result columns.
                - If `None`, the classifier is applied to all columns.
            tags: A tag or list of tags to associate with the classifier. Tags help
                  filter which classifiers an operation should consider.
        """
        column_names = _normalize_to_list(column_names, keep_none=True)
        if column_names and isinstance(column_names[0], str):
            column_names = [(s, None) for s in column_names]
        self._column_names = column_names
        self._tags = _normalize_to_list(tags)

    @property
    def column_names(self) -> Optional[list[ColumnNameWithAltSpec]]:
        """
        Returns the list of columns the classifier applies to, including optional
        destination column names.
        """
        return self._column_names

    @property
    def tags(self) -> list[TagSpec]:
        """
        Returns the list of tags associated with the classifier.
        """
        return self._tags


class BoolClassifier(Classifier):
    """
    Abstract base class for classifiers that produce a boolean result
    (`True` or `False`).
    """


def _validate_min_max(min_val: ScaleLimitSpec, max_val: ScaleLimitSpec, fn_name: str):
    """Validates that `min_val` is less than `max_val`."""
    if min_val >= max_val:
        params_with_error = (
            f"'min': {min_val}, 'max': {max_val} - The maximum value must be strictly "
            "greater than the minimum value."
        )
        raise _validation_error(ErrorCode.PVDQCL32, fn_name, params_with_error)


def _validate_bins(bins: BinsSpec, min_in_first_bin: bool, fn_name: str):
    """Validates the number of bins against the allowed maximum."""
    max_allowed = MAX_BINS if min_in_first_bin else MAX_BINS + 1
    if bins > max_allowed:
        params_with_error = (
            f"'bins': {bins}, 'min_in_first_bin': {min_in_first_bin} - "
            f"Exceeds the maximum allowed bins of {max_allowed}."
        )
        raise _validation_error(ErrorCode.PVDQCL33, fn_name, params_with_error)


class Scale(ABC):
    """
    Abstract base class for scales used in categorization.

    A scale defines how numerical data is divided into bins.
    """

    def __init__(
        self,
        scale_range: tuple[ScaleLimitSpec, ScaleLimitSpec],
        bins: Optional[BinsSpec],
        min_in_first_bin: bool,
    ):
        self._range = scale_range
        self._bins = bins
        self._min_in_first_bin = min_in_first_bin

    @property
    def scale_range(self) -> tuple[ScaleLimitSpec, ScaleLimitSpec]:
        """Returns the (min, max) range of the scale."""
        return self._range

    @property
    def bins(self) -> Optional[BinsSpec]:
        """Returns the number of bins in the scale."""
        return self._bins

    @property
    def min_in_first_bin(self) -> bool:
        """Returns whether the minimum value is included in the first bin."""
        return self._min_in_first_bin


class IdentityScale(Scale):
    """
    An identity scale where each integer value corresponds to a bin.
    """

    @_td_validate_call(ErrorCode.PVDQCL23)
    def __init__(
        self,
        min_val: ScaleIdentityLimitSpec,
        max_val: ScaleIdentityLimitSpec,
        min_in_first_bin: bool = True,
    ):
        """
        Initializes an identity scale.

        Args:
            min_val: The minimum integer value of the scale.
            max_val: The maximum integer value of the scale.
            min_in_first_bin: If `True`, the minimum value is part of the first bin.
        """
        _validate_min_max(min_val, max_val, f"{self.__class__.__name__}.__init__")
        super().__init__((min_val, max_val), max_val - min_val, min_in_first_bin)


class LinearScale(Scale):
    """
    A linear scale that divides a range into equally sized bins.
    """

    @_td_validate_call(ErrorCode.PVDQCL24)
    def __init__(
        self,
        min_val: ScaleLimitSpec,
        max_val: ScaleLimitSpec,
        bins: BinsSpec = MAX_BINS,
        min_in_first_bin: bool = True,
    ):
        """
        Initializes a linear scale.

        Args:
            min_val: The minimum value of the scale.
            max_val: The maximum value of the scale.
            bins: The number of bins to create.
            min_in_first_bin: If `True`, the minimum value is part of the first bin.
        """
        _validate_min_max(min_val, max_val, f"{self.__class__.__name__}.__init__")
        _validate_bins(bins, min_in_first_bin, f"{self.__class__.__name__}.__init__")
        super().__init__((min_val, max_val), bins, min_in_first_bin)


class MonomialScale(Scale):
    """
    A monomial (power-law) scale.
    """

    @_td_validate_call(ErrorCode.PVDQCL25)
    def __init__(
        self,
        min_val: ScaleLimitSpec,
        max_val: ScaleLimitSpec,
        power: ScalePowerSpec,
        bins: BinsSpec = MAX_BINS,
        min_in_first_bin: bool = True,
    ):
        """
        Initializes a monomial scale.

        Args:
            min_val: The minimum value of the scale.
            max_val: The maximum value of the scale.
            power: The exponent for the power-law function.
            bins: The number of bins.
            min_in_first_bin: If `True`, the minimum value is part of the first bin.
        """
        _validate_min_max(min_val, max_val, f"{self.__class__.__name__}.__init__")
        _validate_bins(bins, min_in_first_bin, f"{self.__class__.__name__}.__init__")
        super().__init__((min_val, max_val), bins, min_in_first_bin)
        self._power = power

    @property
    def power(self) -> ScalePowerSpec:
        """Returns the power of the monomial scale."""
        return self._power


class LogarithmicScale(Scale):
    """
    A logarithmic scale.
    """

    @_td_validate_call(ErrorCode.PVDQCL26)
    def __init__(
        self,
        min_val: ScaleLimitSpec,
        max_val: ScaleLimitSpec,
        base: Optional[ScaleBaseSpec] = None,
        bins: BinsSpec = MAX_BINS,
        min_in_first_bin: bool = True,
    ):
        """
        Initializes a logarithmic scale.

        Args:
            min_val: The minimum value of the scale.
            max_val: The maximum value of the scale.
            base: The base of the logarithm. Defaults to `math.e`.
            bins: The number of bins.
            min_in_first_bin: If `True`, the minimum value is part of the first bin.
        """
        _validate_min_max(min_val, max_val, f"{self.__class__.__name__}.__init__")
        _validate_bins(bins, min_in_first_bin, f"{self.__class__.__name__}.__init__")
        super().__init__((min_val, max_val), bins, min_in_first_bin)
        self._base = base or math.e

    @property
    def base(self) -> ScaleBaseSpec:
        """Returns the base of the logarithmic scale."""
        return self._base


class ExponentialScale(Scale):
    """
    An exponential scale.
    """

    @_td_validate_call(ErrorCode.PVDQCL27)
    def __init__(
        self,
        min_val: ScaleLimitSpec,
        max_val: ScaleLimitSpec,
        base: Optional[ScaleBaseSpec] = None,
        bins: BinsSpec = MAX_BINS,
        min_in_first_bin: bool = True,
    ):
        """
        Initializes an exponential scale.

        Args:
            min_val: The minimum value of the scale.
            max_val: The maximum value of the scale.
            base: The base of the exponent. Defaults to `math.e`.
            bins: The number of bins.
            min_in_first_bin: If `True`, the minimum value is part of the first bin.
        """
        _validate_min_max(min_val, max_val, f"{self.__class__.__name__}.__init__")
        _validate_bins(bins, min_in_first_bin, f"{self.__class__.__name__}.__init__")
        super().__init__((min_val, max_val), bins, min_in_first_bin)
        self._base = base or math.e

    @property
    def base(self) -> ScaleBaseSpec:
        """Returns the base of the exponential scale."""
        return self._base


class Categorizer(Classifier):
    """
    A classifier that bucketizes data into bins based on a given scale.
    """

    @_td_validate_call(ErrorCode.PVDQCL2)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        scale: Scale,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes a categorizer.

        Args:
            column_names: The column(s) to categorize.
            scale: The scale to use for categorization.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)
        self._scale = scale

    @property
    def scale(self) -> Scale:
        """Returns the scale used for categorization."""
        return self._scale


def _validate_pattern(pattern: str, fn_name: str, error_code: ErrorCode) -> str:
    """
    Validates a regular expression pattern.
    """
    try:
        re.compile(pattern)
        return pattern
    except re.error:
        msg = f"Invalid regex pattern '{pattern}', error: {re.error}."
        raise _validation_error(error_code, fn_name, msg)


class Matches(BoolClassifier):
    """
    A boolean classifier that checks if a value matches a regex pattern.
    """

    @_td_validate_call(ErrorCode.PVDQCL22)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        pattern: str,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `Matches` classifier.

        Args:
            column_names: The column(s) to check.
            pattern: The regular expression to match against.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)
        self._pattern = _validate_pattern(
            pattern, f"{self.__class__.__name__}.__init__", ErrorCode.PVDQCL35
        )

    @property
    def pattern(self) -> str:
        """Returns the regex pattern."""
        return self._pattern


class DoesNotMatch(BoolClassifier):
    """
    A boolean classifier that checks if a value does *not* match a regex pattern.
    """

    @_td_validate_call(ErrorCode.PVDQCL3)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        pattern: str,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `DoesNotMatch` classifier.

        Args:
            column_names: The column(s) to apply the check to.
            pattern: The regular expression to not match against.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)
        self._pattern = _validate_pattern(
            pattern, f"{self.__class__.__name__}.__init__", ErrorCode.PVDQCL36
        )

    @property
    def pattern(self) -> str:
        """Returns the regex pattern."""
        return self._pattern


class HasLength(BoolClassifier):
    """
    A boolean classifier that checks if the length of a value is within a given range.
    """

    @_td_validate_call(ErrorCode.PVDQCL7)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        min: LenSpec = 0,
        max: LenSpec = sys.maxsize,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `HasLength` classifier.

        Args:
            column_names: The column(s) to check.
            min_len: The minimum allowed length (inclusive).
            max_len: The maximum allowed length (inclusive).
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)
        if min > max:
            msg = (
                " Parameter validation error. "
                "Function 'HasLen.__init__', "
                f" min='{min}' is greater than max='{max}'."
            )
            raise ParameterValidationError(ErrorCode.PVDQCL8, msg)
        self._min = min
        self._max = max

    @property
    def min(self) -> int:
        """Returns the minimum allowed length."""
        return self._min

    @property
    def max(self) -> int:
        """Returns the maximum allowed length."""
        return self._max


class IsIn(BoolClassifier):
    """
    A boolean classifier that checks if a value is in a specified set of values.
    """

    @_td_validate_call(ErrorCode.PVDQCL9)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        values: Collection,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsIn` classifier.

        Args:
            column_names: The column(s) to check.
            values: The collection of values to check against.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)
        self._values = _normalize_to_set(values)

    @property
    def values(self) -> AbstractSet:
        """Returns the set of values for the check."""
        return self._values


class IsNotIn(BoolClassifier):
    """
    A boolean classifier that checks if a value is *not* in a specified set of values.
    """

    @_td_validate_call(ErrorCode.PVDQCL10)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        values: Collection,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsNotIn` classifier.

        Args:
            column_names: The column(s) to check.
            values: The collection of values to check against.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)
        self._values = _normalize_to_set(values)

    @property
    def values(self) -> AbstractSet:
        """Returns the set of values for the check."""
        return self._values


def _validate_range(
    min: Optional[RangeValueSpec], max: Optional[RangeValueSpec], error_code: ErrorCode
):
    if min is not None and max is not None:
        if type(min) is not type(max):
            msg = (
                " Parameter validation error. "
                "Function 'IsInRange.__init__', "
                f" min='{min}' and max='{max}' must be of the same type."
            )
            raise ParameterValidationError(error_code, msg)
        if min > max:
            msg = (
                " Parameter validation error. "
                "Function 'IsInRange.__init__', "
                f" min='{min}' is greater than max='{max}'."
            )
            raise ParameterValidationError(error_code, msg)


class IsBetween(BoolClassifier):
    """
    A boolean classifier that checks if a value is within a specified range.
    """

    @_td_validate_call(ErrorCode.PVDQCL10)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        min: Optional[RangeValueSpec] = None,
        max: Optional[RangeValueSpec] = None,
        closed: RangeClosedOnSpec = "both",
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsBetween` classifier.

        Args:
            column_names: The column(s) to check.
            min_val: The minimum value of the range.
            max_val: The maximum value of the range.
            closed: Specifies if the range is inclusive ("both", "lower", "upper") or
                exclusive ("none").
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)
        _validate_range(min, max, ErrorCode.PVDQCL11)
        self._min = min
        self._max = max
        self._closed = closed

    @property
    def min(self) -> Optional[RangeValueSpec]:
        """Returns the minimum value of the range."""
        return self._min

    @property
    def max(self) -> Optional[RangeValueSpec]:
        """Returns the maximum value of the range."""
        return self._max

    @property
    def closed(self) -> RangeClosedOnSpec:
        """Returns how the range is closed."""
        return self._closed


class IsNotBetween(BoolClassifier):
    """
    A boolean classifier that checks if a value is *outside* a specified range.
    """

    @_td_validate_call(ErrorCode.PVDQCL12)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        min: Optional[RangeValueSpec] = None,
        max: Optional[RangeValueSpec] = None,
        closed: RangeClosedOnSpec = "both",
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsNotBetween` classifier.

        Args:
            column_names: The column(s) to check.
            min_val: The minimum value of the range.
            max_val: The maximum value of the range.
            closed: Specifies if the range is inclusive ("both", "lower", "upper") or
                exclusive ("none").
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)
        _validate_range(min, max, ErrorCode.PVDQCL11)
        self._min = min
        self._max = max
        self._closed = closed

    @property
    def min(self) -> Optional[RangeValueSpec]:
        """Returns the minimum value of the range."""
        return self._min

    @property
    def max(self) -> Optional[RangeValueSpec]:
        """Returns the maximum value of the range."""
        return self._max

    @property
    def closed(self) -> RangeClosedOnSpec:
        """Returns how the range is closed."""
        return self._closed


class IsNan(BoolClassifier):
    """
    A boolean classifier that checks if a value is `NaN` (Not a Number).
    """

    @_td_validate_call(ErrorCode.PVDQCL14)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsNan` classifier.

        Args:
            column_names: The column(s) to check.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)


class IsNegative(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is negative.
    """

    @_td_validate_call(ErrorCode.PVDQCL14)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsNegative` classifier.

        Args:
            column_names: The column(s) to check.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)


class IsNegativeOrZero(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is negative or zero.
    """

    @_td_validate_call(ErrorCode.PVDQCL14)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsNegativeOrZero` classifier.

        Args:
            column_names: The column(s) to check.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)


class IsNotNan(BoolClassifier):
    """
    A boolean classifier that checks if a value is *not* `NaN`.
    """

    @_td_validate_call(ErrorCode.PVDQCL14)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsNotNan` classifier.

        Args:
            column_names: The column(s) to check.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)


class IsNotNull(BoolClassifier):
    """
    A boolean classifier that checks if a value is *not* `NULL`.
    """

    @_td_validate_call(ErrorCode.PVDQCL15)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsNotNull` classifier.

        Args:
            column_names: The column(s) to check.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)


class IsNotNullNorNan(BoolClassifier):
    """
    A boolean classifier that checks if a value is neither `NULL` nor `NaN`.
    """

    @_td_validate_call(ErrorCode.PVDQCL15)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsNotNullNorNan` classifier.

        Args:
            column_names: The column(s) to check.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names=column_names, tags=tags)


class IsNotZero(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is not zero.
    """

    @_td_validate_call(ErrorCode.PVDQCL16)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsNotZero` classifier.

        Args:
            column_names: The column(s) to check.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)


class IsNull(BoolClassifier):
    """
    A boolean classifier that checks if a value is `NULL`.
    """

    @_td_validate_call(ErrorCode.PVDQCL17)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsNull` classifier.

        Args:
            column_names: The column(s) to check.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)


class IsNullOrNan(BoolClassifier):
    """
    A boolean classifier that checks if a value is either `NULL` or `NaN`.
    """

    @_td_validate_call(ErrorCode.PVDQCL18)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsNullOrNan` classifier.

        Args:
            column_names: The column(s) to check.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)


class IsPositive(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is positive.
    """

    @_td_validate_call(ErrorCode.PVDQCL19)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsPositive` classifier.

        Args:
            column_names: The column(s) to check.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)


class IsPositiveOrZero(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is positive or zero.
    """

    @_td_validate_call(ErrorCode.PVDQCL20)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsPositiveOrZero` classifier.

        Args:
            column_names: The column(s) to check.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)


class IsZero(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is zero.
    """

    @_td_validate_call(ErrorCode.PVDQCL21)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `IsZero` classifier.

        Args:
            column_names: The column(s) to check.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, tags=tags)

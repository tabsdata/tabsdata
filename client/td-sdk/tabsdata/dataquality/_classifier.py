#
# Copyright 2025 Tabs Data Inc.
#
"""
This module defines a set of classifiers used for data quality analysis. Classifiers
are used to evaluate data in columns and categorize or flag them based on specific
rules. These include boolean classifiers for simple checks (e.g., `IsNull`, `IsBetween`)
and categorizers for binning data.
"""

import json
import math
import re
import sys
from abc import ABC, abstractmethod
from typing import AbstractSet, Collection, FrozenSet, Optional, Type, Union

from tabsdata.dataquality._typing import (
    MAX_BINS,
    BinCountSpec,
    BinsCriteriaSpec,
    ColumNamesWithAltSpec,
    ColumnNameWithAltSpec,
    LenSpec,
    OnInvalidColumnSpec,
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
)
from tabsdata.exceptions import (
    ErrorCode,
    ParameterValueError,
)
from tabsdata.tableframe.dtypes import (
    _ALL_DTYPES,
    _ALL_NON_BOOLEAN_DTYPES,
    _FLOAT_DTYPES,
    _NUMERIC_DTYPES,
    _SIGNED_NUMERIC_DTYPES,
    _STRING_DTYPES,
)


class Scale(ABC):
    """
    Abstract base class for scales used in categorization.

    A scale defines how numerical data is divided into bins.
    """

    def __init__(
        self,
        scale_range: tuple[ScaleLimitSpec, ScaleLimitSpec],
        bins: Optional[BinCountSpec],
        use_bin_zero: bool,
    ):
        self._range = scale_range
        self._bins = bins
        self._use_bin_zero = use_bin_zero

    @property
    def scale_range(self) -> tuple[ScaleLimitSpec, ScaleLimitSpec]:
        """Returns the (min, max) range of the scale."""
        return self._range

    @property
    def bins(self) -> Optional[BinCountSpec]:
        """Returns the number of bins in the scale."""
        return self._bins

    @property
    def use_bin_zero(self) -> bool:
        """Returns whether the minimum value is in bin zero or in bin one."""
        return self._use_bin_zero

    @abstractmethod
    def to_dict(self) -> dict:
        pass

    def __repr__(self) -> str:
        return json.dumps(self.to_dict())

    def __str__(self) -> str:
        return json.dumps(self.to_dict())


class IdentityScale(Scale):
    """
    An identity scale where each integer value corresponds to a bin.
    """

    @_td_validate_call(ErrorCode.PVDQCL23)
    def __init__(
        self,
        min_val: ScaleIdentityLimitSpec,
        max_val: ScaleIdentityLimitSpec,
        use_bin_zero: bool = False,
    ):
        """
        Initializes an identity scale.

        Args:
            min_val: The minimum integer value of the scale.
            max_val: The maximum integer value of the scale.
            use_bin_zero: If `False`, the minimum value is part of the first bin.
        """
        _validate_identity_min_max(
            min_val,
            max_val,
            "IdentityScale.__init__",
            ["min_val", "max_val"],
            ErrorCode.PVDQCL44,
        )
        super().__init__((min_val, max_val), max_val - min_val, use_bin_zero)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "min_val": self._range[0],
            "max_val": self._range[1],
            "use_bin_zero": self._use_bin_zero,
        }


class LinearScale(Scale):
    """
    A linear scale that divides a range into equally sized bins.
    """

    @_td_validate_call(ErrorCode.PVDQCL24)
    def __init__(
        self,
        min_val: ScaleLimitSpec,
        max_val: ScaleLimitSpec,
        bins: BinCountSpec = MAX_BINS,
        use_bin_zero: bool = False,
    ):
        """
        Initializes a linear scale.

        Args:
            min_val: The minimum value of the scale.
            max_val: The maximum value of the scale.
            bins: The number of bins to create.
            use_bin_zero: If `False`, the minimum value is part of the first bin.
        """
        _validate_min_max(
            min_val,
            max_val,
            "LinearScale.__init__",
            ["min_val", "max_val"],
            ErrorCode.PVDQCL37,
        )
        _validate_bins(
            bins,
            use_bin_zero,
            "LinearScale.__init__",
            ["bins", "min_in_first_bin"],
            ErrorCode.PVDQCL42,
        )
        super().__init__((min_val, max_val), bins, use_bin_zero)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "min_val": self._range[0],
            "max_val": self._range[1],
            "bins": self._bins,
            "use_bin_zero": self._use_bin_zero,
        }


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
        bins: BinCountSpec = MAX_BINS,
        use_bin_zero: bool = False,
    ):
        """
        Initializes a monomial scale.

        Args:
            min_val: The minimum value of the scale.
            max_val: The maximum value of the scale.
            power: The exponent for the power-law function.
            bins: The number of bins.
            use_bin_zero: If `False`, the minimum value is part of the first bin.
        """
        _validate_min_max(
            min_val,
            max_val,
            "MonomialScale.__init__",
            ["min_val", "max_val"],
            ErrorCode.PVDQCL38,
        )
        _validate_bins(
            bins,
            use_bin_zero,
            "MonomialScale.__init__",
            ["bins", "min_in_first_bin"],
            ErrorCode.PVDQCL42,
        )
        super().__init__((min_val, max_val), bins, use_bin_zero)
        self._power = power

    @property
    def power(self) -> ScalePowerSpec:
        """Returns the power of the monomial scale."""
        return self._power

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "min_val": self._range[0],
            "max_val": self._range[1],
            "power": self._power,
            "bins": self._bins,
            "use_bin_zero": self._use_bin_zero,
        }


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
        bins: BinCountSpec = MAX_BINS,
        use_bin_zero: bool = False,
    ):
        """
        Initializes a logarithmic scale.

        Args:
            min_val: The minimum value of the scale.
            max_val: The maximum value of the scale.
            base: The base of the logarithm. Defaults to `math.e`.
            bins: The number of bins.
            use_bin_zero: If `False`, the minimum value is part of the first bin.
        """
        _validate_min_max(
            min_val,
            max_val,
            "LogarithmicScale.__init__",
            ["min_val", "max_val"],
            ErrorCode.PVDQCL39,
        )
        _validate_bins(
            bins,
            use_bin_zero,
            "LogarithmicScale.__init__",
            ["bins", "min_in_first_bin"],
            ErrorCode.PVDQCL42,
        )
        super().__init__((min_val, max_val), bins, use_bin_zero)
        self._base = base or math.e

    @property
    def base(self) -> ScaleBaseSpec:
        """Returns the base of the logarithmic scale."""
        return self._base

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "min_val": self._range[0],
            "max_val": self._range[1],
            "base": self._base,
            "bins": self._bins,
            "use_bin_zero": self._use_bin_zero,
        }


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
        bins: BinCountSpec = MAX_BINS,
        use_bin_zero: bool = False,
    ):
        """
        Initializes an exponential scale.

        Args:
            min_val: The minimum value of the scale.
            max_val: The maximum value of the scale.
            base: The base of the exponent. Defaults to `math.e`.
            bins: The number of bins.
            use_bin_zero: If `False`, the minimum value is part of the first bin.
        """
        _validate_min_max(
            min_val,
            max_val,
            "ExponentialScale.__init__",
            ["min_val", "max_val"],
            ErrorCode.PVDQCL40,
        )
        _validate_bins(
            bins,
            use_bin_zero,
            "ExponentialScale.__init__",
            ["bins", "min_in_first_bin"],
            ErrorCode.PVDQCL41,
        )
        super().__init__((min_val, max_val), bins, use_bin_zero)
        self._base = base or math.e

    @property
    def base(self) -> ScaleBaseSpec:
        """Returns the base of the exponential scale."""
        return self._base

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "min_val": self._range[0],
            "max_val": self._range[1],
            "base": self._base,
            "bins": self._bins,
            "use_bin_zero": self._use_bin_zero,
        }


class Classifier(ABC):
    """
    Abstract base class for all data quality classifiers.

    A classifier is applied to one or more columns to produce a classification
    result, which is stored in a new column. It supports tags to allow data quality
    operations to selectively act on specific classifiers.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by this classifier."""
        return _ALL_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL1)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
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
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: A tag or list of tags to associate with the classifier. Tags help
                  filter which classifiers an operation should consider.
        """
        column_names = _normalize_to_list(column_names, keep_none=True)
        if column_names and isinstance(column_names[0], str):
            column_names = [(s, None) for s in column_names]
        self._column_names = column_names
        self._tags = _normalize_to_list(tags, keep_none=True)
        self._on_missing_column = on_missing_column
        self._on_wrong_type = on_wrong_type

    @property
    def column_names(self) -> Optional[list[ColumnNameWithAltSpec]]:
        """
        Returns the list of columns the classifier applies to, including optional
        destination column names.
        """
        return self._column_names

    @property
    def tags(self) -> list[TagSpec] | None:
        """
        Returns the list of tags associated with the classifier.
        """
        return self._tags

    @property
    def on_missing_column(self) -> OnInvalidColumnSpec:
        """
        Returns what do if the column is missing.
        """
        return self._on_missing_column

    @property
    def on_wrong_type(self) -> OnInvalidColumnSpec:
        """
        Returns what do if the column type is incompatible with the classifier.
        """
        return self._on_wrong_type

    @abstractmethod
    def to_dict(self) -> dict:
        pass

    def __repr__(self) -> str:
        return json.dumps(self.to_dict())

    def __str__(self) -> str:
        return json.dumps(self.to_dict())


class BoolClassifier(Classifier):
    """
    Abstract base class for classifiers that produce a boolean result
    (`True` or `False`).
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by boolean classifiers."""
        return _ALL_DTYPES

    @abstractmethod
    def to_dict(self) -> dict:
        pass


def _validate_identity_min_max(
    min_val: ScaleLimitSpec,
    max_val: ScaleLimitSpec,
    fn_name: str,
    fn_params: Union[str, list[str]],
    error_code: ErrorCode,
):
    """Validates that `min_val` is less than `max_val`."""
    if min_val >= max_val:
        msg = (
            f"min={min_val}, max={max_val}, the maximum value must be strictly "
            "greater than the minimum value."
        )
        raise ParameterValueError(error_code, fn_name, fn_params, msg)
    if max_val - min_val > 100:
        msg = f"min={min_val}, max={max_val}, maximum delta is 100."
        raise ParameterValueError(error_code, fn_name, fn_params, msg)


def _validate_min_max(
    min_val: ScaleLimitSpec,
    max_val: ScaleLimitSpec,
    fn_name: str,
    fn_params: Union[str, list[str]],
    error_code: ErrorCode,
):
    """Validates that `min_val` is less than `max_val`."""
    if min_val >= max_val:
        msg = (
            f"min={min_val}, max={max_val}, the maximum value must be strictly "
            "greater than the minimum value."
        )
        raise ParameterValueError(error_code, fn_name, fn_params, msg)


def _validate_bins(
    bins: BinsCriteriaSpec,
    min_in_first_bin: bool,
    fn_name: str,
    fn_params: Union[str, list[str]],
    error_code: ErrorCode,
):
    """Validates the number of bins against the allowed maximum."""
    max_allowed = MAX_BINS if min_in_first_bin else MAX_BINS + 1
    if bins > max_allowed:
        msg = (
            f"bins={bins}, min_in_first_bin={min_in_first_bin}, "
            f"exceeds the maximum allowed bins of {max_allowed}."
        )
        raise ParameterValueError(error_code, fn_name, fn_params, msg)


class Categorizer(Classifier):
    """
    A classifier that bucketizes data into bins based on a given scale.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by categorizers."""
        return _NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL2)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        scale: Scale,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes a categorizer.

        Args:
            column_names: The column(s) to categorize.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            scale: The scale to use for categorization.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)
        self._scale = scale

    @property
    def scale(self) -> Scale:
        """Returns the scale used for categorization."""
        return self._scale

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "scale": self._scale.to_dict(),
            "tags": self._tags,
        }


def _validate_pattern(pattern: str, fn_name, error_code: ErrorCode) -> str:
    """
    Validates a regular expression pattern.
    """
    try:
        re.compile(pattern)
        return pattern
    except re.error:
        msg = f"Invalid regex pattern '{pattern}': {re.error}."
        raise ParameterValueError(error_code, fn_name, "pattern", msg)


# noinspection DuplicatedCode
class IsNull(BoolClassifier):
    """
    A boolean classifier that checks if a value is `NULL`.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNull classifier."""
        return _ALL_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL17)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsNull` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, "ignore", tags=tags)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "tags": self._tags,
        }


class IsNotNull(BoolClassifier):
    """
    A boolean classifier that checks if a value is *not* `NULL`.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNotNull classifier."""
        return _ALL_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL15)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsNotNull` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, "ignore", tags=tags)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "tags": self._tags,
        }


class IsNan(BoolClassifier):
    """
    A boolean classifier that checks if a value is `NaN` (Not a Number).
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNan classifier."""
        return _FLOAT_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL14)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsNan` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "tags": self._tags,
        }


class IsNotNan(BoolClassifier):
    """
    A boolean classifier that checks if a value is *not* `NaN`.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNotNan classifier."""
        return _FLOAT_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL14)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsNotNan` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "tags": self._tags,
        }


class IsNullOrNan(BoolClassifier):
    """
    A boolean classifier that checks if a value is either `NULL` or `NaN`.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNullOrNan classifier."""
        return _FLOAT_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL18)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsNullOrNan` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "tags": self._tags,
        }


class IsNotNullNorNan(BoolClassifier):
    """
    A boolean classifier that checks if a value is neither `NULL` nor `NaN`.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNotNullNorNan classifier."""
        return _FLOAT_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL15)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsNotNullNorNan` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "tags": self._tags,
        }


# noinspection DuplicatedCode
class IsZero(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is zero.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsZero classifier."""
        return _NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL21)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsZero` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "tags": self._tags,
        }


class IsNotZero(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is not zero.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNotZero classifier."""
        return _NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL16)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsNotZero` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "tags": self._tags,
        }


class IsPositive(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is positive.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsPositive classifier."""
        return _SIGNED_NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL19)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsPositive` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "tags": self._tags,
        }


class IsPositiveOrZero(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is positive or zero.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsPositiveOrZero classifier."""
        return _SIGNED_NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL20)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsPositiveOrZero` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "tags": self._tags,
        }


class IsNegative(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is negative.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNegative classifier."""
        return _SIGNED_NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL14)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsNegative` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "tags": self._tags,
        }


class IsNegativeOrZero(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is negative or zero.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNegativeOrZero classifier."""
        return _SIGNED_NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL14)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsNegativeOrZero` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "tags": self._tags,
        }


# noinspection DuplicatedCode
class IsBetween(BoolClassifier):
    """
    A boolean classifier that checks if a value is within a specified range.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsBetween classifier."""
        return _ALL_NON_BOOLEAN_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL10)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        min_val: Optional[RangeValueSpec] = None,
        max_val: Optional[RangeValueSpec] = None,
        closed: RangeClosedOnSpec = "both",
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsBetween` classifier.

        Args:
            column_names: The column(s) to check.
            min_val: The minimum value of the range.
            max_val: The maximum value of the range.
            closed: Specifies if the range is inclusive ("both", "lower", "upper") or
                exclusive ("none").
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)
        _validate_range(min_val, max_val, "IsBetween.__init__", ErrorCode.PVDQCL11)
        self._min = min_val
        self._max = max_val
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

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "min_len": self._min,
            "max_len": self._max,
            "closed": self._closed,
            "tags": self._tags,
        }


# noinspection DuplicatedCode
class IsNotBetween(BoolClassifier):
    """
    A boolean classifier that checks if a value is *outside* a specified range.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNotBetween classifier."""
        return _ALL_NON_BOOLEAN_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL12)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        min_val: Optional[RangeValueSpec] = None,
        max_val: Optional[RangeValueSpec] = None,
        closed: RangeClosedOnSpec = "both",
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsNotBetween` classifier.

        Args:
            column_names: The column(s) to check.
            min_val: The minimum value of the range.
            max_val: The maximum value of the range.
            closed: Specifies if the range is inclusive ("both", "lower", "upper") or
                exclusive ("none").
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)
        _validate_range(min_val, max_val, "IsNotBetween.__init__", ErrorCode.PVDQCL11)
        self._min = min_val
        self._max = max_val
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

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "min_len": self._min,
            "max_len": self._max,
            "closed": self._closed,
            "tags": self._tags,
        }


# noinspection DuplicatedCode
class IsIn(BoolClassifier):
    """
    A boolean classifier that checks if a value is in a specified set of values.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsIn classifier."""
        return _ALL_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL9)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        *,
        values: Collection,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsIn` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            values: The collection of values to check against.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, "ignore", tags=tags)
        self._values = _normalize_to_set(values)

    @property
    def values(self) -> AbstractSet:
        """Returns the set of values for the check."""
        return self._values

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "values": list(self._values),
            "tags": self._tags,
        }


# noinspection DuplicatedCode
class IsNotIn(BoolClassifier):
    """
    A boolean classifier that checks if a value is *not* in a specified set of values.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNotIn classifier."""
        return _ALL_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL10)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        *,
        values: Collection,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsNotIn` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            values: The collection of values to check against.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, "ignore", tags=tags)
        self._values = _normalize_to_set(values)

    @property
    def values(self) -> AbstractSet:
        """Returns the set of values for the check."""
        return self._values

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "values": list(self._values),
            "tags": self._tags,
        }


# noinspection DuplicatedCode
class Matches(BoolClassifier):
    """
    A boolean classifier that checks if a value matches a regex pattern.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by Matches classifier."""
        return _STRING_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL22)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        pattern: str,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `Matches` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            pattern: The regular expression to match against.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)
        self._pattern = _validate_pattern(
            pattern,
            "Matches.__init__",
            ErrorCode.PVDQCL35,
        )

    @property
    def pattern(self) -> str:
        """Returns the regex pattern."""
        return self._pattern

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "pattern": self._pattern,
            "tags": self._tags,
        }


# noinspection DuplicatedCode
class DoesNotMatch(BoolClassifier):
    """
    A boolean classifier that checks if a value does *not* match a regex pattern.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by DoesNotMatch classifier."""
        return _STRING_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL3)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        pattern: str,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `DoesNotMatch` classifier.

        Args:
            column_names: The column(s) to apply the check to.
            pattern: The regular expression to not match against.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)
        self._pattern = _validate_pattern(
            pattern,
            "DoesNotMatch.__init__",
            ErrorCode.PVDQCL36,
        )

    @property
    def pattern(self) -> str:
        """Returns the regex pattern."""
        return self._pattern

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "pattern": self._pattern,
            "tags": self._tags,
        }


class HasLength(BoolClassifier):
    """
    A boolean classifier that checks if the length of a value is within a given range.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by HasLength classifier."""
        return _STRING_DTYPES

    @_td_validate_call(ErrorCode.PVDQCL7)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        min_len: LenSpec = 0,
        max_len: LenSpec = sys.maxsize,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `HasLength` classifier.

        Args:
            column_names: The column(s) to check.
            min_len: The minimum allowed length (inclusive).
            max_len: The maximum allowed length (inclusive).
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(column_names, on_missing_column, on_wrong_type, tags=tags)
        if min_len > max_len:
            msg = f" min='{min_len}' is greater than max='{max_len}'."
            raise ParameterValueError(
                ErrorCode.PVDQCL8,
                "HasLength.__init__",
                "min_len, max_len",
                msg,
            )
        self._min = min_len
        self._max = max_len

    @property
    def min(self) -> int:
        """Returns the minimum allowed length."""
        return self._min

    @property
    def max(self) -> int:
        """Returns the maximum allowed length."""
        return self._max

    def to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "min_len": self._min,
            "max_len": self._max,
            "tags": self._tags,
        }


def _validate_range(
    min_len: Optional[RangeValueSpec],
    max_len: Optional[RangeValueSpec],
    fn_name: str,
    error_code: ErrorCode,
):
    if min_len is not None and max_len is not None:
        if type(min_len) is not type(max_len):
            msg = (
                f" min_len={min_len} and max_len={max_len}, "
                "values must be of the same type."
            )
            raise ParameterValueError(
                error_code,
                fn_name,
                ["min_len", "max_len"],
                msg,
            )
        if min_len > max_len:
            msg = f" min_len={min_len} is greater than max_len={max_len}."
            raise ParameterValueError(
                error_code,
                fn_name,
                ["min_len", "max_len"],
                msg,
            )

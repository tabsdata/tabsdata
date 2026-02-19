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
from abc import ABC
from typing import AbstractSet, Collection, FrozenSet, Type, Union

from tabsdata._utils.validation import (
    _normalize_to_list,
    _normalize_to_set,
    _td_validate_call,
)
from tabsdata.dataquality._typing import (
    MAX_BINS,
    BinCountSpec,
    BinsCriteriaSpec,
    ColumNamesWithAltSpec,
    ColumnNameWithAltSpec,
    LenSpec,
    OnInvalidColumnSpec,
    OnInvalidValueSpec,
    RangeClosedOnSpec,
    RangeValueSpec,
    ScaleBaseSpec,
    ScaleIdentityLimitSpec,
    ScaleLimitSpec,
    ScalePowerSpec,
    TagSpec,
    TagsSpec,
)
from tabsdata.exceptions import (
    ErrorCode,
    ParameterValueError,
)

# noinspection PyProtectedMember
from tabsdata.tableframe.dtypes import (
    _ALL_DTYPES,
    _ALL_NON_BOOLEAN_DTYPES,
    _BOOLEAN_DTYPES,
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
        bins: BinCountSpec,
        use_bin_zero: bool,
    ):
        self._range = None
        self._scale_bins = None
        self._use_bin_zero = None

        self._scale_range = scale_range

        class_name = "TODO"  # TODO
        _validate_bins(
            bins,
            use_bin_zero,
            f"{class_name}.bins",
            ["bins", "use_bin_zero"],
            ErrorCode.DQC101,
        )
        self._scale_bins = bins
        self._use_bin_zero = use_bin_zero

    @property
    def _scale_range(self) -> tuple[ScaleLimitSpec, ScaleLimitSpec]:
        """Returns the (min, max) range of the scale."""
        return self._range

    @_scale_range.setter
    @_td_validate_call(ErrorCode.DQC101)
    def _scale_range(self, scale_range: tuple[ScaleLimitSpec, ScaleLimitSpec]):
        """Sets the (min, max) range of the scale."""
        class_name = "TODO"  # TODO
        _validate_min_max(
            scale_range[0],
            scale_range[1],
            f"{class_name}.scale_range",
            ["scale_range"],
            ErrorCode.DQC107,
        )
        self._range = scale_range

    @property
    def _bins(self) -> BinCountSpec:
        """Returns the number of bins in the scale."""
        return self._scale_bins

    @_bins.setter
    @_td_validate_call(ErrorCode.DQC102)
    def _bins(self, value: BinCountSpec):
        """Sets the number of bins in the scale."""
        class_name = "TODO"  # TODO
        _validate_bins(
            value,
            self.use_bin_zero,
            f"{class_name}.bins",
            ["value"],
            ErrorCode.DQC103,
        )
        self._scale_bins = value

    @property
    def use_bin_zero(self) -> bool:
        """Returns whether the minimum value is in bin zero or in bin one."""
        return self._use_bin_zero

    @use_bin_zero.setter
    @_td_validate_call(ErrorCode.DQC104)
    def use_bin_zero(self, value: bool):
        """Sets whether the minimum value is in bin zero or in bin one."""
        class_name = "TODO"  # TODO
        _validate_bins(
            self._bins,
            value,
            f"{class_name}.use_bin_zero",
            ["value"],
            ErrorCode.DQC105,
        )
        self._use_bin_zero = value

    def _to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "min_val": self._range[0],
            "max_val": self._range[1],
            "bins": self._bins,
            "use_bin_zero": self._use_bin_zero,
        }

    def __repr__(self) -> str:
        return json.dumps(self._to_dict())

    def __str__(self) -> str:
        return json.dumps(self._to_dict())

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return vars(self) == vars(other)


class IdentityScale(Scale):
    """
    An identity scale where each integer value corresponds to a bin.
    """

    @_td_validate_call(ErrorCode.DQC106)
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
        super().__init__((min_val, max_val), max_val - min_val, use_bin_zero)

    @property
    def scale_range(self) -> tuple[ScaleIdentityLimitSpec, ScaleIdentityLimitSpec]:
        """Returns the (min, max) range of the scale."""
        return super()._scale_range

    @scale_range.setter
    @_td_validate_call(ErrorCode.DQC107)
    def scale_range(
        self, scale_range: tuple[ScaleIdentityLimitSpec, ScaleIdentityLimitSpec]
    ):
        """Sets the (min, max) range of the scale."""
        self._scale_range = scale_range
        self._bins = scale_range[1] - scale_range[0]

    @property
    def bins(self):
        """Returns the number of bins in the scale."""
        return self._bins


class NonIdentityScale(Scale, ABC):
    """
    A non identity scale that divides a range into equally sized bins.
    """

    def __init__(
        self,
        scale_range: tuple[ScaleLimitSpec, ScaleLimitSpec],
        bins: BinCountSpec = MAX_BINS,
        use_bin_zero: bool = False,
    ):
        """
        Initializes a linear scale.

        Args:
            scale_range: The scale range.
            bins: The number of bins to create.
            use_bin_zero: If `False`, the minimum value is part of the first bin.
        """
        super().__init__(scale_range, bins, use_bin_zero)

    @property
    def scale_range(self) -> tuple[ScaleLimitSpec, ScaleLimitSpec]:
        """Returns the (min, max) range of the scale."""
        return super()._scale_range

    @scale_range.setter
    @_td_validate_call(ErrorCode.DQC108)
    def scale_range(self, value: tuple[ScaleLimitSpec, ScaleLimitSpec]):
        """Sets the (min, max) range of the scale."""
        self._scale_range = value

    @property
    def bins(self) -> BinCountSpec | None:
        """Returns the number of bins in the scale."""
        return self._bins

    @bins.setter
    @_td_validate_call(ErrorCode.DQC109)
    def bins(self, value: BinCountSpec):
        """Sets the number of bins to create."""
        self._bins = value


class LinearScale(NonIdentityScale):
    @_td_validate_call(ErrorCode.DQC110)
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
        super().__init__((min_val, max_val), bins, use_bin_zero)


class MonomialScale(NonIdentityScale):
    """
    A monomial (power-law) scale.
    """

    @_td_validate_call(ErrorCode.DQC111)
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
        super().__init__((min_val, max_val), bins, use_bin_zero)
        self._power = power

    @property
    def power(self) -> ScalePowerSpec:
        """Returns the power of the monomial scale."""
        return self._power

    @power.setter
    @_td_validate_call(ErrorCode.DQC112)
    def power(self, value: ScalePowerSpec):
        """Sets the power of the monomial scale."""
        self._power = value

    def _to_dict(self) -> dict:
        d = super()._to_dict()
        d["power"] = self._power
        return d


# noinspection DuplicatedCode
class LogarithmicScale(NonIdentityScale):
    """
    A logarithmic scale.
    """

    @_td_validate_call(ErrorCode.DQC113)
    def __init__(
        self,
        min_val: ScaleLimitSpec,
        max_val: ScaleLimitSpec,
        base: ScaleBaseSpec | None = None,
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
        super().__init__((min_val, max_val), bins, use_bin_zero)
        self._base = base or math.e

    @property
    def base(self) -> ScaleBaseSpec:
        """Returns the base of the logarithmic scale."""
        return self._base

    @base.setter
    @_td_validate_call(ErrorCode.DQC114)
    def base(self, value: ScaleBaseSpec):
        """Sets the base of the logarithmic scale."""
        self._base = value

    def _to_dict(self) -> dict:
        d = super()._to_dict()
        d["base"] = self._base
        return d


# noinspection DuplicatedCode
class ExponentialScale(NonIdentityScale):
    """
    An exponential scale.
    """

    @_td_validate_call(ErrorCode.DQC115)
    def __init__(
        self,
        min_val: ScaleLimitSpec,
        max_val: ScaleLimitSpec,
        base: ScaleBaseSpec | None = None,
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
        super().__init__((min_val, max_val), bins, use_bin_zero)
        self._base = base or math.e

    @property
    def base(self) -> ScaleBaseSpec:
        """Returns the base of the exponential scale."""
        return self._base

    @base.setter
    @_td_validate_call(ErrorCode.DQC116)
    def base(self, value: ScaleBaseSpec):
        """Sets the base of the exponential scale."""
        self._base = value

    def _to_dict(self) -> dict:
        d = super()._to_dict()
        d["base"] = self._base
        return d


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

    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        on_wrong_value: OnInvalidValueSpec = "ignore",
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
            on_wrong_value: Behavior when a classifier's value is incompatible
                            with the column type.
            tags: A tag or list of tags to associate with the classifier. Tags help
                  filter which classifiers an operation should consider.
        """
        self.column_names = column_names
        self.tags = _normalize_to_list(tags, keep_none=True)
        self.on_missing_column = on_missing_column
        self.on_wrong_type = on_wrong_type
        self.on_wrong_value = on_wrong_value

    @property
    def column_names(self) -> list[ColumnNameWithAltSpec] | None:
        """
        Returns the list of columns the classifier applies to, including optional
        destination column names.
        """
        return self._column_names

    @column_names.setter
    @_td_validate_call(ErrorCode.DQC117)
    def column_names(self, column_names: ColumNamesWithAltSpec | None):
        column_names = _normalize_to_list(column_names, keep_none=True)
        if column_names and isinstance(column_names[0], str):
            column_names = [(s, None) for s in column_names]
        self._column_names = column_names

    @property
    def tags(self) -> list[TagSpec] | None:
        """
        Returns the list of tags associated with the classifier.
        """
        return self._tags

    @tags.setter
    @_td_validate_call(ErrorCode.DQC118)
    def tags(self, tags: TagsSpec | None = None):
        """Sets the tags for the classifier.

        Args:
            tags: A single tag or a list of tags.
        """
        self._tags = _normalize_to_list(tags, keep_none=True)

    @property
    def on_missing_column(self) -> OnInvalidColumnSpec:
        """
        Returns what do if the column is missing.
        """
        return self._on_missing_column

    @on_missing_column.setter
    @_td_validate_call(ErrorCode.DQC119)
    def on_missing_column(self, on_missing_column: OnInvalidColumnSpec):
        self._on_missing_column = on_missing_column

    @property
    def on_wrong_type(self) -> OnInvalidColumnSpec:
        """
        Returns what do if the column type is incompatible with the classifier.
        """
        return self._on_wrong_type

    @on_wrong_type.setter
    @_td_validate_call(ErrorCode.DQC120)
    def on_wrong_type(self, on_wrong_type: OnInvalidColumnSpec):
        self._on_wrong_type = on_wrong_type

    @property
    def on_wrong_value(self) -> OnInvalidValueSpec:
        """
        Returns what do if the column type is incompatible with the classifier.
        """
        return self._on_wrong_value

    @on_wrong_value.setter
    @_td_validate_call(ErrorCode.DQC153)
    def on_wrong_value(self, on_wrong_value: OnInvalidValueSpec):
        self._on_wrong_value = on_wrong_value

    def _to_dict(self) -> dict:
        return {
            "type": self.__class__.__name__,
            "column_names": self._column_names,
            "on_missing_column": self._on_missing_column,
            "on_wrong_type": self._on_wrong_type,
            "tags": self._tags,
        }

    def __repr__(self) -> str:
        return json.dumps(self._to_dict())

    def __str__(self) -> str:
        return json.dumps(self._to_dict())


class BoolClassifier(Classifier, ABC):
    """
    Abstract base class for classifiers that produce a boolean result
    (`True` or `False`).
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by boolean classifiers."""
        return _ALL_DTYPES


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


class Categorizer(Classifier, ABC):
    """
    A base class for categorizers.
    """

    pass


class ScaleCategorizer(Categorizer):
    """
    A classifier that bucketizes data into bins based on a given scale.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by categorizers."""
        return _NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.DQC121)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        on_wrong_scale_value: OnInvalidValueSpec = "ignore",
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
            on_wrong_scale_value: Behavior when the scale's min or max values
                                  are incompatible with the column type.
            scale: The scale to use for categorization.
            tags: Optional tags for the classifier.
        """
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value=on_wrong_scale_value,
            tags=tags,
        )
        self._scale = scale

    @property
    def scale(self) -> Scale:
        """Returns the scale used for categorization."""
        return self._scale

    @scale.setter
    @_td_validate_call(ErrorCode.DQC122)
    def scale(self, scale: Scale):
        """Returns the scale used for categorization."""
        self._scale = scale

    def _to_dict(self) -> dict:
        d = super()._to_dict()
        # noinspection PyProtectedMember
        d["scale"] = self._scale._to_dict()
        return d


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


class IsTrue(BoolClassifier):
    """
    A boolean classifier that checks if a value is `True`.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsTrue classifier."""
        return _BOOLEAN_DTYPES

    @_td_validate_call(ErrorCode.DQC154)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsTrue` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            tags: Optional tags for the classifier.
        """
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )


class IsFalse(BoolClassifier):
    """
    A boolean classifier that checks if a value is `False`.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsFalse classifier."""
        return _BOOLEAN_DTYPES

    @_td_validate_call(ErrorCode.DQC155)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsFalse` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            tags: Optional tags for the classifier.
        """
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )


# noinspection DuplicatedCode
class IsNull(BoolClassifier):
    """
    A boolean classifier that checks if a value is `NULL`.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNull classifier."""
        return _ALL_DTYPES

    @_td_validate_call(ErrorCode.DQC123)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type="ignore",
            on_wrong_value="ignore",
            tags=tags,
        )


class IsNotNull(BoolClassifier):
    """
    A boolean classifier that checks if a value is *not* `NULL`.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNotNull classifier."""
        return _ALL_DTYPES

    @_td_validate_call(ErrorCode.DQC124)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type="ignore",
            on_wrong_value="ignore",
            tags=tags,
        )


class IsNan(BoolClassifier):
    """
    A boolean classifier that checks if a value is `NaN` (Not a Number).
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNan classifier."""
        return _FLOAT_DTYPES

    @_td_validate_call(ErrorCode.DQC125)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )


class IsNotNan(BoolClassifier):
    """
    A boolean classifier that checks if a value is *not* `NaN`.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNotNan classifier."""
        return _FLOAT_DTYPES

    @_td_validate_call(ErrorCode.DQC126)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )


class IsNullOrNan(BoolClassifier):
    """
    A boolean classifier that checks if a value is either `NULL` or `NaN`.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNullOrNan classifier."""
        return _FLOAT_DTYPES

    @_td_validate_call(ErrorCode.DQC127)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )


class IsNotNullNorNan(BoolClassifier):
    """
    A boolean classifier that checks if a value is neither `NULL` nor `NaN`.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNotNullNorNan classifier."""
        return _FLOAT_DTYPES

    @_td_validate_call(ErrorCode.DQC128)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )


# noinspection DuplicatedCode
class IsZero(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is zero.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsZero classifier."""
        return _NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.DQC129)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )


class IsNotZero(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is not zero.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNotZero classifier."""
        return _NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.DQC130)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )


class IsPositive(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is positive.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsPositive classifier."""
        return _SIGNED_NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.DQC131)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )


class IsPositiveOrZero(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is positive or zero.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsPositiveOrZero classifier."""
        return _SIGNED_NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.DQC132)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )


class IsNegative(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is negative.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNegative classifier."""
        return _SIGNED_NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.DQC133)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )


class IsNegativeOrZero(BoolClassifier):
    """
    A boolean classifier that checks if a numeric value is negative or zero.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsNegativeOrZero classifier."""
        return _SIGNED_NUMERIC_DTYPES

    @_td_validate_call(ErrorCode.DQC134)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )


# noinspection DuplicatedCode
class BetweenClassifier(BoolClassifier, ABC):
    """
    A boolean classifier that checks if a value is within a specified range.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsBetween classifier."""
        return _ALL_NON_BOOLEAN_DTYPES

    @_td_validate_call(ErrorCode.DQC135)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
        min_val: RangeValueSpec | None = None,
        max_val: RangeValueSpec | None = None,
        closed_on: RangeClosedOnSpec = "both",
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_type: OnInvalidColumnSpec = "ignore",
        on_wrong_value: OnInvalidValueSpec = "ignore",
        *,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsBetween` classifier.

        Args:
            column_names: The column(s) to check.
            min_val: The minimum value of the range.
            max_val: The maximum value of the range.
            closed_on: Specifies if the range is inclusive ("both", "lower", "upper") or
                exclusive ("none").
            on_missing_column: What to do if a column name is missing.
            on_wrong_type: Behavior when a column’s type is incompatible with the
                           classifier.
            on_wrong_value: Behavior when a classifier's value isincompatible with the
                            column type.
            tags: Optional tags for the classifier.
        """
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value=on_wrong_value,
            tags=tags,
        )
        self._min = min_val
        self._max = max_val
        _validate_range(min_val, max_val, "IsBetween.__init__", ErrorCode.DQC136)
        self.closed_on = closed_on

    @property
    def min(self) -> RangeValueSpec | None:
        """Returns the minimum value of the range."""
        return self._min

    @min.setter
    @_td_validate_call(ErrorCode.DQC137)
    def min(self, min_val: RangeValueSpec | None):
        _validate_range(min_val, self._max, "IsBetween.min", ErrorCode.DQC138)
        self._min = min_val

    @property
    def max(self) -> RangeValueSpec | None:
        """Returns the maximum value of the range."""
        return self._max

    @max.setter
    @_td_validate_call(ErrorCode.DQC139)
    def max(self, max_val: RangeValueSpec | None):
        _validate_range(self._min, max_val, "IsBetween.max", ErrorCode.DQC140)
        self._max = max_val

    @property
    def closed_on(self) -> RangeClosedOnSpec:
        """Returns how the range is closed."""
        return self._closed_on

    @closed_on.setter
    @_td_validate_call(ErrorCode.DQC141)
    def closed_on(self, closed_on: RangeClosedOnSpec):
        self._closed_on = closed_on

    def _to_dict(self) -> dict:
        d = super()._to_dict()
        d["min_len"] = self._min
        d["max_len"] = self._max
        d["closed_on"] = self._closed_on
        d["on_wrong_value"] = self._on_wrong_value
        return d


# noinspection DuplicatedCode
class IsBetween(BetweenClassifier):
    """
    A boolean classifier that checks if a value is *inside* a specified range.
    """


class IsNotBetween(BetweenClassifier):
    """
    A boolean classifier that checks if a value is *outside* a specified range.
    """


# noinspection DuplicatedCode
class InClassifier(BoolClassifier, ABC):
    """
    A boolean classifier that checks if a value is in a specified set of values.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by IsIn classifier."""
        return _ALL_DTYPES

    @_td_validate_call(ErrorCode.DQC142)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
        on_missing_column: OnInvalidColumnSpec = "ignore",
        on_wrong_value: OnInvalidValueSpec = "ignore",
        *,
        values: Collection,
        tags: TagsSpec | None = None,
    ):
        """
        Initializes the `IsIn` classifier.

        Args:
            column_names: The column(s) to check.
            on_missing_column: What to do if a column name is missing.
            on_wrong_value: Behavior when a classifier's value isincompatible with the
                            column type.
            values: The collection of values to check against.
            tags: Optional tags for the classifier.
        """
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type="ignore",
            on_wrong_value=on_wrong_value,
            tags=tags,
        )
        self.values = values

    @property
    def values(self) -> AbstractSet:
        """Returns the set of values for the check."""
        return self._values

    @values.setter
    @_td_validate_call(ErrorCode.DQC143)
    def values(self, values: Collection):
        self._values = _normalize_to_set(values)

    def _to_dict(self) -> dict:
        d = super()._to_dict()
        d["values"] = list(self._values)
        d["on_wrong_value"] = self._on_wrong_value
        return d


class IsIn(InClassifier):
    """
    A boolean classifier that checks if a value is in a specified set of values.
    """


# noinspection DuplicatedCode
class IsNotIn(InClassifier):
    """
    A boolean classifier that checks if a value is *not* in a specified set of values.
    """


# noinspection DuplicatedCode
class MatchClassifier(BoolClassifier, ABC):
    """
    A boolean classifier that checks if a value matches a regex pattern.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by Matches classifier."""
        return _STRING_DTYPES

    @_td_validate_call(ErrorCode.DQC144)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )
        self.pattern = pattern

    @property
    def pattern(self) -> str:
        """Returns the regex pattern."""
        return self._pattern

    @pattern.setter
    @_td_validate_call(ErrorCode.DQC145)
    def pattern(self, pattern: str):
        self._pattern = _validate_pattern(
            pattern,
            "Match.pattern",
            ErrorCode.DQC146,
        )

    def _to_dict(self) -> dict:
        d = super()._to_dict()
        d["pattern"] = self._pattern
        return d


class Matches(MatchClassifier):
    """
    A boolean classifier that checks if a value matches a regex pattern.
    """


# noinspection DuplicatedCode
class DoesNotMatch(MatchClassifier):
    """
    A boolean classifier that checks if a value does *not* match a regex pattern.
    """


class HasLength(BoolClassifier):
    """
    A boolean classifier that checks if the length of a value is within a given range.
    """

    @classmethod
    def supported_dtypes(cls) -> FrozenSet[Type]:
        """Returns the set of data types supported by HasLength classifier."""
        return _STRING_DTYPES

    @_td_validate_call(ErrorCode.DQC146)
    def __init__(
        self,
        column_names: ColumNamesWithAltSpec | None = None,
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
        super().__init__(
            column_names=column_names,
            on_missing_column=on_missing_column,
            on_wrong_type=on_wrong_type,
            on_wrong_value="ignore",
            tags=tags,
        )
        self._min = min_len
        self._max = max_len
        _validate_range(min_len, max_len, "HasLength.__init__", ErrorCode.DQC147)

    @property
    def min(self) -> int:
        """Returns the minimum allowed length."""
        return self._min

    @min.setter
    @_td_validate_call(ErrorCode.DQC148)
    def min(self, min_len: LenSpec):
        """Returns the minimum allowed length."""
        _validate_range(min_len, self._max, "HasLength.min", ErrorCode.DQC150)
        self._min = min_len

    @property
    def max(self) -> int:
        """Returns the maximum allowed length."""
        return self._max

    @max.setter
    @_td_validate_call(ErrorCode.DQC149)
    def max(self, max_len: LenSpec):
        """Returns the maximum allowed length."""
        _validate_range(self._min, max_len, "HasLength.max", ErrorCode.DQC151)
        self._max = max_len

    def _to_dict(self) -> dict:
        d = super()._to_dict()
        d["min_len"] = self._min
        d["max_len"] = self._max
        return d


def _validate_range(
    min_len,
    max_len,
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

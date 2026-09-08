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
    PrefixSpec,
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

    :categories: dq-classifier

    A scale defines how a numeric range is divided into bins. Concrete
    scales differ only in how the bin edges are spaced.

    Examples:
        A scale maps a value to a bin for `ScaleCategorizer`:

        .. code-block:: python

            ScaleCategorizer(
                ["amount"],
                scale=LinearScale(0.0, 100.0, bins=10),
            )
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

    Examples:
        One bin per whole value in the range -- here a bin for each score from
        1 to 5:

        .. code-block:: python

            ScaleCategorizer(["score"], scale=IdentityScale(1, 5))
    """

    __kind__ = "dqIdentityScale"

    min_val: ScaleIdentityLimitSpec
    max_val: ScaleIdentityLimitSpec
    use_bin_zero: bool = False

    @property
    def bins(self) -> int:
        """The number of bins, derived from the (min, max) range.

        Examples:
            .. code-block:: python

                IdentityScale(1, 5).bins  # 4
        """
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

    Examples:
        Ten equally wide bins between 0 and 100:

        .. code-block:: python

            ScaleCategorizer(["amount"], scale=LinearScale(0.0, 100.0, bins=10))
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

    Examples:
        Bins whose width grows with a power of the value, so small values are
        resolved more finely:

        .. code-block:: python

            ScaleCategorizer(
                ["amount"],
                scale=MonomialScale(0.0, 100.0, power=2.0, bins=10),
            )
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

    Examples:
        Bins that widen logarithmically -- useful for long-tailed values:

        .. code-block:: python

            ScaleCategorizer(
                ["amount"],
                scale=LogarithmicScale(1.0, 1000.0, base=10.0, bins=3),
            )
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

    Examples:
        Bins that narrow as values grow, the mirror of `LogarithmicScale`:

        .. code-block:: python

            ScaleCategorizer(
                ["amount"],
                scale=ExponentialScale(0.0, 100.0, base=2.0, bins=8),
            )
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

    :categories: dq-classifier

    A classifier evaluates one or more columns and writes a per-row verdict
    into a new column. Collect classifiers in a `DataQuality` action
    (`classifiers=[...]`) alongside the operators that act on their
    verdicts. Concrete classifiers are frozen; build one per rule.

    Concrete classifiers share these construction fields (a few fix
    `on_wrong_type` / `on_wrong_value` and do not expose them):

    Examples:
        Classifiers are the labelling half of a check; pass concrete ones to
        `DataQuality(classifiers=[...])`:

        .. code-block:: python

            DataQuality(
                table="checked",
                classifiers=[IsNotNull(["customer_id"]), IsPositive(["amount"])],
                operators=[Enrich()],
            )
    """

    # devguide-ref: how classifiers/operators compose into a DataQuality check

    __kind__ = "dqClassifier"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        """Returns the set of data types supported by this classifier."""
        return _ALL_DTYPES


class BoolClassifier(Classifier, ABC):
    """Abstract base class for classifiers with a boolean verdict.

    :categories: dq-classifier

    Each row is labelled `True` or `False` (or a reserved sentinel for
    null / nan). Boolean verdicts are consumed by the `AllOk` and
    `AnyFailed` criteria used in `Filter` / `Select` / `Fail`.

    Examples:
        Every boolean classifier labels each row `True` or `False`, one verdict
        column per classified column:

        .. code-block:: python

            IsPositive(["amount"])
    """

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        """Returns the set of data types supported by boolean classifiers."""
        return _ALL_DTYPES


class Categorizer(Classifier, ABC):
    """Abstract base class for classifiers that assign each value to a bin.

    :categories: dq-classifier

    Bin verdicts are consumed by the `InBins` and `NotInBins` criteria.

    Examples:
        A categorizer labels each row with a bin number instead of a boolean;
        `ScaleCategorizer` is the concrete one:

        .. code-block:: python

            ScaleCategorizer(["amount"], scale=LinearScale(0.0, 100.0, bins=4))
    """


@_api.dataclass(DataQualityErrorCode.DQC_154)
class IsTrue(BoolClassifier):
    """A boolean classifier that checks whether a value is `True`. Applies
    to boolean columns.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_true`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsTrue(["is_active"])

        Name the verdict column with a `(column, verdict_column)` tuple:

        .. code-block:: python

            IsTrue([("is_active", "active_ok")])
    """

    __kind__ = "dqIsTrue"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _BOOLEAN_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_155)
class IsFalse(BoolClassifier):
    """A boolean classifier that checks whether a value is `False`. Applies
    to boolean columns.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_false`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsFalse(["is_deleted"])
    """

    __kind__ = "dqIsFalse"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _BOOLEAN_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_123)
class IsNull(BoolClassifier):
    """A boolean classifier that checks whether a value is null. Applies to
    columns of any type.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_null`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_type: Fixed to `"ignore"`: this classifier accepts every
            column type, so there is no wrong type to report.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsNull(["shipped_at"])

        With no columns given it classifies every column of the table:

        .. code-block:: python

            IsNull()
    """

    __kind__ = "dqIsNull"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

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

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_not_null`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_type: Fixed to `"ignore"`: this classifier accepts every
            column type, so there is no wrong type to report.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsNotNull(["customer_id"])

        `on_missing_column` turns a typo in a column name into a failed check
        rather than a silently skipped one:

        .. code-block:: python

            IsNotNull(["customer_id"], on_missing_column="fail")
    """

    __kind__ = "dqIsNotNull"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

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

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_nan`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsNan(["ratio"])
    """

    __kind__ = "dqIsNan"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _FLOAT_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_126)
class IsNotNan(BoolClassifier):
    """A boolean classifier that checks whether a value is not `NaN`.
    Applies to floating-point columns.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_not_nan`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsNotNan(["ratio"])
    """

    __kind__ = "dqIsNotNan"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _FLOAT_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_127)
class IsNullOrNan(BoolClassifier):
    """A boolean classifier that checks whether a value is null or `NaN`.
    Applies to floating-point columns.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_null_or_nan`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsNullOrNan(["ratio"])
    """

    __kind__ = "dqIsNullOrNan"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _FLOAT_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_128)
class IsNotNullNorNan(BoolClassifier):
    """A boolean classifier that checks whether a value is neither null nor
    `NaN`. Applies to floating-point columns.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_not_null_nor_nan`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsNotNullNorNan(["ratio"])

        Tag it to scope an operator to this classifier alone:

        .. code-block:: python

            IsNotNullNorNan(["ratio"], tags=["required"])
    """

    __kind__ = "dqIsNotNullNorNan"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _FLOAT_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_129)
class IsZero(BoolClassifier):
    """A boolean classifier that checks whether a numeric value is zero.
    Applies to numeric columns.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_zero`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsZero(["balance"])
    """

    __kind__ = "dqIsZero"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _NUMERIC_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_130)
class IsNotZero(BoolClassifier):
    """A boolean classifier that checks whether a numeric value is not zero.
    Applies to numeric columns.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_not_zero`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsNotZero(["divisor"])
    """

    __kind__ = "dqIsNotZero"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _NUMERIC_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_131)
class IsPositive(BoolClassifier):
    """A boolean classifier that checks whether a numeric value is positive.
    Applies to signed numeric columns.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_positive`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsPositive(["amount"])
    """

    __kind__ = "dqIsPositive"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _SIGNED_NUMERIC_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_132)
class IsPositiveOrZero(BoolClassifier):
    """A boolean classifier that checks whether a numeric value is positive or
    zero. Applies to signed numeric columns.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_positive_or_zero`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsPositiveOrZero(["balance"])
    """

    __kind__ = "dqIsPositiveOrZero"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _SIGNED_NUMERIC_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_133)
class IsNegative(BoolClassifier):
    """A boolean classifier that checks whether a numeric value is negative.
    Applies to signed numeric columns.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_negative`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsNegative(["adjustment"])
    """

    __kind__ = "dqIsNegative"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _SIGNED_NUMERIC_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_134)
class IsNegativeOrZero(BoolClassifier):
    """A boolean classifier that checks whether a numeric value is negative or
    zero. Applies to signed numeric columns.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_negative_or_zero`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            IsNegativeOrZero(["adjustment"])
    """

    __kind__ = "dqIsNegativeOrZero"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

    on_wrong_value: ClassVar[OnInvalidValueSpec] = "ignore"

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _SIGNED_NUMERIC_DTYPES


@_api.dataclass(DataQualityErrorCode.DQC_135)
class BetweenClassifier(BoolClassifier, ABC):
    """A boolean classifier that checks if a value is within a specified range.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix.
        min_val: Lower bound; `None` leaves the low side unbounded.
        max_val: Upper bound; `None` leaves the high side unbounded. When both
            are given they must be the same type and `min_val` must not exceed
            `max_val`.
        closed_on: Which bounds are inclusive: `"none"`, `"lower"`, `"upper"`
            or `"both"` (the default).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        on_wrong_value: What to do when a value cannot be evaluated against
            this classifier: `"ignore"` (the default) yields a null verdict,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.

    Examples:
        The concrete range classifiers are `IsBetween` and `IsNotBetween`:

        .. code-block:: python

            IsBetween(["amount"], min_val=0.0, max_val=100.0)
            IsNotBetween(["amount"], min_val=0.0, max_val=100.0)
    """

    __kind__ = "dqBetweenClassifier"

    column_names: ColumnNamesWithAltSpec | None = None
    min_val: RangeValueSpec | None = None
    max_val: RangeValueSpec | None = None
    closed_on: RangeClosedOnSpec = "both"
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    on_wrong_value: OnInvalidValueSpec = "ignore"
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

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
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_between`).
        min_val: Lower bound; `None` leaves the low side unbounded.
        max_val: Upper bound; `None` leaves the high side unbounded. When
            both are given they must be the same type and `min_val` must
            not exceed `max_val`.
        closed_on: Which bounds are inclusive: `"none"`, `"lower"`,
            `"upper"` or `"both"` (default `"both"`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        on_wrong_value: What to do when a value cannot be evaluated against
            this classifier: `"ignore"` (the default) yields a null verdict,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.

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
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_not_between`).
        min_val: Lower bound; `None` leaves the low side unbounded.
        max_val: Upper bound; `None` leaves the high side unbounded. When
            both are given they must be the same type and `min_val` must
            not exceed `max_val`.
        closed_on: Which bounds are inclusive: `"none"`, `"lower"`,
            `"upper"` or `"both"` (default `"both"`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        on_wrong_value: What to do when a value cannot be evaluated against
            this classifier: `"ignore"` (the default) yields a null verdict,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.

    Examples:
        .. code-block:: python

            IsNotBetween(["amount"], min_val=0.0, max_val=100.0)

        `closed_on` decides whether the bounds themselves count as inside the
        range:

        .. code-block:: python

            IsNotBetween(
                ["amount"],
                min_val=0.0,
                max_val=100.0,
                closed_on="none",
            )
    """

    __kind__ = "dqIsNotBetween"


@_api.dataclass(DataQualityErrorCode.DQC_142)
class InClassifier(BoolClassifier, ABC):
    """A boolean classifier that checks if a value is in a specified set of values.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix.
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_value: What to do when a value cannot be evaluated against
            this classifier: `"ignore"` (the default) yields a null verdict,
            `"fail"` aborts the check.
        values: Values to test membership against; a non-empty collection,
            deduplicated into a frozenset. `None` may be included to test for
            null membership.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_type: Fixed to `"ignore"`: this classifier accepts every
            column type, so there is no wrong type to report.

    Examples:
        The concrete membership classifiers are `IsIn` and `IsNotIn`:

        .. code-block:: python

            IsIn(["status"], values={"new", "open"})
            IsNotIn(["status"], values={"void"})
    """

    __kind__ = "dqInClassifier"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_value: OnInvalidValueSpec = "ignore"
    values: ValuesSpec = field(kw_only=True)
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

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
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_in`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_value: What to do when a value cannot be evaluated against
            this classifier: `"ignore"` (the default) yields a null verdict,
            `"fail"` aborts the check.
        values: Values to test membership against; a non-empty collection
            (at least one). Include `None` to match nulls; duplicates are
            removed.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_type: Fixed to `"ignore"`: this classifier accepts every
            column type, so there is no wrong type to report.

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
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_is_not_in`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_value: What to do when a value cannot be evaluated against
            this classifier: `"ignore"` (the default) yields a null verdict,
            `"fail"` aborts the check.
        values: Values to test membership against; a non-empty collection
            (at least one). Include `None` to match nulls; duplicates are
            removed.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_type: Fixed to `"ignore"`: this classifier accepts every
            column type, so there is no wrong type to report.

    Examples:
        .. code-block:: python

            IsNotIn(["status"], values={"void", "test"})

        `None` in `values` tests for null membership:

        .. code-block:: python

            IsNotIn(["status"], values={None})
    """

    __kind__ = "dqIsNotIn"


@_api.dataclass(DataQualityErrorCode.DQC_144)
class MatchClassifier(BoolClassifier, ABC):
    """A boolean classifier that checks if a value matches a regex pattern.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix.
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        pattern: Regular expression matched against each value.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        The concrete pattern classifiers are `Matches` and `DoesNotMatch`:

        .. code-block:: python

            Matches(["email"], pattern=r".+@.+")
            DoesNotMatch(["email"], pattern=r"^tmp-")
    """

    __kind__ = "dqMatchClassifier"

    column_names: ColumnNamesWithAltSpec | None = None
    on_missing_column: OnInvalidColumnSpec = "ignore"
    on_wrong_type: OnInvalidColumnSpec = "ignore"
    pattern: str = field(kw_only=True)
    tags: TagsSpec | None = field(default=None, kw_only=True)
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

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
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_matches`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        pattern: Regular expression to match; must be a valid regex.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

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
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_does_not_match`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        pattern: Regular expression to test against; must be a valid regex.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

    Examples:
        .. code-block:: python

            DoesNotMatch(["email"], pattern=r"^tmp-")
    """

    __kind__ = "dqDoesNotMatch"


@_api.dataclass(DataQualityErrorCode.DQC_146)
class HasLength(BoolClassifier):
    """A boolean classifier that checks whether a value's length is within a
    range. Applies to string columns.

    :categories: dq-classifier

    Args:
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_has_length`).
        min_len: Minimum length, inclusive; a non-negative whole number
            (default 0).
        max_len: Maximum length, inclusive; a non-negative whole number.
            Must be the same type as `min_len` and not less than it.
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.
        on_wrong_value: Fixed to `"ignore"`: this classifier takes no value to
            compare against, so there is no wrong value to report.

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
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

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
        column_names: Columns to classify. Each entry is a column name, or a
            `(column, verdict_column)` tuple naming the column the verdict
            lands in; the two forms can be mixed. `None` (the default)
            classifies every user column, skipping the types this classifier
            does not support (see `on_wrong_type`). A verdict column that is
            not named explicitly is the source column plus this classifier's
            suffix (a column `amount` lands in `amount_category`).
        on_missing_column: What to do when a named column is not in the table:
            `"ignore"` (the default) leaves it out, `"fail"` aborts the check.
        on_wrong_type: What to do when a column's data type is not one this
            classifier supports: `"ignore"` (the default) leaves it out,
            `"fail"` aborts the check.
        on_wrong_value: What to do when a value cannot be evaluated against
            this classifier: `"ignore"` (the default) yields a null verdict,
            `"fail"` aborts the check.
        scale: The `Scale` (e.g. `LinearScale`, `LogarithmicScale`)
            that maps values to bins.
        tags: Optional labels for this classifier. An operator given the same
            tag applies only to the classifiers carrying it; untagged operators
            apply to all of them.
        prefix: Prepended verbatim to every verdict column name this
            classifier produces, inferred or explicit. `None` (the default)
            uses `td.dq.`; `""` adds no prefix; any other string is used as
            given.

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
    prefix: PrefixSpec | None = field(default=None, kw_only=True)

    @classmethod
    def supported_dtypes(cls) -> frozenset:
        return _NUMERIC_DTYPES

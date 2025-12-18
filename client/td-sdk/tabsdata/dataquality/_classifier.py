#
# Copyright 2025 Tabs Data Inc.
#
import math
import sys
from abc import ABC
from typing import Collection, Optional

from tabsdata.dataquality._typing import (
    MAX_BINS,
    BinsSpec,
    ColumNamesWithAltSpec,
    ColumnNamesSpec,
    ColumnNameWithAltSpec,
    ColumnTypeSpec,
    ColumnTypesSpec,
    HasColumnsModeSpec,
    LenSpec,
    MaxSpec,
    MinSpec,
    RangeClosedOnSpec,
    ScaleBaseSpec,
    ScaleIdentityLimitSpec,
    ScaleLimitSpec,
    ScalePowerSpec,
    TagSpec,
    TagsSpec,
)
from tabsdata.dataquality._utils import (
    _normalize_to_list,
    _td_validate_call,
    _validation_error,
)
from tabsdata.exceptions import (
    DataQualityConfigurationError,
    ErrorCode,
    ParameterValidationError,
)
from tabsdata.tableframe.typing import TF_DATA_TYPES


class Classifier(ABC):
    """
    Abstract base class for classes that classify data
    """

    @_td_validate_call(ErrorCode.PVDQCL1)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initialize the Classifier object.

        Args:
            column_names: Column name(s) to apply the classifier to and to
            store the classification result.

                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.

                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.

                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.

                Defaults to None.
            tags: Tag(s) for the classifier.

                Tags can be used by DataQuality operations to select which
                classifiers to operate on. If a DataQuality operation does not
                define a tag, it operates on all defined classifiers. If a
                DataQuality operation defines one or more tags, it will only
                operate on classifiers that have at least one of those tags.

                Defaults to None.
        """
        column_names = _normalize_to_list(column_names, keep_none=True)
        if column_names and len(column_names) > 0 and isinstance(column_names[0], str):
            column_names = [(s, None) for s in column_names]
        self._column_names = column_names
        self._tags = _normalize_to_list(tags)

    @property
    def column_names(self) -> Optional[list[ColumnNameWithAltSpec]]:
        """
        :return: The column names the data quality classifier applies to.
            If a `list[str]` is returned, then the data quality column name
            for the classifier will be automatically generated using the
            column names and the classifier type.
            If a `list[(str, str)]` is returned, the first element of the
            tuples is the column name the classifier applies and the second
            element is the data quality column name for the classifier.
        """
        return self._column_names

    @property
    def tags(self) -> list[TagSpec]:
        """
        :return: The classifier tags.
        """
        return self._tags


class BoolClassifier(Classifier):
    """ "
    Abstract base class for classes that classify data into True or False.
    """

    pass


def _validate_min_max(min: ScaleLimitSpec, max: ScaleLimitSpec, fn_name: str):
    if min >= max:
        params_with_error = (
            f"'min' = '{min}', 'max' = '{max}' "
            "error: The maximum must be greater than the minimum."
        )
        raise _validation_error(ErrorCode.PVDQCL32, fn_name, params_with_error)


def _validate_bins(bins: BinsSpec, min_in_first_bin: bool, fn_name: str):
    if min_in_first_bin:
        if bins > MAX_BINS:
            params_with_error = (
                f"'bins' = '{bins}', "
                f"'min_in_first_bin' = '{min_in_first_bin}' "
                f"error: the maximum number of bins is {MAX_BINS}."
            )
            raise _validation_error(ErrorCode.PVDQCL33, fn_name, params_with_error)
    else:
        if bins > MAX_BINS + 1:
            params_with_error = (
                f"'bins' = '{bins}', "
                f"'min_in_first_bin' = '{min_in_first_bin}' "
                f"error: the maximum number of bins is {MAX_BINS + 1}."
            )
            raise _validation_error(ErrorCode.PVDQCL34, fn_name, params_with_error)


class Scale(ABC):
    """
    Base scale class.
    """

    def __init__(
        self,
        range: tuple[ScaleLimitSpec, ScaleLimitSpec],
        bins: Optional[BinsSpec],
        min_in_first_bin: bool,
    ):
        self._range = range
        self._bins = bins
        self._min_in_first_bin = min_in_first_bin

    @property
    def scale_range(self) -> tuple[ScaleLimitSpec, ScaleLimitSpec]:
        return self._range

    @property
    def bins(self) -> Optional[BinsSpec]:
        return self._bins

    @property
    def min_in_first_bin(self):
        return self._min_in_first_bin


class IdentityScale(Scale):

    @_td_validate_call(ErrorCode.PVDQCL23)
    def __init__(
        self,
        min: ScaleIdentityLimitSpec,
        max: ScaleIdentityLimitSpec,
        min_in_first_bin: bool = True,
    ):
        """
        Identity scale.
        """
        _validate_min_max(min, max, f"{self.__class__.__name__}.__init__")
        super().__init__((min, max), max - min, min_in_first_bin)


class LinearScale(Scale):

    @_td_validate_call(ErrorCode.PVDQCL24)
    def __init__(
        self,
        min: ScaleLimitSpec,
        max: ScaleLimitSpec,
        bins: Optional[BinsSpec] = MAX_BINS,
        min_in_first_bin: bool = True,
    ):
        """
        Linear scale.
        """
        _validate_min_max(min, max, f"{self.__class__.__name__}.__init__")
        _validate_bins(bins, min_in_first_bin, f"{self.__class__.__name__}.__init__")
        super().__init__((min, max), bins, min_in_first_bin)


class MonomialScale(Scale):

    @_td_validate_call(ErrorCode.PVDQCL25)
    def __init__(
        self,
        min: ScaleLimitSpec,
        max: ScaleLimitSpec,
        power: ScalePowerSpec,
        bins: Optional[BinsSpec] = MAX_BINS,
        min_in_first_bin: bool = True,
    ):
        """
        Monomial scale.
        """
        _validate_min_max(min, max, f"{self.__class__.__name__}.__init__")
        _validate_bins(bins, min_in_first_bin, f"{self.__class__.__name__}.__init__")
        super().__init__((min, max), bins, min_in_first_bin)
        self._power = power

    @property
    def power(self) -> ScalePowerSpec:
        return self._power


class LogarithmicScale(Scale):

    @_td_validate_call(ErrorCode.PVDQCL26)
    def __init__(
        self,
        min: ScaleLimitSpec,
        max: ScaleLimitSpec,
        base: Optional[ScaleBaseSpec] = None,
        bins: Optional[BinsSpec] = MAX_BINS,
        min_in_first_bin: bool = True,
    ):
        """
        Logarithmic scale.
        """
        _validate_min_max(min, max, f"{self.__class__.__name__}.__init__")
        _validate_bins(bins, min_in_first_bin, f"{self.__class__.__name__}.__init__")
        super().__init__((min, max), bins, min_in_first_bin)
        self._base = base if base else math.e

    @property
    def base(self) -> ScaleBaseSpec:
        return self._base


class ExponentialScale(Scale):

    @_td_validate_call(ErrorCode.PVDQCL27)
    def __init__(
        self,
        min: ScaleLimitSpec,
        max: ScaleLimitSpec,
        base: Optional[ScaleBaseSpec] = None,
        bins: Optional[BinsSpec] = MAX_BINS,
        min_in_first_bin: bool = True,
    ):
        """
        Exponential scale.
        """
        _validate_min_max(min, max, f"{self.__class__.__name__}.__init__")
        _validate_bins(bins, min_in_first_bin, f"{self.__class__.__name__}.__init__")
        super().__init__((min, max), bins, min_in_first_bin)
        self._base = base if base else math.e

    @property
    def base(self) -> ScaleBaseSpec:
        return self._base


class Categorizer(Classifier):
    """
    Categorizer that bucketizes data in bins based on the give scale.
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
        Create a categorizer that categorizes data based on the given scale.

        Args:
            column_names(ColsSpec, optional): Column(s) to categorize.
            scale(Scale): Scale to use for categorization.
            tags(str | list[str] | None, optional): Tag(s) for the classification
                results. Tags allow performing multiple actions, with each
                taking into
                account only classifiers that have certain tag(s).

        """
        super().__init__(column_names, tags=tags)
        self._scale = scale

    @property
    def scale(self) -> Scale:
        return self._scale


class DoesNotMatch(BoolClassifier):
    """
    REGEX NOT MATCH classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL3)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        *,
        regex: str,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names:
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            regex (str): The regex pattern to not match against.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which
                classifiers
                to operate on. If a DataQuality operation does not define a
                tag, it
                operates on all defined classifiers. If a DataQuality
                operation defines
                one or more tags, it will only operate on classifiers
                that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls
        self._regex = regex

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @property
    def regex(self) -> str:
        return self._regex


class HasColumns(BoolClassifier):
    """
    HAS COLUMNS classifier. Checks the given columns exist in the table.
    """

    @_td_validate_call(ErrorCode.PVDQCL4)
    def __init__(
        self,
        column_names: ColumnNamesSpec,
        mode: HasColumnsModeSpec = "at_least",
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(list[str] | str):
                Column name(s) to check for existence.
            mode(Literal["at_least", "exact"], optional): Mode of checking
            columns. If
                "at_least", checks that at least the given columns exist.
                If "exact",
                checks that exactly the given columns exist. Defaults to
                "at_least".
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which
                classifiers
                to operate on. If a DataQuality operation does not define a
                tag, it
                operates on all defined classifiers. If a DataQuality
                operation defines
                one or more tags, it will only operate on classifiers that
                have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._mode = mode

    @property
    def mode(self) -> HasColumnsModeSpec:
        return self._mode


def _validate_column_types(value: list[ColumnTypeSpec]):
    for c_name, c_type in value:
        if c_type not in TF_DATA_TYPES:
            msg = (
                " Parameter validation error. "
                "Function 'HasColumnTypes.__init__', "
                f" 'column_type'=('{c_name}', '{c_type}'): "
                "Invalid data type."
            )
            raise ParameterValidationError(ErrorCode.PVDQCL6, msg)
    return


class HasColumnTypes(BoolClassifier):
    """
    COLUMN TYPES classifier. Checks that the given columns match the given
    types.
    """

    @_td_validate_call(ErrorCode.PVDQCL5)
    def __init__(
        self,
        column_types: ColumnTypesSpec,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_types(list[tuple[str, DataType] | tuple[str, DataType]]):
                Column name(s) and their expected data types to check for.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which
                classifiers
                to operate on. If a DataQuality operation does not define a
                tag, it
                operates on all defined classifiers. If a DataQuality
                operation defines
                one or more tags, it will only operate on classifiers
                that have at least
                one of those tags.
                Defaults to None.
        """
        column_types = _normalize_to_list(column_types)
        column_names = [name for name, _ in _normalize_to_list(column_types)]
        # noinspection PyTypeChecker
        super().__init__(column_names, tags=tags)
        _validate_column_types(column_types)
        self._column_types = column_types

    @property
    def column_types(self) -> list[ColumnTypeSpec]:
        return self._column_types


class HasLen(BoolClassifier):
    """
    HAS LEN classifier. Checks minimum or maximum value length.
    """

    @_td_validate_call(ErrorCode.PVDQCL7)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        min: LenSpec = 0,
        max: LenSpec = sys.maxsize,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names:
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            min (int | float, optional): Minimum length value. Defaults to -inf.
            max (int | float, optional): Maximum length value. Defaults to inf.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which
                 classifiers
                to operate on. If a DataQuality operation does not define a
                tag, it
                operates on all defined classifiers. If a DataQuality
                operation defines
                one or more tags, it will only operate on classifiers that
                have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls
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
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @property
    def min(self) -> int:
        return self._min

    @property
    def max(self) -> int:
        return self._max


class IsIn(BoolClassifier):
    """
    IS IN classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL9)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        *,
        set: Collection,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names:
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            set (Collection): The set of values to check membership against.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which
                classifiers
                to operate on. If a DataQuality operation does not define a
                tag, it
                operates on all defined classifiers. If a DataQuality
                operation defines
                one or more tags, it will only operate on classifiers that
                have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls
        self._set = set

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @property
    def set(self) -> Collection:
        return self._set


class IsNotIn(BoolClassifier):
    """
    NOT IN classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL10)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        *,
        set: Collection,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            set (Collection): The set of values to check membership against.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls
        self._set = set

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @property
    def set(self) -> Collection:
        return self._set


class IsInRange(BoolClassifier):
    """
    IS IN RANGE classifier. Checks that the value is in the given range.
    """

    @_td_validate_call(ErrorCode.PVDQCL10)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        min: MinSpec = float("-inf"),
        max: MaxSpec = float("inf"),
        closed: RangeClosedOnSpec = "both",
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            min (int | float, optional): Minimum length value. Defaults to -inf.
            max (int | float, optional): Maximum length value. Defaults to inf.
            closed (Literal["both", "lower", "none", "upper"], optional): Whether the
                interval is closed on the 'both', 'left', 'right' or 'none' sides.
                Defaults to 'both'.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls
        if min > max:
            msg = (
                " Parameter validation error. "
                "Function 'IsInRange.__init__', "
                f" min='{min}' is greater than max='{max}'."
            )
            raise ParameterValidationError(ErrorCode.PVDQCL11, msg)
        self._min = min
        self._max = max
        self._closed = closed

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @property
    def min(self) -> MinSpec:
        return self._min

    @property
    def max(self) -> MaxSpec:
        return self._max

    @property
    def closed(self) -> RangeClosedOnSpec:
        return self._closed


class IsNotInRange(BoolClassifier):
    """
    NOT IN RANGE classifier. Checks that the value is not in the given range.
    """

    @_td_validate_call(ErrorCode.PVDQCL12)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        min: MinSpec = float("-inf"),
        max: MaxSpec = float("inf"),
        closed: RangeClosedOnSpec = "both",
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            min (int | float, optional): Minimum length value. Defaults to -inf.
            max (int | float, optional): Maximum length value. Defaults to inf.
            closed (Literal["both", "lower", "none", "upper"], optional): Whether the
                interval is closed on the 'both', 'left', 'right' or 'none' sides.
                Defaults to 'both'.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls
        if min > max:
            msg = (
                " Parameter validation error. "
                "Function 'NotInRange.__init__', "
                f" min='{min}' is greater than max='{max}'."
            )
            raise ParameterValidationError(ErrorCode.PVDQCL13, msg)
        self._min = min
        self._max = max
        self._closed = closed

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @property
    def min(self) -> MinSpec:
        return self._min

    @property
    def max(self) -> MaxSpec:
        return self._max

    @property
    def closed(self) -> RangeClosedOnSpec:
        return self._closed


class IsNan(BoolClassifier):
    """
    IS NAN classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL14)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls


class IsNegative(BoolClassifier):
    """
    IS NEGATIVE classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL14)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls


class IsNegativeOrZero(BoolClassifier):
    """
    IS NEGATIVE OR ZERO classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL14)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls


class IsNotNan(BoolClassifier):
    """
    IS NOT NAN classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL14)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls


class IsNotNull(BoolClassifier):
    """
    IS NOT NULL classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL15)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)


class IsNotNullNorNan(BoolClassifier):
    """
    IS NOT NULL AND NOT NAN classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL15)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names=column_names, tags=tags)


class IsNotZero(BoolClassifier):
    """
    IS NOT ZERO classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL16)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self.ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE13, value, type(value))
        self._ignore_nulls = value


class IsNull(BoolClassifier):
    """
    IS NULL classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL17)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)


class IsNullOrNan(BoolClassifier):
    """
    IS NULL OR NAN classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL18)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)


class IsPositive(BoolClassifier):
    """
    IS POSITIVE classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL19)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls


class IsPositiveOrZero(BoolClassifier):
    """
    IS POSITIVE OR ZERO classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL20)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls


class IsZero(BoolClassifier):
    """
    IS ZERO classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL21)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls


class Matches(BoolClassifier):
    """
    REGEX MATCH classifier.
    """

    @_td_validate_call(ErrorCode.PVDQCL22)
    def __init__(
        self,
        column_names: Optional[ColumNamesWithAltSpec] = None,
        ignore_nulls: bool = False,
        *,
        regex: str,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Constructor.

        Args:
            column_names(str | list[str] | tuple[str, str] | list[tuple[str, str]] |
            None, optional):
                Column name(s) to apply the classifier to and to store the
                classification result.
                If a `str | list[str]` is given, those are the column names
                to apply the classifier to. The classification results will be
                stored in columns named 'dq.<column_name>_<classifier_class>`.
                If a `tuple[str, str] | list[tuple[str, str]]` is given, the
                first element of the tuple(s) is the column to apply the
                classifier to and the second element of the tuple(s) is the
                name of the column that will store the classification result.
                If `None`, the classifier will be applied to all columns and
                the results will be stored in columns named
                'dq.<column_name>_<classifier_class>`.
                Defaults to None.
            ignore_nulls (bool, optional): Whether to ignore null values when
                classifying. If True, null values will not be classified.
                Defaults to False.
            regex (str): The regex pattern to match against.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(column_names, tags=tags)
        self._ignore_nulls = ignore_nulls
        self._regex = regex

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @property
    def regex(self) -> str:
        return self._regex

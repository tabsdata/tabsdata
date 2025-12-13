#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Collection, Literal, Self, get_args

from tabsdata.dataquality._utils import _is_collection
from tabsdata.exceptions import (
    DataQualityConfigurationError,
    ErrorCode,
)
from tabsdata.tableframe.typing import TF_DATA_TYPES, DataType

if TYPE_CHECKING:
    from tabsdata.dataquality.typing._typing import ColsSpec, TagsSpec


class Classifier(ABC):
    """
    Abstract base class for classes that classify data
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        *,
        tags: TagsSpec = None,
    ):
        """
        Initialize the Classifier object.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        self.cols = cols
        self.tags = tags

    @property
    def cols(self) -> list[str] | list[tuple[str, str]] | None:
        return self._cols

    @cols.setter
    def cols(self, value: ColsSpec):
        if isinstance(value, str):
            value = [value]
        elif value is None:
            pass
        elif isinstance(value, tuple):
            _is_valid_classifier_tuple(value)
            value = [value]
        elif isinstance(value, list):
            _is_valid_classifier_list(value)
        else:
            raise DataQualityConfigurationError(ErrorCode.DQCE9, value, type(value))
        self._cols = value

    @property
    def tags(self) -> list[str]:
        return self._tags

    @tags.setter
    def tags(self, value: TagsSpec):
        if isinstance(value, str):
            value = [value]
        elif value is None:
            value = []
        elif isinstance(value, list):
            for v in value:
                if not isinstance(v, str):
                    raise DataQualityConfigurationError(ErrorCode.DQCE1, v, type(v))
        else:
            raise DataQualityConfigurationError(ErrorCode.DQCE2, value, type(value))
        self._tags = value


def _is_valid_classifier_tuple(
    value: tuple[str, str],
):
    if len(value) != 2:
        raise DataQualityConfigurationError(ErrorCode.DQCE3, value, len(value))
    if not (isinstance(value[0], str) and isinstance(value[1], str)):
        raise DataQualityConfigurationError(ErrorCode.DQCE4, value)
    return


def _is_valid_classifier_list(value: list):
    current_type = None
    for v in value:
        if isinstance(v, str):
            if not current_type:
                current_type = str
            elif current_type is not str:
                raise DataQualityConfigurationError(ErrorCode.DQCE8, current_type, str)
        elif isinstance(v, tuple):
            if not current_type:
                current_type = tuple
            elif current_type is not tuple:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE8, current_type, tuple
                )
            if len(v) != 2:
                raise DataQualityConfigurationError(ErrorCode.DQCE5, v, len(v))
            if not (isinstance(v[0], str) and isinstance(v[1], str)):
                raise DataQualityConfigurationError(ErrorCode.DQCE6, v)
        else:
            raise DataQualityConfigurationError(ErrorCode.DQCE7, v, type(v))
    return


class BoolClassifier(Classifier):
    """ "
    Abstract base class for classes that classify data into True or False.
    """

    pass


ScaleTypeSpec = Literal["exponential", "linear", "logarithmic", "quadratic"]


class Scale:
    """
    Scale for `Categorizer` classifiers.
    """

    VALID_SCALE_TYPES = get_args(ScaleTypeSpec)

    def __init__(
        self,
        scale_type: ScaleTypeSpec,
        buckets: int,
        scale_range: tuple[int, int] | tuple[float, float],
        scale_tick: int | float | None,
        bucket_names: list[str] = None,
        **kwargs,
    ):
        if not kwargs.get("_internal"):
            raise DataQualityConfigurationError(
                ErrorCode.DQCE57, self.__class__.__name__
            )
        self.scale_type = scale_type
        self.scale_range = scale_range
        self.scale_tick = scale_tick
        self.buckets = buckets
        self.bucket_names = bucket_names

    @property
    def scale_type(self) -> ScaleTypeSpec:
        return self._scale_type

    @scale_type.setter
    def scale_type(self, value: ScaleTypeSpec):
        if value not in self.VALID_SCALE_TYPES:
            raise DataQualityConfigurationError(
                ErrorCode.DQCE47, ", ".join(self.VALID_SCALE_TYPES), value
            )
        self._scale_type = value

    @property
    def scale_range(self) -> tuple[int, int] | tuple[float, float]:
        return self._scale_range

    @scale_range.setter
    def scale_range(self, value: tuple[int, int] | tuple[float | float]):
        if not isinstance(value, tuple):
            raise DataQualityConfigurationError(ErrorCode.DQCE48, value, type(value))
        elif len(value) != 2:
            raise DataQualityConfigurationError(ErrorCode.DQCE49, value, len(value))
        elif not all(isinstance(v, (int, float)) for v in value):
            raise DataQualityConfigurationError(ErrorCode.DQCE50, value)
        self._scale_range = value

    @property
    def scale_tick(self) -> int | float | None:
        return self._scale_tick

    @scale_tick.setter
    def scale_tick(self, value: int | float | None):
        if value is not None and not isinstance(value, (int, float)):
            raise DataQualityConfigurationError(ErrorCode.DQCE51, value, type(value))
        self._scale_tick = value

    @property
    def buckets(self) -> int:
        return self._buckets

    @buckets.setter
    def buckets(self, value: int):
        if not isinstance(value, int):
            raise DataQualityConfigurationError(ErrorCode.DQCE52, value, type(value))
        if value < 2 or value > 100:
            raise DataQualityConfigurationError(ErrorCode.DQCE80, value)
        self._buckets = value
        self._verify_buckets_and_bucket_names()

    @property
    def bucket_names(self) -> list[str] | None:
        return self._bucket_names

    @bucket_names.setter
    def bucket_names(self, value: list[str] | None):
        match value:
            case None:
                self._bucket_names = None
            case list():
                for v in value:
                    if not isinstance(v, str):
                        raise DataQualityConfigurationError(
                            ErrorCode.DQCE54, value, v, type(v)
                        )
                self._bucket_names = value
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE55, value, type(value)
                )
        self._verify_buckets_and_bucket_names()

    def _verify_buckets_and_bucket_names(self):
        if hasattr(self, "_buckets") and hasattr(self, "_bucket_names"):
            buckets = self.buckets
            bucket_names = self.bucket_names
            if self.bucket_names is not None:
                if len(bucket_names) != buckets:
                    raise DataQualityConfigurationError(
                        ErrorCode.DQCE53, buckets, bucket_names, len(bucket_names)
                    )

    @classmethod
    def identity(
        cls,
        buckets: int,
        scale_range: tuple[int, int] | tuple[float, float],
        bucket_names: list[str] = None,
    ) -> Self:
        """
        identity scale.

        If bucket_names are provided, they  can be used with
            `CategoryCriteria.by_name()`.
        """
        return cls(
            scale_type="linear",
            buckets=buckets,
            scale_range=scale_range,
            scale_tick=1,
            bucket_names=bucket_names,
            _internal=True,
        )

    @classmethod
    def linear(
        cls,
        buckets: int,
        scale_range: tuple[int, int] | tuple[float, float],
        bucket_names: list[str] = None,
    ) -> Self:
        """
        linear scale.

        If bucket_names are provided, they  can be used with
            `CategoryCriteria.by_name()`.
        """
        return cls(
            scale_type="linear",
            buckets=buckets,
            scale_range=scale_range,
            scale_tick=1,
            bucket_names=bucket_names,
            _internal=True,
        )

    @classmethod
    def quadratic(
        cls,
        power: int | float,
        buckets: int,
        scale_range: tuple[int, int],
        bucket_names: list[str] = None,
    ) -> Self:
        """
        quadratic scale.

        If bucket_names are provided, they  can be used with
            `CategoryCriteria.by_name()`.
        """
        return cls(
            scale_type="quadratic",
            buckets=buckets,
            scale_range=scale_range,
            scale_tick=power,
            bucket_names=bucket_names,
            _internal=True,
        )

    @classmethod
    def exponential(
        cls,
        base: float | int,
        buckets: int,
        scale_range: tuple[float, float],
        bucket_names: list[str] = None,
    ) -> Self:
        """
        exponential scale.

        If bucket_names are provided, they  can be used with
            `CategoryCriteria.by_name()`.
        """
        return cls(
            scale_type="exponential",
            buckets=buckets,
            scale_range=scale_range,
            scale_tick=base,
            bucket_names=bucket_names,
            _internal=True,
        )

    @classmethod
    def logarithmic(
        cls,
        base: float | int,
        buckets: int,
        scale_range: tuple[float, float],
        bucket_names: list[str] = None,
    ) -> Self:
        """
        logarithmic scale.

        If bucket_names are provided, they  can be used with
            `CategoryCriteria.by_name()`.
        """
        return cls(
            scale_type="logarithmic",
            buckets=buckets,
            scale_range=scale_range,
            scale_tick=base,
            bucket_names=bucket_names,
            _internal=True,
        )


class Categorizer(Classifier):
    """
    Categorizer that bucketizes data based on the give scale.
    """

    def __init__(self, cols: ColsSpec = None, *, scale: Scale, tags: TagsSpec = None):
        """
        Create a categorizer that categorizes data based on the given scale.

        Args:
            cols(ColsSpec, optional): Column(s) to categorize.
            scale(Scale): Scale to use for categorization.
            tags(str | list[str] | None, optional): Tag(s) for the classification results. Tags allow
                performing multiple actions, with each taking into account
                only classifiers that have certain tag(s).

        """
        super().__init__(cols=cols, tags=tags)
        self.scale = scale

    @property
    def scale(self) -> Scale:
        return self._scale

    @scale.setter
    def scale(self, value: Scale):
        if not isinstance(value, Scale):
            raise DataQualityConfigurationError(ErrorCode.DQCE56, value, type(value))
        self._scale = value


class DoesNotMatch(BoolClassifier):
    """
    REGEX NOT MATCH classifier.
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        *,
        regex: str,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(cols=cols, tags=tags)
        self.ignore_nulls = ignore_nulls
        self.regex = regex

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE20, value, type(value))
        self._ignore_nulls = value

    @property
    def regex(self) -> str:
        return self._regex

    @regex.setter
    def regex(self, value: str):
        if not isinstance(value, str):
            raise DataQualityConfigurationError(ErrorCode.DQCE21, value, type(value))
        self._regex = value


class HasColumns(BoolClassifier):
    """
    HAS COLUMNS classifier. Checks the given columns exist in the table.
    """

    VALID_MODES = ["at_least", "exact"]

    def __init__(
        self,
        cols: list[str] | str,
        mode: Literal["at_least", "exact"] = "at_least",
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(list[str] | str):
                Column name(s) to check for existence.
            mode(Literal["at_least", "exact"], optional): Mode of checking columns. If
                "at_least", checks that at least the given columns exist. If "exact",
                checks that exactly the given columns exist. Defaults to "at_least".
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(cols=cols, tags=tags)
        self.mode = mode

    @property
    def cols(self) -> list[str]:
        return self._cols

    @cols.setter
    def cols(self, value: list[str]):
        match value:
            case str():
                value = [value]
            case list():
                for v in value:
                    if not isinstance(v, str):
                        raise DataQualityConfigurationError(
                            ErrorCode.DQCE33, v, type(v)
                        )
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE34, value, type(value)
                )
        self._cols = value

    @property
    def mode(self) -> Literal["at_least", "exact"]:
        return self._mode

    @mode.setter
    def mode(self, value: Literal["at_least", "exact"]):
        if value not in self.VALID_MODES:
            raise DataQualityConfigurationError(
                ErrorCode.DQCE35,
                ", ".join(self.VALID_MODES),
                value,
            )
        self._mode = value


class HasColumnTypes(BoolClassifier):
    """
    COLUMN TYPES classifier. Checks that the given columns match the given types.
    """

    def __init__(
        self,
        cols: list[tuple[str, DataType]] | tuple[str, DataType],
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(list[tuple[str, DataType] | tuple[str, DataType]]):
                Column name(s) and their expected data types to check for.
            tags (str | list[str] | None, optional): Tag(s) for the classifier.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(cols=cols, tags=tags)

    @property
    def cols(self) -> list[tuple[str, DataType]]:
        return self._cols

    @cols.setter
    def cols(self, value: list[tuple[str, DataType]]):
        match value:
            case tuple():
                if len(value) != 2:
                    raise DataQualityConfigurationError(
                        ErrorCode.DQCE43, value, len(value)
                    )
                a, b = value
                if not isinstance(a, str):
                    raise DataQualityConfigurationError(ErrorCode.DQCE36, a, type(a))
                elif b not in TF_DATA_TYPES:
                    raise DataQualityConfigurationError(ErrorCode.DQCE37, b, type(b))
                value = [value]
            case list():
                for v in value:
                    if not isinstance(v, tuple):
                        raise DataQualityConfigurationError(
                            ErrorCode.DQCE38, v, type(v)
                        )
                    elif len(v) != 2:
                        raise DataQualityConfigurationError(ErrorCode.DQCE39, v, len(v))
                    a, b = v
                    if not isinstance(a, str):
                        raise DataQualityConfigurationError(
                            ErrorCode.DQCE40, v, a, type(a)
                        )
                    elif b not in TF_DATA_TYPES:
                        raise DataQualityConfigurationError(
                            ErrorCode.DQCE41, v, b, type(b)
                        )
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE42, value, type(value)
                )
        self._cols = value


class HasLen(BoolClassifier):
    """
    HAS LEN classifier. Checks minimum or maximum value length.
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        min: int | float = float("-inf"),
        max: int | float = float("inf"),
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(cols=cols, tags=tags)
        self.ignore_nulls = ignore_nulls
        self.min = min
        self.max = max

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE28, value, type(value))
        self._ignore_nulls = value

    @property
    def min(self) -> int | float:
        return self._min

    @min.setter
    def min(self, value: int | float):
        if not isinstance(value, (int, float)):
            raise DataQualityConfigurationError(ErrorCode.DQCE22, value, type(value))
        self._min = value

    @property
    def max(self) -> int | float:
        return self._max

    @max.setter
    def max(self, value: int | float):
        if not isinstance(value, (int, float)):
            raise DataQualityConfigurationError(ErrorCode.DQCE23, value, type(value))
        self._max = value


class IsIn(BoolClassifier):
    """
    IS IN classifier.
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        *,
        set: Collection,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)
        self.ignore_nulls = ignore_nulls
        self.set = set

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE29, value, type(value))
        self._ignore_nulls = value

    @property
    def set(self) -> Collection:
        return self._set

    @set.setter
    def set(self, value: Collection):
        if not _is_collection(value):
            raise DataQualityConfigurationError(ErrorCode.DQCE30, value, type(value))
        self._set = value


class IsInRange(BoolClassifier):
    """
    IS IN RANGE classifier. Checks that the value is in the given range.
    """

    VALID_CLOSED_OPTIONS = ["both", "lower", "none", "upper"]

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        min: int | float = float("-inf"),
        max: int | float = float("inf"),
        closed: Literal["both", "lower", "none", "upper"] = "both",
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)
        self.ignore_nulls = ignore_nulls
        self.min = min
        self.max = max
        self.closed = closed

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE24, value, type(value))
        self._ignore_nulls = value

    @property
    def min(self) -> int | float:
        return self._min

    @min.setter
    def min(self, value: int | float):
        if not isinstance(value, (int, float)):
            raise DataQualityConfigurationError(ErrorCode.DQCE25, value, type(value))
        self._min = value

    @property
    def max(self) -> int | float:
        return self._max

    @max.setter
    def max(self, value: int | float):
        if not isinstance(value, (int, float)):
            raise DataQualityConfigurationError(ErrorCode.DQCE26, value, type(value))
        self._max = value

    @property
    def closed(self) -> Literal["both", "lower", "none", "upper"]:
        return self._closed

    @closed.setter
    def closed(self, value: Literal["both", "lower", "none", "upper"]):
        if value not in self.VALID_CLOSED_OPTIONS:
            raise DataQualityConfigurationError(
                ErrorCode.DQCE27,
                ", ".join(self.VALID_CLOSED_OPTIONS),
                value,
            )
        self._closed = value


class IsNan(BoolClassifier):
    """
    IS NAN classifier.
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)
        self.ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE10, value, type(value))
        self._ignore_nulls = value


class IsNegative(BoolClassifier):
    """
    IS NEGATIVE classifier.
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)
        self.ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE16, value, type(value))
        self._ignore_nulls = value


class IsNegativeOrZero(BoolClassifier):
    """
    IS NEGATIVE OR ZERO classifier.
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)
        self.ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE17, value, type(value))
        self._ignore_nulls = value


class IsNotIn(BoolClassifier):
    """
    IS NOT IN classifier.
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        *,
        set: Collection,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)
        self.ignore_nulls = ignore_nulls
        self.set = set

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE31, value, type(value))
        self._ignore_nulls = value

    @property
    def set(self) -> Collection:
        return self._set

    @set.setter
    def set(self, value: Collection):
        if not _is_collection(value):
            raise DataQualityConfigurationError(ErrorCode.DQCE32, value, type(value))
        self._set = value


class IsNotNan(BoolClassifier):
    """
    IS NOT NAN classifier.
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)
        self.ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE11, value, type(value))
        self._ignore_nulls = value


class IsNotNull(BoolClassifier):
    """
    IS NOT NULL classifier.
    """

    def __init__(self, cols: ColsSpec = None, *, tags: TagsSpec = None):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)


class IsNotNullNorNan(BoolClassifier):
    """
    IS NOT NULL AND NOT NAN classifier.
    """

    def __init__(self, cols: ColsSpec = None, *, tags: TagsSpec = None):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)


class IsNotZero(BoolClassifier):
    """
    IS NOT ZERO classifier.
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)
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

    def __init__(self, cols: ColsSpec = None, *, tags: TagsSpec = None):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)


class IsNullOrNan(BoolClassifier):
    """
    IS NULL OR NAN classifier.
    """

    def __init__(self, cols: ColsSpec = None, *, tags: TagsSpec = None):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)


class IsPositive(BoolClassifier):
    """
    IS POSITIVE classifier.
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)
        self.ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE14, value, type(value))
        self._ignore_nulls = value


class IsPositiveOrZero(BoolClassifier):
    """
    IS POSITIVE OR ZERO classifier.
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)
        self.ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE15, value, type(value))
        self._ignore_nulls = value


class IsZero(BoolClassifier):
    """
    IS ZERO classifier.
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        *,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)
        self.ignore_nulls = ignore_nulls

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE12, value, type(value))
        self._ignore_nulls = value


class Matches(BoolClassifier):
    """
    REGEX MATCH classifier.
    """

    def __init__(
        self,
        cols: ColsSpec = None,
        ignore_nulls: bool = False,
        *,
        regex: str,
        tags: TagsSpec = None,
    ):
        """
        Constructor.

        Args:
            cols(str | list[str] | tuple[str, str] | list[tuple[str, str]] | None,
            optional):
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
        super().__init__(cols=cols, tags=tags)
        self.ignore_nulls = ignore_nulls
        self.regex = regex

    @property
    def ignore_nulls(self) -> bool:
        return self._ignore_nulls

    @ignore_nulls.setter
    def ignore_nulls(self, value: bool):
        if not isinstance(value, bool):
            raise DataQualityConfigurationError(ErrorCode.DQCE18, value, type(value))
        self._ignore_nulls = value

    @property
    def regex(self) -> str:
        return self._regex

    @regex.setter
    def regex(self, value: str):
        if not isinstance(value, str):
            raise DataQualityConfigurationError(ErrorCode.DQCE19, value, type(value))
        self._regex = value

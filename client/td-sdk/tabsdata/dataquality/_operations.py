#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Literal, Optional, Self, get_args

from sqlalchemy.sql.functions import count

from tabsdata.dataquality._utils import _is_collection
from tabsdata.exceptions import DataQualityConfigurationError, ErrorCode
from tabsdata.typing._specs import BucketsSpec, TableSpec

if TYPE_CHECKING:
    from tabsdata.dataquality.typing._typing import TagsSpec


class Operation(ABC):
    """
    Base abstract class for data quality operations.
    """

    def __init__(self, *, tags: TagsSpec = None):
        """
        Constructor.

        Args:
            tags: Tag(s) for the operation.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        self.tags = tags

    @property
    def tags(self) -> list[str]:
        """
        :return: the data quality operation tags.
        """
        return self._tags

    # noinspection DuplicatedCode
    @tags.setter
    def tags(self, value: TagsSpec):
        if isinstance(value, str):
            value = [value]
        elif value is None:
            value = []
        elif isinstance(value, list):
            for v in value:
                if not isinstance(v, str):
                    raise DataQualityConfigurationError(ErrorCode.DQCE44, v, type(v))
        else:
            raise DataQualityConfigurationError(ErrorCode.DQCE45, value, type(value))
        self._tags = value


CriteriaTypeSpec = Literal["all_ok", "any_failed", "in_buckets", "not_in_buckets"]


class Criteria:
    """
    Data quality operations define a classifiers criteria for the records
    of the table. Only records that match the criteria are used for the
    data quality operation.

    Possible criteria are:

        * `all_ok`: all boolean classifiers are `True`.
        * `any_failed`: any boolean classifier is `False`.
        * `in_buckets`: for the specified columns, rows that are
    """

    VALID_CRITERIA_TYPES = get_args(CriteriaTypeSpec)

    def __init__(self):
        raise DataQualityConfigurationError(ErrorCode.DQCE58, self.__class__.__name__)

    def _init_boolean(
        self,
        criteria_type: CriteriaTypeSpec,
    ):
        self.criteria_type = criteria_type
        self.column_names = None
        self.buckets = None

    def _init_categories(
        self,
        criteria_type: CriteriaTypeSpec,
        column_names: list[str],
        buckets: BucketsSpec,
    ):
        self.criteria_type = criteria_type
        self.column_names = column_names
        self.buckets = buckets

    @property
    def criteria_type(self) -> CriteriaTypeSpec:
        """
        :return: the criteria type.
        """
        return self._criteria_type

    @criteria_type.setter
    def criteria_type(self, value: CriteriaTypeSpec):
        if value not in self.VALID_CRITERIA_TYPES:
            raise DataQualityConfigurationError(
                ErrorCode.DQCE59, ", ".join(self.VALID_CRITERIA_TYPES), value
            )
        self._criteria_type = value

    @property
    def column_names(self) -> list[str] | None:
        """
        :return: The column names the criteria is applied to. Only used with
        `in_bucket` and `not_in_bucket` criteria. For other criteria is
        `None`.
        """
        return self._column_names

    @column_names.setter
    def column_names(self, column_names: Optional[TableSpec]):
        if isinstance(column_names, str):
            column_names = [column_names]
        if not _is_collection(column_names) and column_names is not None:
            raise DataQualityConfigurationError(
                ErrorCode.DQCE60, column_names, type(column_names)
            )
        self._column_names = column_names

    @property
    def buckets(self) -> BucketsSpec:
        """
        :return: The category buckets used by the criteria. Only used with
        `in_bucket` and `not_in_bucket` criteria. For other criteria is
        `None`.
        """
        return self._buckets

    @buckets.setter
    def buckets(self, value: BucketsSpec):
        if not _is_collection(value) and value is not None:
            raise DataQualityConfigurationError(ErrorCode.DQCE60, value, type(value))
        self._buckets = value

    @classmethod
    def all_ok(cls) -> Self:
        """
        :return: Creates a criteria that requires all boolean classifiers to
        be `True` for the record to be considered.
        """
        c = cls.__new__(cls)
        c._init_boolean(criteria_type="all_ok")
        return c

    @classmethod
    def any_failed(cls) -> Self:
        """
        :return: Creates a criteria that requires at least one boolean
        classifier to be `False` for the record to be considered.
        """
        c = cls.__new__(cls)
        c._init_boolean(criteria_type="any_failed")
        return c

    @classmethod
    def in_buckets(cls, column_names: TableSpec, buckets: BucketsSpec) -> Self:
        """
        :return: Creates a criteria for the category classifiers of the
        specified columns. For the record to be considered, it requires
        the value in the columns to be in one of the specified buckets
        (given either by index or name).
        """
        if isinstance(column_names, str):
            column_names = [column_names]
        if not _is_collection(buckets):
            raise DataQualityConfigurationError(
                ErrorCode.DQCE61, buckets, type(buckets)
            )
        c = cls.__new__(cls)
        c._init_categories("in_buckets", column_names, buckets)
        return c

    @classmethod
    def not_in_buckets(cls, column_names: TableSpec, buckets: BucketsSpec) -> Self:
        """
        :return: Creates a criteria for the category classifiers of the
        specified columns. For the record to be considered, it requires
        the value in the columns not to be in any of the specified
        buckets (given either by index or name).
        """
        if isinstance(column_names, str):
            column_names = [column_names]
        if not _is_collection(buckets):
            raise DataQualityConfigurationError(
                ErrorCode.DQCE61, buckets, type(buckets)
            )
        c = cls.__new__(cls)
        c._init_categories("not_in_buckets", column_names, buckets)
        return c


class Threshold:
    """ "
    Defines a Threshold for the data quality `Fail` operation.

    Possible thresholds are:

        * `count`: the number of records that will activate the threshold.
        * `percent`: the percentage of records that will activate the
        threshold.
        * `one`: one record will activate the threshold. A convenience for
        `count(1)`
    """

    def __init__(self):
        self._absolute = None
        self._relative = None
        raise DataQualityConfigurationError(ErrorCode.DQCE67, self.__class__.__name__)

    def __init__count(self, count: int):
        self.absolute = count
        self._relative = None

    def __init__percent(self, percent: int):
        self._absolute = None
        self.relative = percent

    @property
    def absolute(self) -> int | None:
        return self._absolute

    @absolute.setter
    def absolute(self, value: int | None):
        if value is None:
            self._absolute = value
        elif isinstance(value, int):
            if value <= 0:
                raise DataQualityConfigurationError(ErrorCode.DQCE69, value)
            self._absolute = value
        else:
            raise DataQualityConfigurationError(ErrorCode.DQCE68, value, type(value))

    @property
    def relative(self) -> float | None:
        return self._relative

    @relative.setter
    def relative(self, value: int | float | None):
        if value is None:
            self._relative = value
        elif isinstance(value, (float, int)):
            value = float(value)
            if value <= 0 or value >= 100:
                raise DataQualityConfigurationError(ErrorCode.DQCE72, value)
            self._relative = value
        else:
            raise DataQualityConfigurationError(ErrorCode.DQCE73, value, type(value))

    @classmethod
    def count(cls, count: int) -> Self:
        """
        Number of rows that will fail the transaction.
        """
        if count is None:
            raise DataQualityConfigurationError(ErrorCode.DQCE70)
        t = cls.__new__(cls)
        t.__init__count(count)
        return t

    @classmethod
    def percent(cls, percent: float | int) -> Self:
        """
        Percentage of rows that will fail the transaction.
        """
        if percent is None:
            raise DataQualityConfigurationError(ErrorCode.DQCE71)
        t = cls.__new__(cls)
        t.__init__percent(percent)
        return t

    @classmethod
    def one(cls):
        """
        Convenience method to fail the transaction if one rows matches the criteria.
        """
        return cls.count(1)


class Fail(Operation):
    """
    Fails the transaction running the function if a data quality threshold
    is reached for a given criteria.
    """

    def __init__(
        self,
        threshold: Threshold,
        criteria: Criteria = None,
        *,
        tags: TagsSpec = None,
    ):
        """
        Creates a data quality fail operation.

        Args:
            threshold(Threshold): The row threshold to fail the transaction.
            criteria(Criteria, optional): The criteria to fail the transaction.
                Defaults to 'any_failed'.
            tags (str | list[str] | None, optional): Tag(s) for the operation.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(tags=tags)
        self.threshold = threshold
        self.criteria = criteria

    @property
    def criteria(self) -> Criteria:
        """
        :return: The fail criteria.
        """
        if self._criteria is None:
            return Criteria.any_failed()
        return self._criteria

    @criteria.setter
    def criteria(self, value: Criteria):
        match value:
            case Criteria():
                self._criteria = value
            case None:
                self._criteria = None
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE66, value, type(value)
                )

    @property
    def threshold(self) -> Threshold:
        """
        :return: The fail threshold.
        """
        return self._threshold

    @threshold.setter
    def threshold(self, value: Threshold):
        match value:
            case Threshold():
                self._threshold = value
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE74, value, type(value)
                )


# noinspection DuplicatedCode
class Filter(Operation):
    """
    Filters a table based on data quality criteria.

    The table to which the filter is being applied will not have
    the filtered rows anymore.

    The filtered rows can be discarded or redirected to another table.
    """

    def __init__(
        self,
        to_table: str | None = None,
        criteria: Criteria = None,
        *,
        tags: TagsSpec = None,
    ):
        """
        Creates a data quality filter.

        Args:
            to_table: table to send the rows being filtered out to. If
                `None`, the rows will be discarded. Defaults to None
            criteria: The criteria to filter out rows for.
                Defaults to 'any_failed'.
            tags: Tag(s) for the operation.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(tags=tags)
        self.to_table = to_table
        self.criteria = criteria

    @property
    def to_table(self) -> str | None:
        """
        :return: table to redirect the filtered out rows to.
        """
        return self._to_table

    @to_table.setter
    def to_table(self, value: str | None):
        match value:
            case None:
                self._to_table = None
            case str():
                self._to_table = value
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE62, value, type(value)
                )

    @property
    def criteria(self) -> Criteria:
        """
        :return: Criteria to filter out rows for.
        """
        if self._criteria is None:
            return Criteria.any_failed()
        return self._criteria

    @criteria.setter
    def criteria(self, value: Criteria):
        match value:
            case Criteria():
                self._criteria = value
            case None:
                self._criteria = None
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE63, value, type(value)
                )


# noinspection DuplicatedCode
class Select(Operation):
    """
    Selects rows from the table to which data quality is being applied and
    writes them to another table.

    The table to which the select is being applied is not modified.
    """

    def __init__(
        self,
        to_table: str,
        criteria: Criteria = None,
        *,
        tags: TagsSpec = None,
    ):
        """
        Creates a data quality selector.

        Args:
            to_table(str, optional): Table to send the rows being selected.
            criteria(Criteria, optional): The criteria to select the rows with.
                Defaults to 'all_ok'.
            tags (str | list[str] | None, optional): Tag(s) for the operation.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(tags=tags)
        self.to_table = to_table
        self.criteria = criteria

    @property
    def to_table(self) -> str:
        """
        :return: table to write the selected rows to.
        """
        return self._to_table

    @to_table.setter
    def to_table(self, value: str):
        match value:
            case str():
                self._to_table = value
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE64, value, type(value)
                )

    @property
    def criteria(self) -> Criteria:
        """
        :return: Criteria to select rows.
        """
        if self._criteria is None:
            return Criteria.all_ok()
        return self._criteria

    @criteria.setter
    def criteria(self, value: Criteria):
        match value:
            case Criteria():
                self._criteria = value
            case None:
                self._criteria = None
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE65, value, type(value)
                )


class Summary(Operation):
    """
    Generates a data quality report for a table.
    """

    def __init__(self, table: str | None = None, *, tags: TagsSpec = None):
        """
        Creates a data quality summary.

        Args:
            table (str, optional): Name of the data quality summary table. Default name is
                   `<table_name>_dq_summary`
            tags (str | list[str] | None, optional): Tag(s) for the operation.
                Tags can be used by DataQuality operations to select which classifiers
                to operate on. If a DataQuality operation does not define a tag, it
                operates on all defined classifiers. If a DataQuality operation defines
                one or more tags, it will only operate on classifiers that have at least
                one of those tags.
                Defaults to None.
        """
        super().__init__(tags=tags)
        self.table = table

    @property
    def table(self) -> str:
        """
        :return: Name of the data quality summary table.
        """
        return self._table

    @table.setter
    def table(self, value: str):
        match value:
            case None:
                self._table = None
            case str():
                self._table = value
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE46, value, type(value)
                )

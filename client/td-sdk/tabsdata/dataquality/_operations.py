#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Collection, Literal, Self, get_args

from tabsdata.dataquality._utils import _is_collection
from tabsdata.exceptions import DataQualityConfigurationError, ErrorCode

if TYPE_CHECKING:
    from tabsdata.dataquality.typing._typing import TagsSpec


class Operation(ABC):
    """
    Data quality operations to perform on tables.
    """

    def __init__(self, *, tags: TagsSpec = None):
        """
        Initialize the DataQuality object.
        Args:
        tags (TagsSpec, optional): Tag(s) of the `Classifiers` to use.
               If not tag is provided it will only use the `Classifiers` without
               tags. If tags are provided it will use all `Classifiers` that
               have any of those tags. Defaults to None
        """
        self.tags = tags

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
                    raise DataQualityConfigurationError(ErrorCode.DQCE44, v, type(v))
        else:
            raise DataQualityConfigurationError(ErrorCode.DQCE45, value, type(value))
        self._tags = value


CriteriaTypeSpec = Literal["all_ok", "any_failed", "by_bucket"]


class Criteria:
    """ "
    Class for data quality criteria
    """

    VALID_CRITERIA_TYPES = get_args(CriteriaTypeSpec)

    def __init__(
        self, criteria_type: CriteriaTypeSpec, buckets: Collection = None, **kwargs
    ):
        if not kwargs.get("_internal"):
            raise DataQualityConfigurationError(
                ErrorCode.DQCE58, self.__class__.__name__
            )
        self.criteria_type = criteria_type
        self.buckets = buckets

    @property
    def criteria_type(self) -> CriteriaTypeSpec:
        return self._criteria_type

    @criteria_type.setter
    def criteria_type(self, value: CriteriaTypeSpec):
        if value not in self.VALID_CRITERIA_TYPES:
            raise DataQualityConfigurationError(
                ErrorCode.DQCE59, ", ".join(self.VALID_CRITERIA_TYPES), value
            )
        self._criteria_type = value

    @property
    def buckets(self) -> Collection:
        return self._buckets

    @buckets.setter
    def buckets(self, value: Collection):
        if not _is_collection(value) and value is not None:
            raise DataQualityConfigurationError(ErrorCode.DQCE60, value, type(value))
        self._buckets = value

    @classmethod
    def all_ok(cls) -> Self:
        return cls(criteria_type="all_ok", _internal=True)

    @classmethod
    def any_failed(cls) -> Self:
        return cls(criteria_type="any_failed", _internal=True)

    @classmethod
    def in_buckets(cls, buckets: Collection) -> Self:
        if not _is_collection(buckets):
            raise DataQualityConfigurationError(
                ErrorCode.DQCE61, buckets, type(buckets)
            )
        return cls(criteria_type="by_bucket", buckets=buckets, _internal=True)


class Threshold:
    """ "
    Class for data quality thresholds
    """

    def __init__(
        self, count: int | None = None, percent: float | None = None, **kwargs
    ):
        if not kwargs.get("_internal"):
            raise DataQualityConfigurationError(
                ErrorCode.DQCE67, self.__class__.__name__
            )
        self.absolute = count
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
        return cls(count=count, percent=None, _internal=True)

    @classmethod
    def percent(cls, percent: int | float) -> Self:
        """
        Percentage of rows that will fail the transaction.
        """
        if percent is None:
            raise DataQualityConfigurationError(ErrorCode.DQCE71)
        return cls(count=None, percent=percent, _internal=True)

    @classmethod
    def one(cls):
        """
        Convenience method to fail the transaction if one rows matches the criteria.
        """
        return cls(count=1, percent=None, _internal=True)


class Fail(Operation):
    """
    Fails the transaction running the function if a threshold is passed
    for a given criteria.
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
            tags(TagsSpec, optional): Tag(s) of the `Classifiers` to use.
               If not tag is provided it will only use the `Classifiers` without
               tags. If tags are provided it will use all `Classifiers` that
               have any of those tags. Defaults to None
        """
        super().__init__(tags=tags)
        self.threshold = threshold
        self.criteria = criteria

    @property
    def criteria(self) -> Criteria:
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


class Filter(Operation):
    """
    Creates a data quality filter within a `DataQuality` action Data Quality
    operation that filters table rows using the given criteria.

    The table to which the filter is being applied will not have
    the filtered rows anymore.
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
            to_table(str, optional): table to send the rows being filtered out to. If
                `None`, the rows will be discarded. Defaults to None
            criteria(Criteria, optional): The criteria to filter out rows for.
                Defaults to 'any_failed'.
            tags(TagsSpec, optional): Tag(s) of the `Classifiers` to use.
               If not tag is provided it will only use the `Classifiers` without
               tags. If tags are provided it will use all `Classifiers` that
               have any of those tags. Defaults to None
        """
        super().__init__(tags=tags)
        self.to_table = to_table
        self.criteria = criteria

    @property
    def to_table(self) -> str | None:
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


class Select(Operation):
    """
    Creates a data quality selector within a `DataQuality` action
    Data Quality operation that selects table rows using the given
    criteria.

    The table to which the select is being applied will not be changed,
    the selected rows will still be there.
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
            tags(TagsSpec, optional): Tag(s) of the `Classifiers` to use.
               If not tag is provided it will only use the `Classifiers` without
               tags. If tags are provided it will use all `Classifiers` that
               have any of those tags. Defaults to None
        """
        super().__init__(tags=tags)
        self.to_table = to_table
        self.criteria = criteria

    @property
    def to_table(self) -> str:
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
    Data Quality operation that generates a data quality summary for a table.
    """

    def __init__(self, table: str | None = None, *, tags: TagsSpec = None):
        """
        Creates a data quality summary within a `DataQuality` action using the
        defined `Classifiers`.

        Args:
        table (str, optional): Name of the data quality summary table. Default name is
               `dq.summary_<table_name>`
        tags (TagsSpec, optional): Tag(s) of the `Classifiers` to summarize.
               If not tag is provided it will use only the `Classifiers` without
               tags. If tags are provided it will use all `Classifiers` that
               have any of the tags defined here.
        """
        super().__init__(tags=tags)
        self.table = table

    @property
    def table(self) -> str:
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

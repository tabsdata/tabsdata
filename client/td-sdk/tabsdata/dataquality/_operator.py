#
# Copyright 2025 Tabs Data Inc.
#

"""
This module defines the data quality operators that can be applied to a table.
These operators, such as `Fail`, `Filter`, `Select`, and `Summary`, allow you to
define actions based on the results of data quality classifiers.
"""

from abc import ABC
from typing import AbstractSet

# noinspection PyProtectedMember
from tabsdata._specs import TableName
from tabsdata.dataquality._typing import (
    BinsCriteriaSpec,
    BinSpec,
    ColumnNameSpec,
    IncludeQualityColumnsSpec,
    PercentSpec,
    RowCountSpec,
    TagSpec,
    TagsSpec,
)
from tabsdata.dataquality._utils import _normalize_to_list, _td_validate_call
from tabsdata.exceptions import ErrorCode


class Operator(ABC):
    """Abstract base class for all data quality operators.

    This class provides a common structure for operators, including support for tags,
    which allow for selective application of classifiers.
    """

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return vars(self) == vars(other)


class Criteria(ABC):
    """Defines the conditions for selecting records based on classifier results.

    A `Criteria` object specifies which rows of a table should be considered for a
    data quality operation. This can be based on the outcomes of boolean classifiers
    or the binning results of categorical classifiers.
    """

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return vars(self) == vars(other)


class BoolCriteria(Criteria, ABC):
    """A boolean criteria to select records based on classifier results."""

    @_td_validate_call(ErrorCode.DQC3)
    def __init__(
        self, none_is_ok: bool, nan_is_ok: bool, *, tags: TagsSpec | None = None
    ):
        """Initializes the `BoolCriteria` operator.

        Args:
            none_is_ok: If `None` values should be considered OK.
            nan_is_ok: If `NaN` values should be considered OK.
            tags: Optional tags to link this operator to specific classifiers.
        """
        self.tags = tags
        self.none_is_ok = none_is_ok
        self.nan_is_ok = nan_is_ok

    @property
    def tags(self) -> list[TagSpec] | None:
        """Returns the list of tags associated with this criteria."""
        return self._tags

    @tags.setter
    @_td_validate_call(ErrorCode.DQC4)
    def tags(self, tags: TagsSpec | None = None):
        """Sets the tags for the criteria.

        Args:
            tags: A single tag or a list of tags.
        """
        self._tags = _normalize_to_list(tags, keep_none=True)

    @property
    def none_is_ok(self) -> bool:
        """Returns if `None` values should be considered OK."""
        return self._none_is_ok

    @none_is_ok.setter
    @_td_validate_call(ErrorCode.DQC5)
    def none_is_ok(self, none_is_ok: bool):
        """Sets if `None` values should be considered OK.

        Args:
            none_is_ok: A boolean value.
        """
        self._none_is_ok = none_is_ok

    @property
    def nan_is_ok(self) -> bool:
        """Returns if `NaN` values should be considered OK."""
        return self._nan_is_ok

    @nan_is_ok.setter
    @_td_validate_call(ErrorCode.DQC6)
    def nan_is_ok(self, nan_is_ok: bool):
        """Sets if `NaN` values should be considered OK.

        Args:
            nan_is_ok: A boolean value.
        """
        self._nan_is_ok = nan_is_ok


class CategoryCriteria(Criteria, ABC):
    """A category criteria to select records based on classifier results."""

    @_td_validate_call(ErrorCode.DQC7)
    def __init__(
        self,
        column_name: ColumnNameSpec,
        bins: BinsCriteriaSpec,
    ):
        """Initializes the `CategoryCriteria` operator.

        Args:
            column_name: The column name to check for values.
            bins: A single bin or a list of bins.
        """
        self._column_name = column_name
        self.bins = bins

    @property
    def column_name(self) -> ColumnNameSpec:
        """Returns the column name to check for values."""
        return self._column_name

    @column_name.setter
    @_td_validate_call(ErrorCode.DQC8)
    def column_name(self, column_name: ColumnNameSpec):
        """Sets the column name to check for values.

        Args:
            column_name: The column name.
        """
        self._column_name = column_name

    @property
    def bins(self) -> AbstractSet[BinSpec]:
        """Returns a set of bins."""
        return self._bins

    @bins.setter
    @_td_validate_call(ErrorCode.DQC9)
    def bins(self, bins: BinsCriteriaSpec):
        """Sets the bins for the criteria.

        Args:
            bins: A single bin or a list of bins.
        """
        self._bins = set(_normalize_to_list(bins))


class AllOk(BoolCriteria):
    """
    A boolean criteria that selects records where all boolean classifiers
    pass (`True`).
    """

    @_td_validate_call(ErrorCode.DQC10)
    def __init__(
        self,
        none_is_ok: bool = False,
        nan_is_ok: bool = False,
        *,
        tags: TagsSpec | None = None,
    ):
        """Creates a criteria that selects records where all boolean classifiers
        pass (`True`).

        Args:
            none_is_ok: If `None` values should be considered OK.
            nan_is_ok: If `NaN` values should be considered OK.
            tags: Optional tags to link this operator to specific classifiers.
        """
        super().__init__(none_is_ok, nan_is_ok, tags=tags)


class AnyFailed(BoolCriteria):
    """
    A boolean criteria that selects records where at least one boolean
    classifier fails (`False`).
    """

    @_td_validate_call(ErrorCode.DQC11)
    def __init__(
        self,
        none_is_ok: bool = False,
        nan_is_ok: bool = False,
        *,
        tags: TagsSpec | None = None,
    ):
        """Creates a criteria that selects records where at least one boolean
        classifier fails (`False`).

        Args:
            none_is_ok: If `None` values should be considered OK.
            nan_is_ok: If `NaN` values should be considered OK.
            tags: Optional tags to link this operator to specific classifiers.
        """
        super().__init__(none_is_ok, nan_is_ok, tags=tags)


class InBins(CategoryCriteria):
    """A category criteria that selects records where the value in the specified
    columns falls into one of the given bins."""

    @_td_validate_call(ErrorCode.DQC12)
    def __init__(
        self,
        column_name: ColumnNameSpec,
        bins: BinsCriteriaSpec,
    ):
        """Creates a criteria for categorical classifiers. It selects records
        where the value in the specified columns falls into one of the given
        bins.

        Args:
            column_name: The column name to check for values.
            bins: A single bin or a list of bins.
        """
        super().__init__(
            column_name,
            bins,
        )


class NotInBins(CategoryCriteria):
    """A category criteria that selects records where the value in the specified
    columns does not fall into any of the given bins."""

    @_td_validate_call(ErrorCode.DQC13)
    def __init__(
        self,
        column_name: ColumnNameSpec,
        bins: BinsCriteriaSpec,
    ):
        """Creates a criteria for categorical classifiers. It selects records
        where the value in the specified columns does not fall into any of the
        given bins.

        Args:
            column_name: The column name to check for values.
            bins: A single bin or a list of bins.
        """
        super().__init__(
            column_name,
            bins,
        )


class Threshold(ABC):
    """Abstract base class for thresholds used in the `Fail` operator."""

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return vars(self) == vars(other)


class RowCountThreshold(Threshold):
    """A threshold based on a specific number of rows."""

    @_td_validate_call(ErrorCode.DQC14)
    def __init__(self, row_count: RowCountSpec):
        """Initializes the threshold with a maximum row count.

        Args:
            row_count: The number of rows that, if met or exceeded, will trigger the
                       `Fail` operation.
        """
        self.row_count = row_count

    @property
    def row_count(self) -> int:
        """Returns the row count for the threshold."""
        return self._row_count

    @row_count.setter
    @_td_validate_call(ErrorCode.DQC15)
    def row_count(self, row_count: RowCountSpec):
        """Sets the row count for the threshold.

        Args:
            row_count: The number of rows.
        """
        self._row_count = row_count


class PercentThreshold(Threshold):
    """A threshold based on a percentage of total rows."""

    @_td_validate_call(ErrorCode.DQC16)
    def __init__(self, percent: PercentSpec):
        """Initializes the threshold with a percentage.

        Args:
            percent: The percentage of rows that, if met or exceeded, will trigger
                     the `Fail` operation.
        """
        self.percent = percent

    @property
    def percent(self) -> float:
        """Returns the percentage for the threshold."""
        return self._percent

    @percent.setter
    @_td_validate_call(ErrorCode.DQC17)
    def percent(self, percent: PercentSpec):
        """Sets the percentage for the threshold.

        Args:
            percent: The percentage value.
        """
        self._percent = percent


class TagOperator(Operator, ABC):
    """An operator that adds the data quality classifier columns to the original
    table frame. It does not remove any row from the original table.
    """

    @_td_validate_call(ErrorCode.DQC1)
    def __init__(
        self,
        tags: TagsSpec | None,
    ):
        """Initializes the `Enrich` operator.

        Args:
            tags: tags to link this operator to specific classifiers.
        """
        self.tags = tags

    @property
    def tags(self) -> list[TagSpec] | None:
        """Returns the list of tags associated with this operator."""
        return self._tags

    @tags.setter
    @_td_validate_call(ErrorCode.DQC2)
    def tags(self, tags: TagsSpec | None = None):
        """Sets the tags for the operator.

        Args:
            tags: A single tag or a list of tags.
        """
        self._tags = _normalize_to_list(tags, keep_none=True)


class Enrich(TagOperator):
    """An operator that adds the data quality classifier columns to the original
    table frame. It does not remove any row from the original table.
    """

    @_td_validate_call(ErrorCode.DQC18)
    def __init__(
        self,
        to_table: TableName | None = None,
        *,
        tags: TagsSpec | None = None,
    ):
        """Initializes the `Enrich` operator.

        Args:
            to_table: The name of the table with the added data quality
                      classifier columns.
                      If `None`, the original table is used.
            tags: Optional tags to link this operator to specific classifiers.
        """
        super().__init__(tags=tags)
        self.to_table = to_table

    @property
    def to_table(self) -> TableName | None:
        """Returns the destination table with the enriched data quality classifier
        columns. If `None`, the original table is used.
        """
        return self._to_table

    @to_table.setter
    @_td_validate_call(ErrorCode.DQC19)
    def to_table(self, to_table: TableName | None = None):
        """Sets the destination table.

        Args:
            to_table: The name of the table.
        """
        self._to_table = to_table


class CriteriaOperator(Operator, ABC):
    """An operator that uses a criteria to select records."""

    @_td_validate_call(ErrorCode.DQC20)
    def __init__(
        self,
        criteria: Criteria,
    ):
        """Initializes the `CriteriaOperator`.

        Args:
            criteria: The criteria for selecting rows.
        """
        self.criteria = criteria

    @property
    def criteria(self) -> Criteria:
        """Returns the criteria for this `Fail` operation."""
        return self._criteria

    @criteria.setter
    @_td_validate_call(ErrorCode.DQC22)
    def criteria(self, criteria: Criteria):
        """Sets the criteria for the operator.

        Args:
            criteria: The criteria object.
        """
        self._criteria = criteria


class Fail(CriteriaOperator):
    """
    An operator that fails the current transaction if a data quality threshold
    is met.
    """

    @_td_validate_call(ErrorCode.DQC23)
    def __init__(
        self,
        criteria: Criteria,
        threshold: Threshold,
    ):
        """Initializes the `Fail` operator.

        Args:
            criteria: The criteria for selecting rows to count against the threshold.
                      Defaults to `Criteria.any_failed()`.
            threshold: The threshold that triggers the failure.
        """
        super().__init__(criteria)
        self.threshold = threshold

    @property
    def threshold(self) -> Threshold:
        """Returns the threshold for this `Fail` operation."""
        return self._threshold

    @threshold.setter
    @_td_validate_call(ErrorCode.DQC24)
    def threshold(self, threshold: Threshold):
        """Sets the threshold for the `Fail` operation.

        Args:
            threshold: The threshold object.
        """
        self._threshold = threshold


class Filter(CriteriaOperator):
    """An operator that filters rows from a table based on data quality criteria.

    The filtered rows are removed from the table being processed. They can
    either be discarded or redirected to another table.
    """

    @_td_validate_call(ErrorCode.DQC25)
    def __init__(
        self,
        criteria: Criteria,
        to_table: TableName | None = None,
        include_quality_columns: IncludeQualityColumnsSpec = "none",
    ):
        """Initializes the `Filter` operator.

        Args:
            criteria: The criteria for selecting rows to filter.
                      Defaults to `Criteria.any_failed()`.
            to_table: The name of the table to which filtered rows will be sent.
                      If `None`, the filtered rows are discarded.
            include_quality_columns: The data quality columns to include in
                                     the table with filtered out data.
        """
        super().__init__(criteria)
        self.to_table = to_table
        self.include_quality_columns = include_quality_columns

    @property
    def to_table(self) -> TableName | None:
        """Returns the destination table for filtered rows, or `None` if they are
        discarded.
        """
        return self._to_table

    @to_table.setter
    @_td_validate_call(ErrorCode.DQC26)
    def to_table(self, to_table: TableName | None = None):
        """Sets the destination table for filtered rows.

        Args:
            to_table: The name of the table.
        """
        self._to_table = to_table

    @property
    def include_quality_columns(self) -> IncludeQualityColumnsSpec:
        """
        Returns the data quality columns to included in the table with
        filtered out data.
        """
        return self._include_quality_columns

    @include_quality_columns.setter
    @_td_validate_call(ErrorCode.DQC27)
    def include_quality_columns(
        self,
        include_quality_columns: IncludeQualityColumnsSpec = "none",
    ):
        """Sets the data quality columns to include in the filtered data.

        Args:
            include_quality_columns: The columns to include.
        """
        self._include_quality_columns = include_quality_columns


class Select(CriteriaOperator):
    """
    An operator that selects rows based on data quality criteria and writes
    them to another table.

    This operation does not modify the original table.
    """

    @_td_validate_call(ErrorCode.DQC28)
    def __init__(
        self,
        criteria: Criteria,
        to_table: TableName,
        include_quality_columns: IncludeQualityColumnsSpec = "none",
    ):
        """Initializes the `Select` operator.

        Args:
            criteria: The criteria for selecting rows. Defaults to
                      `Criteria.all_ok()`.
            to_table: The name of the table where selected rows will be
                      written.
            include_quality_columns: The data quality columns to include in
                                     the table with selected data.
        """
        super().__init__(criteria)
        self.to_table = to_table
        self.include_quality_columns = include_quality_columns

    @property
    def to_table(self) -> TableName:
        """Returns the destination table for the selected rows."""
        return self._to_table

    @to_table.setter
    @_td_validate_call(ErrorCode.DQC29)
    def to_table(self, to_table: TableName | None = None):
        """Sets the destination table for the selected rows.

        Args:
            to_table: The name of the table.
        """
        self._to_table = to_table

    @property
    def include_quality_columns(self) -> IncludeQualityColumnsSpec:
        """
        Returns the data quality columns to included in the table with
        selected data.
        """
        return self._include_quality_columns

    @include_quality_columns.setter
    @_td_validate_call(ErrorCode.DQC30)
    def include_quality_columns(
        self,
        include_quality_columns: IncludeQualityColumnsSpec = "none",
    ):
        """Sets the data quality columns to include in the selected data.

        Args:
            include_quality_columns: The columns to include.
        """
        self._include_quality_columns = include_quality_columns


class Summary(TagOperator):
    """An operator that generates a data quality summary report for a table."""

    @_td_validate_call(ErrorCode.DQC31)
    def __init__(self, table: TableName | None = None, *, tags: TagsSpec | None = None):
        """Initializes the `Summary` operator.

        Args:
            table: The name of the table where the summary report will be stored.
                   If `None`, a default name like `<source_table_name>_dq_summary`
                   is used.
            tags: Optional tags to link this operator to specific classifiers.
        """
        super().__init__(tags=tags)
        self.table = table

    @property
    def table(self) -> TableName | None:
        """Returns the name of the data quality summary table."""
        return self._table

    @table.setter
    @_td_validate_call(ErrorCode.DQC29)
    def table(self, table: TableName | None = None):
        """Sets the name of the data quality summary table.

        Args:
            table: The name of the table.
        """
        self._table = table

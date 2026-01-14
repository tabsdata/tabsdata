#
# Copyright 2025 Tabs Data Inc.
#
"""
This module defines the data quality operators that can be applied to a table.
These operators, such as `Fail`, `Filter`, `Select`, and `Summary`, allow you to
define actions based on the results of data quality classifiers.
"""

from abc import ABC
from typing import Optional, Self

from tabsdata.dataquality._typing import (
    MAX_BINS,
    BinsCriteriaSpec,
    ColumnNameSpec,
    ColumnNamesSpec,
    CriteriaTypeSpec,
    PercentSpec,
    RowCountSpec,
    TagSpec,
    TagsSpec,
)
from tabsdata.dataquality._utils import _normalize_to_list, _td_validate_call
from tabsdata.exceptions import ConstructorError, ErrorCode, ParameterValidationError

# noinspection PyProtectedMember
from tabsdata.typing._specs import TableName


class Operator(ABC):
    """
    Abstract base class for all data quality operators.

    This class provides a common structure for operators, including support for tags,
    which allow for selective application of classifiers.
    """

    @_td_validate_call(ErrorCode.PVDQOP1)
    def __init__(self, *, tags: Optional[TagsSpec] = None):
        """
        Initializes the operator with optional tags.

        Args:
            tags: A single tag or a list of tags. Tags are used to link operators
                  to specific classifiers. If an operator has tags, it will only
                  consider classifiers that share at least one of those tags.
                  If no tags are provided, the operator applies to all classifiers.
        """
        self._tags = _normalize_to_list(tags, keep_none=True)

    @property
    def tags(self) -> Optional[list[TagSpec]]:
        """
        Returns the list of tags associated with this operator.
        """
        return self._tags


class Criteria:
    """
    Defines the conditions for selecting records based on classifier results.

    A `Criteria` object specifies which rows of a table should be considered for a
    data quality operation. This can be based on the outcomes of boolean classifiers
    or the binning results of categorical classifiers.

    The constructor is private. Use one of the class methods to create an instance:
    - `Criteria.all_ok()`
    - `Criteria.any_failed()`
    - `Criteria.in_bins()`
    - `Criteria.not_in_bins()`
    """

    def __init__(self):
        """
        The constructor is private. Use one of the factory class methods to create
        a `Criteria` instance.
        """
        self._criteria_type = None
        self._column_names = None
        self._bins = None
        raise ConstructorError(
            ErrorCode.PVDQOP1,
            f"{self.__class__.__name__} constructor cannot be invoked."
            "use `all_ok()`, `any_fail()`, `in_bins()` or "
            "`not_in_bins()`",
        )

    @classmethod
    def _init(
        cls,
        criteria_type: CriteriaTypeSpec,
        column_names: Optional[ColumnNamesSpec],
        bins: Optional[BinsCriteriaSpec],
    ):
        if bins and len(bins) > MAX_BINS:
            raise ParameterValidationError(
                ErrorCode.PVDQOP2,
                f"{cls.__name__} cannot create a Criteria with "
                f"'{len(bins)}' bins, maximum is '{MAX_BINS}' "
                "bins.",
            )
        criteria = cls.__new__(cls)
        criteria._criteria_type = criteria_type
        criteria._column_names = _normalize_to_list(column_names, True)
        criteria._bins = bins
        return criteria

    @property
    def criteria_type(self) -> CriteriaTypeSpec:
        """
        Returns the type of criteria (e.g., 'all_ok', 'in_bins').
        """
        return self._criteria_type

    @property
    def column_names(self) -> Optional[list[ColumnNameSpec]]:
        """
        Returns the column names for bin-based criteria. `None` for other types.
        """
        return self._column_names

    @property
    def bins(self) -> Optional[BinsCriteriaSpec]:
        """
        Returns the bins for bin-based criteria. `None` for other types.
        """
        return self._bins

    @classmethod
    def all_ok(cls) -> Self:
        """
        Creates a criteria that selects records where all boolean classifiers pass
        (`True`).
        """
        return cls._init("all_ok", None, None)

    @classmethod
    def any_failed(cls) -> Self:
        """
        Creates a criteria that selects records where at least one boolean classifier
        fails (`False`).
        """
        return cls._init("any_failed", None, None)

    @classmethod
    @_td_validate_call(ErrorCode.PVDQOP3)
    def in_bins(cls, column_names: ColumnNamesSpec, bins: BinsCriteriaSpec) -> Self:
        """
        Creates a criteria for categorical classifiers. It selects records where the
        value in the specified columns falls into one of the given bins.

        Args:
            column_names: The column(s) to which the binning criteria applies.
            bins: A list of bin indices or names to include.
        """
        return cls._init("in_bins", column_names, bins)

    @classmethod
    @_td_validate_call(ErrorCode.PVDQOP4)
    def not_in_bins(cls, column_names: ColumnNamesSpec, bins: BinsCriteriaSpec) -> Self:
        """
        Creates a criteria for categorical classifiers. It selects records where the
        value in the specified columns does not fall into any of the given bins.

        Args:
            column_names: The column(s) to which the binning criteria applies.
            bins: A list of bin indices or names to exclude.
        """
        return cls._init("not_in_bins", column_names, bins)


class Threshold(ABC):
    """
    Abstract base class for thresholds used in the `Fail` operator.
    """


class RowCountThreshold(Threshold):
    """
    A threshold based on a specific number of rows.
    """

    @_td_validate_call(ErrorCode.PVDQOP6)
    def __init__(self, row_count: RowCountSpec):
        """
        Initializes the threshold with a maximum row count.

        Args:
            row_count: The number of rows that, if met or exceeded, will trigger the
                       `Fail` operation.
        """
        self._row_count = row_count

    @property
    def row_count(self) -> int:
        """
        Returns the row count for the threshold.
        """
        return self._row_count


class PercentThreshold(Threshold):
    """
    A threshold based on a percentage of total rows.
    """

    @_td_validate_call(ErrorCode.PVDQOP7)
    def __init__(self, percent: PercentSpec):
        """
        Initializes the threshold with a percentage.

        Args:
            percent: The percentage of rows that, if met or exceeded, will trigger
                     the `Fail` operation.
        """
        self._percent = percent

    @property
    def percent(self) -> float:
        """
        Returns the percentage for the threshold.
        """
        return self._percent


class Enrich(Operator):
    """
    An operator that adds the data quality classifier columns to the original
    table frame.
    It does not remove any row from the original table.
    """

    @_td_validate_call(ErrorCode.PVDQOP9)
    def __init__(
        self,
        to_table: Optional[TableName] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `Enrich` operator.

        Args:
            to_table: The name of the table with the added data quality
                      classifer columns.
                      If `None`, the original table is used.
            tags: Optional tags to link this operator to specific classifiers.
        """
        super().__init__(tags=tags)
        self._to_table = to_table

    @property
    def to_table(self) -> Optional[TableName]:
        """
        Returns the destination table with the enriched data quality classifer
        columns.
        If `None`, the original table is used.
        """
        return self._to_table


class Fail(Operator):
    """
    An operator that fails the current transaction if a data quality threshold is met.
    """

    @_td_validate_call(ErrorCode.PVDQOP8)
    def __init__(
        self,
        threshold: Threshold,
        criteria: Optional[Criteria] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `Fail` operator.

        Args:
            threshold: The threshold that triggers the failure.
            criteria: The criteria for selecting rows to count against the threshold.
                      Defaults to `Criteria.any_failed()`.
            tags: Optional tags to link this operator to specific classifiers.
        """
        super().__init__(tags=tags)
        self._threshold = threshold
        if not criteria:
            criteria = Criteria.any_failed()
        self._criteria = criteria

    @property
    def criteria(self) -> Criteria:
        """
        Returns the criteria for this `Fail` operation.
        """
        return self._criteria

    @property
    def threshold(self) -> Threshold:
        """
        Returns the threshold for this `Fail` operation.
        """
        return self._threshold


class Filter(Operator):
    """
    An operator that filters rows from a table based on data quality criteria.

    The filtered rows are removed from the table being processed. They can
    either be discarded or redirected to another table.
    """

    @_td_validate_call(ErrorCode.PVDQOP9)
    def __init__(
        self,
        to_table: Optional[TableName] = None,
        criteria: Optional[Criteria] = None,
        enrich: bool = False,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `Filter` operator.

        Args:
            to_table: The name of the table to which filtered rows will be sent.
                      If `None`, the filtered rows are discarded.
            criteria: The criteria for selecting rows to filter.
                      Defaults to `Criteria.any_failed()`.
            enrich: If the table with filtered data should include the data
                    quality classifier columns used by the criteria.
            tags: Optional tags to link this operator to specific classifiers.
        """
        super().__init__(tags=tags)
        self._to_table = to_table
        if not criteria:
            criteria = Criteria.any_failed()
        self._criteria = criteria
        self._enrich = enrich

    @property
    def to_table(self) -> Optional[TableName]:
        """
        Returns the destination table for filtered rows, or `None` if they are
        discarded.
        """
        return self._to_table

    @property
    def criteria(self) -> Criteria:
        """
        Returns the criteria for this `Filter` operation.
        """
        return self._criteria

    @property
    def enrich(self) -> bool:
        """
        Returns if the table with filtered data should include the data quality
        classifier columns used by the criteria.
        """
        return self._enrich


class Select(Operator):
    """
    An operator that selects rows based on data quality criteria and writes them to
    another table.

    This operation does not modify the original table.
    """

    @_td_validate_call(ErrorCode.PVDQOP10)
    def __init__(
        self,
        to_table: TableName,
        criteria: Optional[Criteria] = None,
        enrich: bool = False,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Initializes the `Select` operator.

        Args:
            to_table: The name of the table where selected rows will be written.
            criteria: The criteria for selecting rows. Defaults to `Criteria.all_ok()`.
            enrich: If the table with filtered data should include the data
                    quality classifier columns used by the criteria.
            tags: Optional tags to link this operator to specific classifiers.
        """
        super().__init__(tags=tags)
        self._to_table = to_table
        if not criteria:
            criteria = Criteria.all_ok()
        self._criteria = criteria
        self._enrich = enrich

    @property
    def to_table(self) -> TableName:
        """
        Returns the destination table for the selected rows.
        """
        return self._to_table

    @property
    def criteria(self) -> Criteria:
        """
        Returns the criteria for this `Select` operation.
        """
        return self._criteria

    @property
    def enrich(self) -> bool:
        """
        Returns if the table with filtered data should include the data quality
        classifier columns used by the criteria.
        """
        return self._enrich


class Summary(Operator):
    """
    An operator that generates a data quality summary report for a table.
    """

    @_td_validate_call(ErrorCode.PVDQOP11)
    def __init__(
        self, table: Optional[TableName] = None, *, tags: Optional[TagsSpec] = None
    ):
        """
        Initializes the `Summary` operator.

        Args:
            table: The name of the table where the summary report will be stored.
                   If `None`, a default name like `<source_table_name>_dq_summary`
                   is used.
            tags: Optional tags to link this operator to specific classifiers.
        """
        super().__init__(tags=tags)
        self._table = table

    @property
    def table(self) -> Optional[TableName]:
        """
        Returns the name of the data quality summary table.
        """
        return self._table

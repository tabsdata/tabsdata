#
# Copyright 2025 Tabs Data Inc.
#

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
from tabsdata.typing._specs import TableName


class Operator(ABC):
    """
    Base abstract class for data quality operations.
    """

    @_td_validate_call(ErrorCode.PVDQOP1)
    def __init__(self, *, tags: Optional[TagsSpec] = None):
        """
        Constructor.

        Args:
            tags: Tag(s) for the operation.

                Tags can be used by DataQuality operations to select which
                classifiers to operate on. If a DataQuality operation does not
                define a tag, it operates on all defined classifiers. If a
                DataQuality operation defines one or more tags, it will only
                operate on classifiers that have at least one of those tags.

                Defaults to None.
        """
        self._tags = _normalize_to_list(tags)

    @property
    def tags(self) -> list[TagSpec]:
        """
        :return: the tags for the data operation.
        """
        return self._tags


class Criteria:
    """
    Data quality operations define a classifiers criteria for the records
    of the table. Only records that match the criteria are used for the
    data quality operation.

    Possible criteria are:

    * `all_ok`: all boolean classifiers are `True`.
    * `any_failed`: any boolean classifier is `False`.
    * `in_bins`: for the given columns, rows that are in the specified
      bins.
    * `not_in_bins`: for the given columns, rows that are not in the
      specified bins.
    """

    def __init__(self):
        self._criteria_type = None
        self._column_names = None
        self._bins = None
        """
        Do not use directly, use one of `all_ok()`, `any_fail()`,
        `in_bins()` or `not_in_bins()` class method to create a
        `Criteria`.
        """
        raise ConstructorError(
            ErrorCode.PVDQOP1,
            f"{self.__class__.__name__} constructor cannot be invoked."
            "use `all_ok()`, `any_fail()`, `in_bins()` or "
            "`not_in_bins()`",
        )

    @staticmethod
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
        :return: the criteria type.
        """
        return self._criteria_type

    @property
    def column_names(self) -> Optional[list[ColumnNameSpec]]:
        """
        :return: The column names the criteria is applied to. Only used with
        `in_bin` and `not_in_bin` criteria. For other criteria types is
        `None`.
        """
        return self._column_names

    @property
    def bins(self) -> Optional[BinsCriteriaSpec]:
        """
        :return: The category bins used by the criteria. Only used with
        `in_bin` and `not_in_bin` criteria. For other criteria types is
        `None`.
        """
        return self._bins

    @classmethod
    def all_ok(cls) -> Self:
        """
        :return: Creates a criteria that requires all boolean classifiers to
        be `True` for the record to be considered.
        """
        return cls._init(cls, "all_ok", None, None)

    @classmethod
    def any_failed(cls) -> Self:
        """
        :return: Creates a criteria that requires at least one boolean
        classifier to be `False` for the record to be considered.
        """
        return cls._init(cls, "any_failed", None, None)

    @classmethod
    @_td_validate_call(ErrorCode.PVDQOP3)
    def in_bins(cls, column_names: ColumnNamesSpec, bins: BinsCriteriaSpec) -> Self:
        """
        :return: Creates a criteria for the category classifiers of the
        specified columns. For the record to be considered, it requires
        the value in the columns to be in one of the specified bins
        (given either by index or name).
        """
        return cls._init(cls, "in_bins", column_names, bins)

    @classmethod
    @_td_validate_call(ErrorCode.PVDQOP4)
    def not_in_bins(cls, column_names: ColumnNamesSpec, bins: BinsCriteriaSpec) -> Self:
        """
        :return: Creates a criteria for the category classifiers of the
        specified columns. For the record to be considered, it requires
        the value in the columns not to be in any of the specified
        bins (given either by index or name).
        """
        return cls._init(cls, "not_in_bins", column_names, bins)


class Threshold(ABC):
    """ "
    Base threshold class for the data quality `Fail` operation.
    """


class RowCountThreshold(Threshold):

    @_td_validate_call(ErrorCode.PVDQOP6)
    def __init__(self, row_count: RowCountSpec):
        """
        Number of rows that will fail the transaction.
        """
        self._row_count = row_count

    @property
    def row_count(self) -> int:
        return self._row_count


class PercentThreshold(Threshold):

    @_td_validate_call(ErrorCode.PVDQOP7)
    def __init__(self, percent: PercentSpec):
        self._percent = percent

    @property
    def percent(self) -> int:
        return self._percent


class Fail(Operator):
    """
    Fails the transaction running the function if a data quality threshold
    is reached for a given criteria.
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
        Creates a data quality fail operation.

        Args:
            threshold: The threshold to fail the transaction running the
                function.
            criteria: The criteria to fail the transaction running the function
                if the threshold is exceeded.

                Defaults to 'any_failed'.
            tags: Tag(s) for the operation.

                Tags can be used by DataQuality operations to select which
                classifiers to operate on. If a DataQuality operation does not
                define a tag, it operates on all defined classifiers. If a
                DataQuality operation defines one or more tags, it will only
                operate on classifiers that have at least one of those tags.

                Defaults to None.
        """
        super().__init__(tags=tags)
        self._threshold = threshold
        if not criteria:
            criteria = Criteria.any_failed()
        self._criteria = criteria

    @property
    def criteria(self) -> Criteria:
        """
        :return: The fail criteria.
        """
        return self._criteria

    @property
    def threshold(self) -> Threshold:
        """
        :return: The fail threshold.
        """
        return self._threshold


# noinspection DuplicatedCode
class Filter(Operator):
    """
    Filters a table based on data quality criteria.

    The table to which the filter is being applied will not have
    the filtered rows anymore.

    The filtered rows can be discarded or redirected to another table.
    """

    @_td_validate_call(ErrorCode.PVDQOP9)
    def __init__(
        self,
        to_table: Optional[TableName] = None,
        criteria: Optional[Criteria] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Creates a data quality filter.

        Args:
            to_table: table to send the rows being filtered out to. If `None`,
                the rows will be discarded.

                Defaults to None
            criteria: The criteria to filter out rows for.

                Defaults to 'any_failed'.
            tags: Tag(s) for the operation.

                Tags can be used by DataQuality operations to select which
                classifiers to operate on. If a DataQuality operation does not
                define a tag, it operates on all defined classifiers. If a
                DataQuality operation defines one or more tags, it will only
                operate on classifiers that have at least one of those tags.

                Defaults to None.
        """
        super().__init__(tags=tags)
        self._to_table = to_table
        if not criteria:
            criteria = Criteria.any_failed()
        self._criteria = criteria

    @property
    def to_table(self) -> Optional[TableName]:
        """
        :return: table to redirect the filtered out rows to.
        """
        return self._to_table

    @property
    def criteria(self) -> Criteria:
        """
        :return: Criteria to filter out rows for.
        """
        return self._criteria


# noinspection DuplicatedCode
class Select(Operator):
    """
    Selects rows from the table to which data quality is being applied and
    writes them to another table.

    The table to which the select is being applied is not modified.
    """

    @_td_validate_call(ErrorCode.PVDQOP10)
    def __init__(
        self,
        to_table: TableName,
        criteria: Optional[Criteria] = None,
        *,
        tags: Optional[TagsSpec] = None,
    ):
        """
        Creates a data quality selector.

        Args:
            to_table: Table to send the rows being selected.
            criteria: The criteria to select the rows with. Defaults to
                'all_ok'.
            tags: Tag(s) for the operation.

                Tags can be used by DataQuality operations to select which
                classifiers to operate on. If a DataQuality operation does not
                define a tag, it operates on all defined classifiers. If a
                DataQuality operation defines one or more tags, it will only
                operate on classifiers that have at least one of those tags.

                Defaults to None.
        """
        super().__init__(tags=tags)
        self._to_table = to_table
        if not criteria:
            criteria = Criteria.all_ok()
        self._criteria = criteria

    @property
    def to_table(self) -> TableName:
        """
        :return: table to write the selected rows to.
        """
        return self._to_table

    @property
    def criteria(self) -> Criteria:
        """
        :return: Criteria to select rows.
        """
        return self._criteria


class Summary(Operator):
    """
    Generates a data quality report for a table.
    """

    @_td_validate_call(ErrorCode.PVDQOP11)
    def __init__(
        self, table: Optional[TableName] = None, *, tags: Optional[TagsSpec] = None
    ):
        """
        Creates a data quality summary.

        Args:
            table: Name of the data quality summary table.

                Defaults to `<table_name>_dq_summary`.
            tags: Tag(s) for the operation.

                Tags can be used by DataQuality operations to select which
                classifiers to operate on. If a DataQuality operation does not
                define a tag, it operates on all defined classifiers. If a
                DataQuality operation defines one or more tags, it will only
                operate on classifiers that have at least one of those tags.

                Defaults to None.
        """
        super().__init__(tags=tags)
        self._table = table

    @property
    def table(self) -> Optional[TableName]:
        """
        :return: Name of the data quality summary table.
        """
        return self._table

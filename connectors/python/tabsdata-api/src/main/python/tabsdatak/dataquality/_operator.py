#
# Copyright 2026 Tabsdata Inc.
#

"""
This module defines the data quality operators that can be applied to a table.
These operators, such as `Fail`, `Filter`, `Select`, and `Summary`, allow you to
define actions based on the results of data quality classifiers.

Operators, criteria and thresholds are frozen dataclasses that serialize
through the `JsonSerde` envelope; polymorphic fields (`criteria`,
`threshold`) are resolved on load by the envelope `type`/`kind`.
"""

from abc import ABC
from dataclasses import field

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak.api import TableNameSpec
from tabsdatak.dataquality._error import DataQualityErrorCode
from tabsdatak.dataquality._typing import (
    BinsCriteriaSpec,
    ColumnNameSpec,
    IncludeQualityColumnsSpec,
    PercentSpec,
    RowCountSpec,
    TagsSpec,
)


# ---------------------------------------------------------------------------
# Abstract bases
# ---------------------------------------------------------------------------


class Operator(ABC, _api.JsonSerde):
    """Abstract base class for all data quality operators.

    An operator acts on the verdicts produced by a `DataQuality` action's
    classifiers: append them (`Enrich`), summarize them (`Summary`),
    filter or select rows by a `Criteria` (`Filter` / `Select`), or
    abort the transaction (`Fail`). Collect operators in
    `DataQuality(operators=[...])`.
    """

    # devguide-ref: how classifiers/operators compose into a DataQuality check

    __kind__ = "dqOperator"


class Criteria(ABC, _api.JsonSerde):
    """Selects rows for a `Filter`, `Select` or `Fail` operator.

    A criteria turns classifier verdicts into a per-row selection: boolean
    criteria (`AllOk` / `AnyFailed`) read boolean classifiers; category
    criteria (`InBins` / `NotInBins`) read a categorizer's bins.
    """

    __kind__ = "dqCriteria"


class Threshold(ABC, _api.JsonSerde):
    """Abstract base class for the thresholds used by the `Fail` operator.

    A threshold sets how many selected rows trigger the failure: an absolute
    count (`RowCountThreshold`) or a percentage (`PercentThreshold`).
    """

    __kind__ = "dqThreshold"


# ---------------------------------------------------------------------------
# Criteria
# ---------------------------------------------------------------------------


@_api.dataclass(DataQualityErrorCode.DQC_3)
class BoolCriteria(Criteria, ABC):
    """Abstract base for criteria over boolean classifier verdicts.

    Args:
        none_is_ok: Treat a null verdict as passing (default `False`).
        nan_is_ok: Treat a `NaN` verdict as passing (default `False`).
        tags: Optional list of tags; only classifiers carrying a matching
            tag are considered.
    """

    __kind__ = "dqBoolCriteria"

    none_is_ok: bool = False
    nan_is_ok: bool = False
    tags: TagsSpec | None = field(default=None, kw_only=True)


@_api.dataclass(DataQualityErrorCode.DQC_10)
class AllOk(BoolCriteria):
    """A boolean criteria that selects rows where every boolean classifier
    passes (`True`). Use with `Filter`, `Select` or `Fail`.

    :categories: dq-operator
    """

    __kind__ = "dqAllOk"


@_api.dataclass(DataQualityErrorCode.DQC_11)
class AnyFailed(BoolCriteria):
    """A boolean criteria that selects rows where at least one boolean
    classifier fails (`False`). Use with `Filter`, `Select` or
    `Fail`.

    :categories: dq-operator
    """

    __kind__ = "dqAnyFailed"


@_api.dataclass(DataQualityErrorCode.DQC_7)
class CategoryCriteria(Criteria, ABC):
    """Abstract base for criteria over a categorizer's bin assignments.

    Args:
        column_name: Name of the column holding the bin index produced by a
            `ScaleCategorizer`.
        bins: Bins to match; a non-empty set of bin indices (0 to 100) and/
            or the special bins `"none"`, `"nan"`, `"underflow"` and
            `"overflow"`.
    """

    __kind__ = "dqCategoryCriteria"

    column_name: ColumnNameSpec
    bins: BinsCriteriaSpec


@_api.dataclass(DataQualityErrorCode.DQC_12)
class InBins(CategoryCriteria):
    """A category criteria that selects rows whose bin is one of the given
    bins. Use with `Filter`, `Select` or `Fail`.

    :categories: dq-operator

    Examples:
        .. code-block:: python

            InBins(column_name="value_category", bins={0, 1, 2, "underflow"})
    """

    __kind__ = "dqInBins"


@_api.dataclass(DataQualityErrorCode.DQC_13)
class NotInBins(CategoryCriteria):
    """A category criteria that selects rows whose bin is not among the given
    bins. Use with `Filter`, `Select` or `Fail`.

    :categories: dq-operator
    """

    __kind__ = "dqNotInBins"


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------


@_api.dataclass(DataQualityErrorCode.DQC_14)
class RowCountThreshold(Threshold):
    """A `Fail` threshold expressed as an absolute number of rows.

    :categories: dq-operator

    Args:
        row_count: Row count that, when reached, triggers the failure; a
            non-negative whole number.
    """

    __kind__ = "dqRowCountThreshold"

    row_count: RowCountSpec


@_api.dataclass(DataQualityErrorCode.DQC_16)
class PercentThreshold(Threshold):
    """A `Fail` threshold expressed as a percentage of the table's rows.

    :categories: dq-operator

    Args:
        percent: Percentage of rows (0 to 100) that, when reached, triggers
            the failure.
    """

    __kind__ = "dqPercentThreshold"

    percent: PercentSpec


# ---------------------------------------------------------------------------
# Operators
# ---------------------------------------------------------------------------


@_api.dataclass(DataQualityErrorCode.DQC_1)
class TagOperator(Operator, ABC):
    """
    An operator that is scoped to classifiers by tag.
    """

    __kind__ = "dqTagOperator"

    tags: TagsSpec | None = field(default=None, kw_only=True)


@_api.dataclass(DataQualityErrorCode.DQC_18)
class Enrich(TagOperator):
    """An operator that appends each classifier's verdict column to the target
    table. No rows are removed.

    :categories: dq-operator

    Args:
        to_table: Table to write the enriched rows to; a valid table name.
            If `None` (default), the target table is enriched in place.
        tags: Optional list of tags; only classifiers carrying a matching
            tag are applied.
    """

    __kind__ = "dqEnrich"

    to_table: TableNameSpec | None = None


@_api.dataclass(DataQualityErrorCode.DQC_31)
class Summary(TagOperator):
    """An operator that writes a data quality summary (one row per classifier:
    verdict tallies, or bin counts for a categorizer) to a table.

    :categories: dq-operator

    Args:
        table: Table to write the report to; a valid table name. If
            `None` (default), a table named `<target>_dq_summary` is
            used.
        tags: Optional list of tags; only classifiers carrying a matching
            tag are included.
    """

    __kind__ = "dqSummary"

    table: TableNameSpec | None = None


@_api.dataclass(DataQualityErrorCode.DQC_20)
class CriteriaOperator(Operator, ABC):
    """An operator that uses a criteria to select records."""

    __kind__ = "dqCriteriaOperator"

    criteria: Criteria


@_api.dataclass(DataQualityErrorCode.DQC_23)
class Fail(CriteriaOperator):
    """An operator that fails (aborts) the current transaction when the rows
    selected by `criteria` reach `threshold`.

    :categories: dq-operator

    Args:
        criteria: Selects the rows counted against the threshold.
        threshold: The `RowCountThreshold` or `PercentThreshold` that,
            when reached, fails the transaction.
    """

    __kind__ = "dqFail"

    threshold: Threshold


@_api.dataclass(DataQualityErrorCode.DQC_25)
class Filter(CriteriaOperator):
    """An operator that removes the rows selected by `criteria` from the
    target table. Removed rows are discarded or redirected to another table.

    :categories: dq-operator

    Args:
        criteria: Selects the rows to remove.
        to_table: Table to redirect the removed rows to; a valid table
            name. If `None` (default), they are discarded.
        include_quality_columns: Verdict columns to carry into `to_table`:
            `"none"`, `"criteria"` or `"all"` (default `"none"`).
    """

    __kind__ = "dqFilter"

    to_table: TableNameSpec | None = None
    include_quality_columns: IncludeQualityColumnsSpec = "none"


@_api.dataclass(DataQualityErrorCode.DQC_28)
class Select(CriteriaOperator):
    """An operator that copies the rows selected by `criteria` to another
    table, leaving the target table unchanged.

    :categories: dq-operator

    Args:
        criteria: Selects the rows to copy.
        to_table: Table the selected rows are written to; a valid table
            name (required).
        include_quality_columns: Verdict columns to carry into `to_table`:
            `"none"`, `"criteria"` or `"all"` (default `"none"`).
    """

    __kind__ = "dqSelect"

    to_table: TableNameSpec
    include_quality_columns: IncludeQualityColumnsSpec = "none"

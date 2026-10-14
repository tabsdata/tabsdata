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

from pydantic import model_validator

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak.api import TableNameSpec
from tabsdatak.dataquality._error import DataQualityErrorCode
from tabsdatak.dataquality._typing import (
    BinsCriteriaSpec,
    ColumnNameSpec,
    IncludeQualityColumnsSpec,
    PercentSpec,
    RetentionSpec,
    RowCountSpec,
    TagsSpec,
)

# ---------------------------------------------------------------------------
# Abstract bases
# ---------------------------------------------------------------------------


class Operator(ABC, _api.JsonSerde):
    """Abstract base class for all data quality operators.

    :categories: dq-operator

    An operator acts on the verdicts produced by a `DataQuality` action's
    classifiers: append them (`Enrich`), summarize them (`Summary`),
    filter or select rows by a `Criteria` (`Filter` / `Select`), or
    abort the transaction (`Fail`). Collect operators in
    `DataQuality(operators=[...])`.

    Examples:
        Operators are the acting half of a check; pass concrete ones to
        `DataQuality(operators=[...])`:

        .. code-block:: python

            DataQuality(
                table="checked",
                classifiers=[IsNotNull(["customer_id"])],
                operators=[Enrich(), Summary()],
            )
    """

    # devguide-ref: how classifiers/operators compose into a DataQuality check

    __kind__ = "dqOperator"


class Criteria(ABC, _api.JsonSerde):
    """Selects rows for a `Filter`, `Select` or `Fail` operator.

    :categories: dq-operator

    A criteria turns classifier verdicts into a per-row selection: boolean
    criteria (`AllOk` / `AnyFailed`) read boolean classifiers; category
    criteria (`InBins` / `NotInBins`) read a categorizer's bins.

    Examples:
        A criteria turns verdicts into a row selection for `Filter`, `Select`
        or `Fail`:

        .. code-block:: python

            Filter(AnyFailed())
            Select(InBins(column_name="amount_bin", bins={0}), to_table="low")
    """

    __kind__ = "dqCriteria"


class Threshold(ABC, _api.JsonSerde):
    """Abstract base class for the thresholds used by the `Fail` operator.

    :categories: dq-operator

    A threshold sets how many selected rows trigger the failure: an absolute
    count (`RowCountThreshold`) or a percentage (`PercentThreshold`).

    Examples:
        A threshold says how many selected rows make `Fail` abort:

        .. code-block:: python

            Fail(AnyFailed(), RowCountThreshold(1))
            Fail(AnyFailed(), PercentThreshold(5))
    """

    __kind__ = "dqThreshold"


# ---------------------------------------------------------------------------
# Criteria
# ---------------------------------------------------------------------------


@_api.dataclass(DataQualityErrorCode.DQC_3)
class BoolCriteria(Criteria, ABC):
    """Abstract base for criteria over boolean classifier verdicts.

    :categories: dq-operator

    Args:
        none_is_ok: Treat a null verdict as passing (default `False`).
        nan_is_ok: Treat a `NaN` verdict as passing (default `False`).
        tags: Optional list of tags; only classifiers carrying a matching
            tag are considered.

    Examples:
        The concrete boolean criteria are `AllOk` and `AnyFailed`:

        .. code-block:: python

            Filter(AnyFailed())
            Select(AllOk(), to_table="clean")
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

    Args:
        none_is_ok: Treat a null verdict as passing (default `False`).
        nan_is_ok: Treat a `NaN` verdict as passing (default `False`).
        tags: Optional tags; only the classifiers carrying a matching tag are
            considered.

    Examples:
        .. code-block:: python

            Select(AllOk(), to_table="clean")

        By default a null or `NaN` verdict counts as a failure; the
        `none_is_ok` / `nan_is_ok` flags let them pass:

        .. code-block:: python

            AllOk(none_is_ok=True, nan_is_ok=True)
    """

    __kind__ = "dqAllOk"


@_api.dataclass(DataQualityErrorCode.DQC_11)
class AnyFailed(BoolCriteria):
    """A boolean criteria that selects rows where at least one boolean
    classifier fails (`False`). Use with `Filter`, `Select` or
    `Fail`.

    :categories: dq-operator

    Args:
        none_is_ok: Treat a null verdict as passing (default `False`).
        nan_is_ok: Treat a `NaN` verdict as passing (default `False`).
        tags: Optional tags; only the classifiers carrying a matching tag are
            considered.

    Examples:
        .. code-block:: python

            Filter(AnyFailed())

        Scope it to the classifiers carrying a tag:

        .. code-block:: python

            AnyFailed(tags=["required"])
    """

    __kind__ = "dqAnyFailed"


@_api.dataclass(DataQualityErrorCode.DQC_7)
class CategoryCriteria(Criteria, ABC):
    """Abstract base for criteria over a categorizer's bin assignments.

    :categories: dq-operator

    Args:
        column_name: Name of the column holding the bin index produced by a
            `ScaleCategorizer`.
        bins: Bins to match; a non-empty set of bin indices (0 to 100) and/
            or the special bins `"none"`, `"nan"`, `"underflow"` and
            `"overflow"`.

    Examples:
        The concrete category criteria are `InBins` and `NotInBins`:

        .. code-block:: python

            InBins(column_name="amount_bin", bins={0, 1})
            NotInBins(column_name="amount_bin", bins={"overflow"})
    """

    __kind__ = "dqCategoryCriteria"

    column_name: ColumnNameSpec
    bins: BinsCriteriaSpec


@_api.dataclass(DataQualityErrorCode.DQC_12)
class InBins(CategoryCriteria):
    """A category criteria that selects rows whose bin is one of the given
    bins. Use with `Filter`, `Select` or `Fail`.

    :categories: dq-operator

    Args:
        column_name: Name of the column holding the bin index a
            `ScaleCategorizer` produced.
        bins: Bins to match; a non-empty set of bin indices (0 to 100) and/or
            the special bins `"none"`, `"nan"`, `"underflow"` and `"overflow"`.

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

    Args:
        column_name: Name of the column holding the bin index a
            `ScaleCategorizer` produced.
        bins: Bins to match; a non-empty set of bin indices (0 to 100) and/or
            the special bins `"none"`, `"nan"`, `"underflow"` and `"overflow"`.

    Examples:
        .. code-block:: python

            Filter(NotInBins(column_name="amount_bin", bins={0, 1, 2}))

        The special bins catch the values that fell outside the scale:

        .. code-block:: python

            NotInBins(
                column_name="amount_bin",
                bins={"none", "nan", "underflow", "overflow"},
            )
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

    Examples:
        Fail as soon as one row is selected:

        .. code-block:: python

            Fail(AnyFailed(), RowCountThreshold(1))
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

    Examples:
        Fail when a twentieth of the table is selected:

        .. code-block:: python

            Fail(AnyFailed(), PercentThreshold(5))
    """

    __kind__ = "dqPercentThreshold"

    percent: PercentSpec


# ---------------------------------------------------------------------------
# Operators
# ---------------------------------------------------------------------------


@_api.dataclass(DataQualityErrorCode.DQC_1)
class TagOperator(Operator, ABC):
    """An operator that is scoped to classifiers by tag.

    :categories: dq-operator

    Args:
        tags: Optional tags; only the classifiers carrying a matching tag are
            applied.

    Examples:
        A tagged operator applies only to the classifiers carrying that tag:

        .. code-block:: python

            DataQuality(
                table="checked",
                classifiers=[IsNotNull(["customer_id"], tags=["required"])],
                operators=[Summary(tags=["required"])],
            )
    """

    __kind__ = "dqTagOperator"

    tags: TagsSpec | None = field(default=None, kw_only=True)


@_api.dataclass(DataQualityErrorCode.DQC_18)
class Enrich(TagOperator):
    """An operator that appends each classifier's verdict column to the target
    table. No rows are removed.

    :categories: dq-operator

    Args:
        tags: Optional list of tags; only classifiers carrying a matching
            tag are applied.
        to_table: Table to write the enriched rows to; a valid table name.
            If `None` (default), the target table is enriched in place.

    Examples:
        .. code-block:: python

            Enrich()

        Write the enriched rows to another table instead of adding the verdict
        columns in place:

        .. code-block:: python

            Enrich(to_table="checked_enriched")
    """

    __kind__ = "dqEnrich"

    to_table: TableNameSpec | None = None


@_api.dataclass(DataQualityErrorCode.DQC_32)
class Trends(_api.JsonSerde):
    """The rolling trend derived from DQ Summary runs.

    :categories: dq-operator

    Each run rewrites the whole table: the previous version is read back,
    merged with the new summary and re-tiered. Every run counts towards its
    week and month bucket from its first execution, so the three tiers cover
    overlapping spans of the same history. Run rows are dropped after `days`,
    week rows after `weeks` and month rows after `months`, each tier expiring
    independently -- a week that falls out of the `weeks` window survives in
    its month.

    Buckets are UTC: a week truncates to the Monday that starts it, a month to
    the 1st. Beside the three tiers the table carries `folded` rows, one per
    transaction already counted. They hold no tallies; they are what lets a
    re-run of a transaction be recognised and not counted a second time, and
    they are kept for as long as either coarse tier could still be carrying
    that run.

    Args:
        table: Table to write the trend report to; a valid table name. If
            `None` (default), a table named `<target>_dq_summary_trends` is
            used.
        days: Days a row is kept as an exact run row. It is folded into the week
            and month tiers from its first run regardless. Defaults to 7.
        weeks: Weeks a folded week row is kept after its bucket start.
            Defaults to 12.
        months: Months a folded month row is kept after its bucket start.
            Defaults to 24.

    Examples:
        Maintain the trend table with the default retention:

        .. code-block:: python

            Summary(trends=Trends())

        Keep exact rows for a week, weeks for a quarter, and no month tier:

        .. code-block:: python

            Summary(trends=Trends(days=7, weeks=13, months=0))
    """

    __kind__ = "dqTrends"

    table: TableNameSpec | None = None
    days: RetentionSpec = 7
    weeks: RetentionSpec = 12
    months: RetentionSpec = 24

    @model_validator(mode="after")
    def validate_retention_tiers(self) -> "Trends":
        # At least 1 tier has to be non-zero
        if not (self.days or self.weeks or self.months):
            raise DataQualityErrorCode.DQC_33.exception()
        return self


@_api.dataclass(DataQualityErrorCode.DQC_31)
class Summary(TagOperator):
    """An operator that writes a data quality summary (one row per classifier:
    verdict tallies, or bin counts for a categorizer) to a table.

    :categories: dq-operator

    Args:
        tags: Optional list of tags; only classifiers carrying a matching
            tag are included.
        table: Table to write the report to; a valid table name. If
            `None` (default), a table named `<target>_dq_summary` is
            used.
        trends: The rolling trend table derived from this report. If `None`
            (default), no trend table is maintained; pass `Trends()` for one
            with the default retention.

    Examples:
        .. code-block:: python

            Summary()

        Name the report table, and restrict it to the classifiers carrying a
        tag:

        .. code-block:: python

            Summary(table="orders_quality", tags=["required"])

        Also maintain a rolling trend table with the default retention:

        .. code-block:: python

            Summary(trends=Trends())
    """

    __kind__ = "dqSummary"

    table: TableNameSpec | None = None
    trends: Trends | None = None


@_api.dataclass(DataQualityErrorCode.DQC_20)
class CriteriaOperator(Operator, ABC):
    """An operator that uses a criteria to select records.

    :categories: dq-operator

    Args:
        criteria: Selects the rows this operator acts on.

    Examples:
        The operators driven by a criteria are `Filter`, `Select` and `Fail`:

        .. code-block:: python

            Filter(AnyFailed())
            Select(AnyFailed(), to_table="rejected")
            Fail(AnyFailed(), PercentThreshold(1))
    """

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

    Examples:
        Abort the transaction on the first failing row:

        .. code-block:: python

            Fail(AnyFailed(), RowCountThreshold(1))

        Tolerate a little bad data instead, and fail only past a percentage:

        .. code-block:: python

            Fail(AnyFailed(), PercentThreshold(5))
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
            `"none"`, `"criteria"` or `"all"` (default `"none"`). The target
            table never carries them, so any value other than `"none"`
            requires `to_table`.

    Examples:
        Drop every row a classifier failed:

        .. code-block:: python

            Filter(AnyFailed())

        Keep them instead, in a table of their own, together with the verdict
        columns that explain the removal:

        .. code-block:: python

            Filter(
                AnyFailed(),
                to_table="rejected",
                include_quality_columns="criteria",
            )
    """

    __kind__ = "dqFilter"

    to_table: TableNameSpec | None = None
    include_quality_columns: IncludeQualityColumnsSpec = "none"

    @model_validator(mode="after")
    def validate_quality_columns_have_a_table(self) -> "Filter":
        if self.to_table is None and self.include_quality_columns != "none":
            raise DataQualityErrorCode.DQC_26.exception(
                policy=self.include_quality_columns
            )
        return self


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

    Examples:
        Copy the failing rows out for review, leaving the table untouched:

        .. code-block:: python

            Select(AnyFailed(), to_table="to_review")
    """

    __kind__ = "dqSelect"

    to_table: TableNameSpec
    include_quality_columns: IncludeQualityColumnsSpec = "none"

#
# Copyright 2026 Tabsdata Inc.
#

"""
This module defines the data quality operators that can be applied to a table.
These operators, such as `Fail`, `Filter`, `Select`, and `Summary`, allow you to
define actions based on the results of data quality classifiers.

Operators, criteria and thresholds are frozen dataclasses that serialize
through the `JsonSerde` envelope; polymorphic fields (``criteria``,
``threshold``) are resolved on load by the envelope ``type``/``kind``.
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

    This class provides a common structure for operators, including support for tags,
    which allow for selective application of classifiers.
    """

    __kind__ = "dqOperator"


class Criteria(ABC, _api.JsonSerde):
    """Defines the conditions for selecting records based on classifier results.

    A `Criteria` object specifies which rows of a table should be considered for a
    data quality operation. This can be based on the outcomes of boolean classifiers
    or the binning results of categorical classifiers.
    """

    __kind__ = "dqCriteria"


class Threshold(ABC, _api.JsonSerde):
    """Abstract base class for thresholds used in the `Fail` operator."""

    __kind__ = "dqThreshold"


# ---------------------------------------------------------------------------
# Criteria
# ---------------------------------------------------------------------------


@_api.dataclass(DataQualityErrorCode.DQC_3)
class BoolCriteria(Criteria, ABC):
    """A boolean criteria to select records based on classifier results."""

    __kind__ = "dqBoolCriteria"

    none_is_ok: bool = False
    nan_is_ok: bool = False
    tags: TagsSpec | None = field(default=None, kw_only=True)


@_api.dataclass(DataQualityErrorCode.DQC_10)
class AllOk(BoolCriteria):
    """
    :categories: dq-operator

    A boolean criteria that selects records where all boolean classifiers
    pass (`True`).
    """

    __kind__ = "dqAllOk"


@_api.dataclass(DataQualityErrorCode.DQC_11)
class AnyFailed(BoolCriteria):
    """
    :categories: dq-operator

    A boolean criteria that selects records where at least one boolean
    classifier fails (`False`).
    """

    __kind__ = "dqAnyFailed"


@_api.dataclass(DataQualityErrorCode.DQC_7)
class CategoryCriteria(Criteria, ABC):
    """A category criteria to select records based on classifier results."""

    __kind__ = "dqCategoryCriteria"

    column_name: ColumnNameSpec
    bins: BinsCriteriaSpec


@_api.dataclass(DataQualityErrorCode.DQC_12)
class InBins(CategoryCriteria):
    """
    :categories: dq-operator

    A category criteria that selects records where the value in the specified
    columns falls into one of the given bins.
    """

    __kind__ = "dqInBins"


@_api.dataclass(DataQualityErrorCode.DQC_13)
class NotInBins(CategoryCriteria):
    """
    :categories: dq-operator

    A category criteria that selects records where the value in the specified
    columns does not fall into any of the given bins.
    """

    __kind__ = "dqNotInBins"


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------


@_api.dataclass(DataQualityErrorCode.DQC_14)
class RowCountThreshold(Threshold):
    """A threshold based on a specific number of rows."""

    __kind__ = "dqRowCountThreshold"

    row_count: RowCountSpec


@_api.dataclass(DataQualityErrorCode.DQC_16)
class PercentThreshold(Threshold):
    """
    :categories: dq-operator

    A threshold based on a percentage of total rows.
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
    """
    :categories: dq-operator

    An operator that adds the data quality classifier columns to the original
    table frame. It does not remove any row from the original table.
    """

    __kind__ = "dqEnrich"

    to_table: TableNameSpec | None = None


@_api.dataclass(DataQualityErrorCode.DQC_31)
class Summary(TagOperator):
    """
    :categories: dq-operator

    An operator that generates a data quality summary report for a table.
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
    """
    :categories: dq-operator

    An operator that fails the current transaction if a data quality threshold
    is met.
    """

    __kind__ = "dqFail"

    threshold: Threshold


@_api.dataclass(DataQualityErrorCode.DQC_25)
class Filter(CriteriaOperator):
    """
    :categories: dq-operator

    An operator that filters rows from a table based on data quality criteria.

    The filtered rows are removed from the table being processed. They can
    either be discarded or redirected to another table.
    """

    __kind__ = "dqFilter"

    to_table: TableNameSpec | None = None
    include_quality_columns: IncludeQualityColumnsSpec = "none"


@_api.dataclass(DataQualityErrorCode.DQC_28)
class Select(CriteriaOperator):
    """
    :categories: dq-operator

    An operator that selects rows based on data quality criteria and writes
    them to another table.

    This operation does not modify the original table.
    """

    __kind__ = "dqSelect"

    to_table: TableNameSpec
    include_quality_columns: IncludeQualityColumnsSpec = "none"

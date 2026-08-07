#
# Copyright 2026 Tabsdata Inc.
#

"""
This module defines the `DataQuality` action, which orchestrates the application
of data quality rules on a table. It combines classifiers and operators to perform
comprehensive data quality checks and actions.
"""

from typing import Annotated, TypeAlias

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak.api import OnTablesAction, TableNameSpec
from tabsdatak.dataquality._classifier import Classifier
from tabsdatak.dataquality._error import DataQualityErrorCode
from tabsdatak.dataquality._operator import Operator

ClassifiersSpec: TypeAlias = Annotated[list[Classifier], _api.NON_EMPTY_LIST_FIELD]
"""A non-empty list of `Classifier` instances."""

OperatorsSpec: TypeAlias = Annotated[list[Operator], _api.NON_EMPTY_LIST_FIELD]
"""A non-empty list of `Operator` instances."""


@_api.dataclass(DataQualityErrorCode.DQC_201)
class DataQuality(OnTablesAction):
    """A data quality check applied to one of a function's output tables.

    :categories: dq-action

    Bundles classifiers (which label each row) with operators (which act on
    those labels). Pass it to a `@publisher` / `@transformer` /
    `@subscriber` via `on_tables=[DataQuality(...)]`; the runtime runs
    the check against the named output table after the function succeeds.

    Args:
        table: Output table the check runs on; a valid table name
            (alphanumerics and underscores, starting with a letter).
        classifiers: Classifiers that label the table's columns; a
            non-empty list (at least one).
        operators: Operators applied to the labels -- `Enrich`,
            `Summary`, `Filter`, `Select` or `Fail`; a non-empty
            list (at least one).

    Examples:
        .. code-block:: python

            DataQuality(
                table="checked",
                classifiers=[IsNull(["value"])],
                operators=[Enrich(), Summary()],
            )
    """

    # devguide-ref: how classifiers/operators compose into a DataQuality check

    table: TableNameSpec
    classifiers: ClassifiersSpec
    operators: OperatorsSpec

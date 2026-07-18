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

ClassifiersSpec: TypeAlias = Annotated[
    list[Classifier], _api.NON_EMPTY_LIST_CONSTRAINTS
]
"""A non-empty list of `Classifier` instances."""

OperatorsSpec: TypeAlias = Annotated[list[Operator], _api.NON_EMPTY_LIST_CONSTRAINTS]
"""A non-empty list of `Operator` instances."""


@_api.dataclass(DataQualityErrorCode.DQC_201)
class DataQuality(OnTablesAction):
    """
    :categories: dq-action

    Represents a data quality action to be performed on a table.

    This class encapsulates a set of data quality rules, defined by classifiers
    and operators, that are applied to a specified table. It serves as a container
    for a complete data quality workflow.

    :param table: The name of the table to which the data quality rules will be
        applied.
    :param classifiers: The classifiers to evaluate the data in the table.
    :param operators: The operators that define the actions to take based on the
        classifier results.
    """

    table: TableNameSpec
    classifiers: ClassifiersSpec
    operators: OperatorsSpec

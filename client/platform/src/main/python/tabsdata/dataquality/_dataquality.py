#
# Copyright 2025 Tabsdata Inc.
#

"""
This module defines the `DataQuality` action, which orchestrates the application
of data quality rules on a table. It combines classifiers and operators to perform
comprehensive data quality checks and actions.
"""

from typing import Union

from typing_extensions import TypeAlias

# noinspection PyProtectedMember
from tabsdata._specs import TableName
from tabsdata._table_actions.actions import OnTablesAction
from tabsdata._utils.validation import _normalize_to_list, _td_validate_call
from tabsdata.dataquality._classifier import Classifier
from tabsdata.dataquality._operator import Operator
from tabsdata.exceptions import ErrorCode

ClassifiersSpec: TypeAlias = Union[Classifier, list[Classifier]]
"""A type alias for one or more `Classifier` instances."""

OperatorsSpec: TypeAlias = Union[Operator, list[Operator]]
"""A type alias for one or more `Operator` instances."""


class DataQuality(OnTablesAction):
    """
    :categories: dq-action

    Represents a data quality action to be performed on a table.

    This class encapsulates a set of data quality rules, defined by classifiers
    and operators, that are applied to a specified table. It serves as a container
    for a complete data quality workflow.
    """

    @_td_validate_call(ErrorCode.DQC201)
    def __init__(
        self,
        table: TableName,
        classifiers: ClassifiersSpec,
        operators: OperatorsSpec,
    ):
        """
        Initializes a `DataQuality` action.

        Args:
            table: The name of the table to which the data quality rules will be
                   applied.
            classifiers: A single classifier or a list of classifiers to evaluate
                         the data in the table.
            operators: A single operator or a list of operators that define the
                       actions to take based on the classifier results.
        """
        self.table = table
        self.classifiers = classifiers
        self.operators = operators

    @property
    def table(self) -> TableName:
        """
        Returns the name of the table targeted by this data quality action.
        """
        return self._table

    @table.setter
    @_td_validate_call(ErrorCode.DQC202)
    def table(self, table: TableName):
        self._table = table

    @property
    def classifiers(self) -> list[Classifier]:
        """
        Returns the list of classifiers used in this data quality action.
        """
        return self._classifiers

    @classifiers.setter
    @_td_validate_call(ErrorCode.DQC203)
    def classifiers(self, classifiers: ClassifiersSpec):
        self._classifiers = _normalize_to_list(classifiers)

    @property
    def operators(self) -> list[Operator]:
        """
        Returns the list of operators that will be executed based on the
        classifier results.
        """
        return self._operators

    @operators.setter
    @_td_validate_call(ErrorCode.DQC204)
    def operators(self, operators: OperatorsSpec):
        self._operators = _normalize_to_list(operators)

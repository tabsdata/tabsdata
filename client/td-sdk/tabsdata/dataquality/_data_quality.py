#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from typing import Union

from typing_extensions import TypeAlias

from tabsdata._table_actions.actions import OnTablesAction
from tabsdata.dataquality._classifiers import Classifier
from tabsdata.dataquality._operations import Operation
from tabsdata.exceptions import DataQualityConfigurationError, ErrorCode

ClassifiersSpec: TypeAlias = Union[Classifier, list[Classifier]]
OperationsSpec: TypeAlias = Union[Operation, list[Operation]]


class DataQuality(OnTablesAction):
    """
    A data quality action on a table.
    """

    def __init__(
        self,
        table: str,
        classifiers: ClassifiersSpec,
        operations: OperationsSpec,
    ):
        """
        Constructor.

        Args:
            table: Name of the table to apply the data quality action.
            classifiers: Data quality classifiers to apply to the table.
            operations: Data quality operations, based on the defined
                classifiers, to perform on the table.
        """
        self.table = table
        self.classify = classifiers
        self.operations = operations

    @property
    def table(self) -> str:
        """
        :return: the table the data quality action operates on.
        """
        return self._table

    @table.setter
    def table(self, table: str):
        match table:
            case str():
                self._table = table
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE75, table, type(table)
                )

    @property
    def classify(self) -> list[Classifier]:
        """
        :return: the data quality classifiers.
        """
        return self._classify

    @classify.setter
    def classify(self, classify: Classifier | list[Classifier]):
        match classify:
            case Classifier():
                self._classify = [classify]
            case list():
                for v in classify:
                    if not isinstance(v, Classifier):
                        raise DataQualityConfigurationError(
                            ErrorCode.DQCE77, v, type(v)
                        )
                self._classify = classify
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE76, classify, type(classify)
                )

    @property
    def operations(self) -> list[Operation]:
        """
        :return: the data quality operations.
        """
        return self._operations

    @operations.setter
    def operations(self, operations: Operation | list[Operation]):
        match operations:
            case Operation():
                self._operations = [operations]
            case list():
                for v in operations:
                    if not isinstance(v, Operation):
                        raise DataQualityConfigurationError(
                            ErrorCode.DQCE79, v, type(v)
                        )
                self._operations = operations
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE78, operations, type(operations)
                )

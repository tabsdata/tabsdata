#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from tabsdata._table_actions.actions import OnTablesAction
from tabsdata.dataquality._classifiers import Classifier
from tabsdata.dataquality._operations import Operation, Summary
from tabsdata.exceptions import DataQualityConfigurationError, ErrorCode


class DataQuality(OnTablesAction):
    """
    Data quality actions to perform on tables.
    """

    def __init__(
        self,
        table: str,
        classify: Classifier | list[Classifier],
        operations: Operation | list[Operation] = None,
    ):
        """
        Initialize the DataQuality object.
        Args:
            table(str): Name of the table to which the data quality applies.
            classify(Classifier | list[Classifier]): One or more classifiers to
                apply to the table.
            operations(Operation | list[Operation], optional): The operations to
                perform based on the results of the classifiers. Defaults to None,
                in which case the Summary() operation will be performed.
        """
        self.table = table
        self.classify = classify
        self.operations = operations

    @property
    def table(self) -> str:
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
        if self._operations is None:
            return [Summary()]
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
            case None:
                self._operations = None
            case _:
                raise DataQualityConfigurationError(
                    ErrorCode.DQCE78, operations, type(operations)
                )

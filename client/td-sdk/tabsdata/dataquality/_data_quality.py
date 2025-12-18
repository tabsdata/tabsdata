#
# Copyright 2025 Tabs Data Inc.
#

from typing import Union

from typing_extensions import TypeAlias

from tabsdata._table_actions.actions import OnTablesAction
from tabsdata.dataquality._classifiers import Classifier
from tabsdata.dataquality._operations import Operation
from tabsdata.dataquality._utils import _normalize_to_list, _td_validate_call
from tabsdata.exceptions import ErrorCode
from tabsdata.typing._specs import TableName

ClassifiersSpec: TypeAlias = Union[Classifier, list[Classifier]]
OperationsSpec: TypeAlias = Union[Operation, list[Operation]]


class DataQuality(OnTablesAction):
    """
    A data quality action on a table.
    """

    @_td_validate_call(ErrorCode.PVDQ1)
    def __init__(
        self,
        table: TableName,
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
        self._table = table
        self._classifiers = _normalize_to_list(classifiers)
        self._operations = _normalize_to_list(operations)

    @property
    def table(self) -> str:
        """
        :return: the table the data quality action operates on.
        """
        return self._table

    @property
    def classifiers(self) -> list[Classifier]:
        """
        :return: the data quality classifiers.
        """
        return self._classifiers

    @property
    def operations(self) -> list[Operation]:
        """
        :return: the data quality operations.
        """
        return self._operations

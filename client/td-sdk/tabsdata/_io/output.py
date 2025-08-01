#
# Copyright 2025 Tabs Data Inc.
#

import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import List

from tabsdata.exceptions import (
    ErrorCode,
    OutputConfigurationError,
)

logger = logging.getLogger(__name__)


class OutputIdentifiers(Enum):
    """
    Enum for the identifiers of the different types of data outputs.
    """

    TABLE = "table-output"


class Output(ABC):
    """
    Abstract base class for managing data output configurations.
    """

    @abstractmethod
    def _to_dict(self) -> dict:
        """
        Convert the Output object to a dictionary with all
            the relevant information.

        Returns:
            dict: A dictionary with the relevant information of the Output
                object.
        """

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Output):
            return False
        return self._to_dict() == other._to_dict()

    def __repr__(self) -> str:
        """
        Returns a string representation of the Output.

        Returns:
            str: A string representation of the Output.
        """
        return f"{self.__class__.__name__}({self._to_dict()[self.IDENTIFIER]})"


class TableOutput(Output):
    """
    Class for managing the configuration of table-based data outputs.

    Attributes:
        table (str | List[str]): The table(s) to create. If multiple tables are
            provided, they must be provided as a list.

    Methods:
        to_dict(): Converts the TableOutput object to a dictionary
    """

    IDENTIFIER = OutputIdentifiers.TABLE.value

    TABLE_KEY = "table"

    def __init__(self, table: str | List[str]):
        """
        Initializes the TableOutput with the given table(s) to create.

        Args:
            table (str | List[str]): The table(s) to create. If multiple tables are
                provided, they must be provided as a list.
        """
        self.table = table

    @property
    def table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to create. If multiple tables are provided,
            they must be provided as a list.
        """
        return self._table

    @table.setter
    def table(self, table: str | List[str]):
        """
        Sets the table(s) to create.

        Args:
            table (str | List[str]): The table(s) to create. If multiple tables are
                provided, they must be provided as a list.
        """
        self._table = table
        self._table_list = table if isinstance(table, list) else [table]
        for single_table in self._table_list:
            if not isinstance(single_table, str):
                raise OutputConfigurationError(
                    ErrorCode.OCE10, single_table, type(single_table)
                )

    def _to_dict(self) -> dict:
        """
        Converts the TableOutput object to a dictionary with all the relevant
        information.
        """
        return {self.IDENTIFIER: {self.TABLE_KEY: self._table_list}}


def build_output(
    output: dict | Output | None,
) -> Output | TableOutput | None:
    """
    Builds an Output object.

    Args:
        output (dict | Output | None): A dictionary with the output information,
            or an Output object.

    Returns:
        Output: A Output object built from the output. That can be an Output object,
            or None if nothing was provided.

    Raises:
        OutputConfigurationError
    """
    if not output:
        return None
    elif isinstance(output, Output):
        return output
    elif isinstance(output, dict):
        return build_output_from_dict(output)
    else:
        raise OutputConfigurationError(ErrorCode.OCE7, type(output))


def build_output_from_dict(
    output: dict,
) -> Output | TableOutput | None:
    # The output dictionary must have exactly one key, which must be one of the
    # valid identifiers
    valid_identifiers = [element.value for element in OutputIdentifiers]
    if len(output) != 1 or next(iter(output)) not in valid_identifiers:
        raise OutputConfigurationError(
            ErrorCode.OCE3, valid_identifiers, list(output.keys())
        )
    # Since we have only one key, we select the identifier and the configuration
    identifier, configuration = next(iter(output.items()))
    # The configuration must be a dictionary
    if not isinstance(configuration, dict):
        raise OutputConfigurationError(ErrorCode.OCE4, identifier, type(configuration))
    existing_outputs = [
        TableOutput,
    ]
    for output_class in existing_outputs:
        if identifier == output_class.IDENTIFIER:
            return output_class(**configuration)

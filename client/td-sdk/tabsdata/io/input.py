#
# Copyright 2025 Tabs Data Inc.
#

import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import List

from tabsdata.exceptions import (
    ErrorCode,
    InputConfigurationError,
)
from tabsdata.tableuri import build_table_uri_object

logger = logging.getLogger(__name__)


class InputIdentifiers(Enum):
    """
    Enum for the identifiers of the different types of data inputs.
    """

    TABLE = "table-input"


class Input(ABC):
    """
    Abstract base class for managing data input configurations.
    """

    @abstractmethod
    def to_dict(self) -> dict:
        """
        Convert the Input object to a dictionary with all
            the relevant information.

        Returns:
            dict: A dictionary with the relevant information of the Input
                object.
        """

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Input):
            return False
        return self.to_dict() == other.to_dict()

    def __repr__(self) -> str:
        """
        Returns a string representation of the Input.

        Returns:
            str: A string representation of the Input.
        """
        return f"{self.__class__.__name__}({self.to_dict()[self.IDENTIFIER]})"


class TableInput(Input):
    """
    Class for managing the configuration of table-based data inputs.

    Attributes:
        table (str | List[str]): The table(s) to load.

    Methods:
        to_dict(): Converts the TableInput object to a dictionary.
    """

    IDENTIFIER = InputIdentifiers.TABLE.value

    TABLE_KEY = "table"

    def __init__(self, table: str | List[str]):
        """
        Initializes the TableInput with the given tables. If multiple tables are
            provided, they must be provided as a list.

        Args:
            table (str | List[str]): The table(s) to load.
                If multiple tables are provided, they must be provided as a list.
        """
        self.table = table

    @property
    def table(self) -> str | List[str]:
        """
        str | List[str]: The table(s) to load.
        """
        return self._table

    @table.setter
    def table(self, table: str | List[str]):
        """
        Sets the table(s) to load.

        Args:
            table (str | List[str]): The table(s) to load.
                If multiple tables are provided, they must be provided as a list
        """
        self._table = table
        if isinstance(table, list):
            assert [build_table_uri_object(single_uri) for single_uri in table]
            self._table = table
            self._table_list = self._table
        else:
            assert build_table_uri_object(table)
            self._table = table
            self._table_list = [self._table]
        self._verify_valid_table_list()

    def _verify_valid_table_list(self):
        """
        Verifies that the tables in the list are valid.
        """
        for table in self._table_list:
            uri = build_table_uri_object(table)
            if not uri.table:
                raise InputConfigurationError(ErrorCode.ICE25, table)

    def to_dict(self) -> dict:
        """
        Converts the TableInput object to a dictionary with all the relevant
            information.

        Returns:
            dict: A dictionary with the relevant information of the Output
                object.
        """
        return {self.IDENTIFIER: {self.TABLE_KEY: self._table_list}}


def build_input(input: dict | Input | None) -> Input | None:
    """
    Builds an Input object.

    Args:
        input (dict | Input | None): A dictionary with the input information or an
            Input object.

    Returns:
        Input: A Input object built from the input.

    Raises:
        InputConfigurationError
    """
    if not input:
        return None
    elif isinstance(input, Input):
        return input
    elif isinstance(input, dict):
        return build_input_from_dict(input)
    else:
        raise InputConfigurationError(ErrorCode.ICE11, type(input))


def build_input_from_dict(input: dict) -> Input:
    valid_identifiers = [element.value for element in InputIdentifiers]
    # The input dictionary must have exactly one key, which must be one of the
    # valid identifiers
    if len(input) != 1 or next(iter(input)) not in valid_identifiers:
        raise InputConfigurationError(
            ErrorCode.ICE7, valid_identifiers, list(input.keys())
        )
    # Since we have only one key, we select the identifier and the configuration
    identifier, configuration = next(iter(input.items()))
    # The configuration must be a dictionary
    if not isinstance(configuration, dict):
        raise InputConfigurationError(ErrorCode.ICE8, identifier, type(configuration))
    existing_inputs = [
        TableInput,
    ]
    for input_class in existing_inputs:
        if identifier == input_class.IDENTIFIER:
            return input_class(**configuration)

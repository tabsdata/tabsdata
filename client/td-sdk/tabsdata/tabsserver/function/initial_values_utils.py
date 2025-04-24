#
# Copyright 2024 Tabs Data Inc.
#

import logging

import polars as pl

from tabsdata.tabsserver.function.logging_utils import pad_string

from .global_utils import convert_uri_to_path
from .yaml_parsing import InputYaml

logger = logging.getLogger(__name__)

INITIAL_VALUES_LAST_MODIFIED_VARIABLE_NAME = "last_modified"
INITIAL_VALUES_TABLE_NAME = "td-initial-values"
INITIAL_VALUES_VARIABLE_COLUMN = "variable"
INITIAL_VALUES_VALUE_COLUMN = "value"


class InitialValues:
    """
    A class to represent the initial values to store at the end of the function
    execution.
    """

    def __init__(self):
        self.new_initial_values = {}
        self.current_initial_values = {}
        self.returns_values = False

    def add_new_value(self, variable_name: str, value: str):
        """
        Add a value to the initial values to store.

        Args:
            variable_name: The name of the variable.
            value: The value of the variable.
        """
        logger.debug(f"Adding initial value '{variable_name}': '{value}'")
        self.new_initial_values[variable_name] = value

    def update_new_values(self, new_values: dict):
        """
        Update the values of the initial values to store.

        Args:
            new_values: The new values to update.
        """
        logger.debug(f"Updating initial values with {new_values}")
        if new_values is None:
            self.new_initial_values = {}
        else:
            self.new_initial_values.update(new_values)

    @property
    def returns_values(self) -> bool:
        return self._returns_values

    @returns_values.setter
    def returns_values(self, returns_values: bool):
        logger.debug(f"Setting initial values 'returns_values' to {returns_values}")
        self._returns_values = returns_values

    def __str__(self):
        return str(self.new_initial_values)

    def load_current_initial_values(self, request: InputYaml):
        """
        Load the current initial values from the execution context.

        Args:
            request: The request information.
        """
        logger.debug("Loading current initial values")
        if request:
            system_input = request.system_input
            logger.debug(f"System input: {system_input}")
            td_initial_values_table = None
            if system_input:
                for table in system_input:
                    if table.name == INITIAL_VALUES_TABLE_NAME:
                        td_initial_values_table = table
                        break
            td_initial_values_uri = None
            if td_initial_values_table:
                logger.debug(f"TD initial values table: {td_initial_values_table}")
                logger.debug(
                    f"TD initial values location: {td_initial_values_table.location}"
                )
                td_initial_values_uri = td_initial_values_table.uri
            logger.debug(f"TD initial values URI: {td_initial_values_uri}")
            if td_initial_values_uri:
                try:
                    td_initial_values_frame = pl.read_parquet(td_initial_values_uri)
                    logger.debug(f"TD initial values: {td_initial_values_frame}")
                    df_dict = td_initial_values_frame.to_dict(as_series=False)
                    self.current_initial_values = dict(
                        zip(
                            df_dict[INITIAL_VALUES_VARIABLE_COLUMN],
                            df_dict[INITIAL_VALUES_VALUE_COLUMN],
                        )
                    )
                except Exception as e:
                    logger.error(
                        "Error retrieving initial values from"
                        f" {td_initial_values_uri}: {e}"
                    )
                    raise ValueError(
                        "Error retrieving initial values from"
                        f" {td_initial_values_uri}: {e}"
                    )

        logger.debug(f"Current initial values: {self.current_initial_values}")

    def store(self, execution_context: InputYaml) -> bool:
        """
        Store the initial values in the global variable.

        Args:
            execution_context: The execution context.

        Returns:
            True if the initial values were stored successfully, False otherwise.
        """
        logger.info(pad_string("[Storing execution information]"))
        logger.info(f"Storing initial values {str(self)}")

        _new_initial_values = self.new_initial_values
        if self.new_initial_values is None or self.new_initial_values == {}:
            logger.warning(
                "No initial values to store. Using old initial values instead."
            )
            # When no new initial values are provided, the current initial values are
            # used instead, as it is assumed no new data was read.
            _new_initial_values = self.current_initial_values

        system_output = execution_context.system_output
        destination_table_uri = None
        if system_output:
            for table in system_output:
                if table.name == INITIAL_VALUES_TABLE_NAME:
                    destination_table_uri = table.uri
                    logger.info(
                        f"Found the table '{INITIAL_VALUES_TABLE_NAME}' with "
                        f"URI '{destination_table_uri}'"
                    )
                    break
        if destination_table_uri:
            variables_column = []
            values_column = []
            for variable_name, value in _new_initial_values.items():
                variables_column.append(variable_name)
                values_column.append(value)
            df = pl.DataFrame(
                {
                    INITIAL_VALUES_VARIABLE_COLUMN: variables_column,
                    INITIAL_VALUES_VALUE_COLUMN: values_column,
                }
            )
            logger.info(
                f"Storing the initial values {df} in the table "
                f"'{INITIAL_VALUES_TABLE_NAME}' with URI '{destination_table_uri}'"
            )
            df.write_parquet(convert_uri_to_path(destination_table_uri))
            logger.debug("Initial values stored successfully.")
            return True
        else:
            logger.warning(
                f"The URI of the table '{INITIAL_VALUES_TABLE_NAME}' was None. No "
                "values were stored"
            )
            return False

#
# Copyright 2024 Tabs Data Inc.
#
import copy
import logging
import os
from typing import Literal

import polars as pl

from tabsdata.tabsserver.function.logging_utils import pad_string

from .global_utils import convert_uri_to_path
from .yaml_parsing import InputYaml

logger = logging.getLogger(__name__)

INITIAL_VALUES_LAST_MODIFIED_VARIABLE_NAME = "last_modified"
INITIAL_VALUES_LIST_POSITION = 0
INITIAL_VALUES_VARIABLE_COLUMN = "variable"
INITIAL_VALUES_VALUE_COLUMN = "value"

NEW_MODE = "NEW"
NONE_MODE = "NONE"
SAME_MODE = "SAME"
VALID_UPDATE_MODES = [
    NEW_MODE,
    NONE_MODE,
    SAME_MODE,
]
VALID_UPDATE_MODES_HINT = Literal["NEW", "NONE", "SAME"]

INITIAL_VALUES_VALID_VALUE_TYPES = (str,)


class InitialValues:
    """
    A class to represent the initial values to store at the end of the function
    execution.
    """

    def __init__(self):
        self.new_initial_values = {}
        self.loaded_initial_values = {}
        self.current_initial_values = {}
        self.returns_values = False
        self.update_mode = NONE_MODE
        self.output_table_name = None
        self.use_decorator_values = True

    @property
    def loaded_initial_values(
        self,
    ) -> dict:
        """
        The initial values loaded from the request. This should NEVER be
        modified after loading from the request, using instead current_initial_values
        during function execution.
        """
        return self._loaded_initial_values

    @loaded_initial_values.setter
    def loaded_initial_values(
        self,
        loaded_initial_values: dict,
    ):
        """
        The initial values loaded from the request. This should NEVER be
        modified after loading from the request, using instead current_initial_values
        during function execution.
        """
        logger.debug(f"Setting loaded initial values to {loaded_initial_values}")
        self._loaded_initial_values = loaded_initial_values

    @property
    def update_mode(
        self,
    ) -> VALID_UPDATE_MODES_HINT:
        """
        Indicates what mode will be used for updating the initial values.
            Currently, this is hidden from the end-user as this is purely internal,
            but it sets the foundation to show it eventually.
        """
        logger.debug(f"Getting initial values 'update_mode': {self._update_mode}")
        return self._update_mode

    @update_mode.setter
    def update_mode(
        self,
        update_mode: VALID_UPDATE_MODES_HINT,
    ):
        """
        Indicates what mode will be used for updating the initial values.
            Currently, this is hidden from the end-user as this is purely internal,
            but it sets the foundation to show it eventually.
        """
        logger.debug(f"Setting initial values 'update_mode' to {update_mode}")
        if update_mode not in VALID_UPDATE_MODES:
            raise ValueError(
                f"Invalid value for update_mode: {update_mode}. "
                f"Must be one of {VALID_UPDATE_MODES}."
            )
        self._update_mode = update_mode

    def update_new_values(self, new_values: dict | None):
        """
        Update the values to store.

        Args:
            new_values: The new values.
        """
        logger.debug(f"Updating initial values with {new_values}")
        if new_values == SAME_MODE:
            # If new_values is "SAME", we want to 'freeze' the value of
            # the current execution and use it in the next. Therefore, we mark
            # update_mode as 'SAME', so that no new value is sent in the response
            # yaml, and the one used in this execution is used in the next.
            self.update_mode = SAME_MODE
        elif isinstance(new_values, dict):
            # If new_values is a non-empty dictionary, we want to update the initial
            # values for the next execution. Therefore, we will store the new values.
            if all(
                isinstance(k, str) and isinstance(v, INITIAL_VALUES_VALID_VALUE_TYPES)
                for k, v in new_values.items()
            ):
                self.new_initial_values.update(new_values)
                self.update_mode = NEW_MODE
            else:
                logger.error(
                    f"Invalid type for new initial values: {new_values}. "
                    "The dictionary provided must have all keys of type 'str' "
                    f"and all values of a type in '{INITIAL_VALUES_VALID_VALUE_TYPES}'."
                )
                raise TypeError(
                    f"Invalid type for new initial values: {new_values}. "
                    "The dictionary provided must have all keys of type 'str' "
                    f"and all values of a type in '{INITIAL_VALUES_VALID_VALUE_TYPES}'."
                )
        else:
            # If we are in none of the above cases, we have an invalid type or value for
            # new_values. We will log an error and raise a TypeError.
            logger.error(
                "Invalid type or value for new initial values:"
                f" type = {type(new_values)}, value = {new_values}."
                " No initial values stored."
            )
            raise TypeError(
                "Invalid type or value for new initial values:"
                f" type = {type(new_values)}, value = {new_values}."
                " No initial values stored."
            )

    @property
    def returns_values(self) -> bool:
        """
        Indicates whether the function will return initial values after
            execution or not. Note that even if false, the function might still
            use or update initial values, but they will not be obtained after
            function execution (for example the 'last_modified' variable for certain
            file sources).
        """
        return self._returns_values

    @returns_values.setter
    def returns_values(self, returns_values: bool):
        """
        Indicates whether the function will return initial values after
            execution or not. Note that even if false, the function might still
            use or update initial values, but they will not be obtained after
            function execution (for example the 'last_modified' variable for certain
            file sources).
        """
        logger.debug(f"Setting initial values 'returns_values' to {returns_values}")
        self._returns_values = returns_values

    def __str__(self):
        return (
            f"< Old value: {str(self.loaded_initial_values)} ; new value:"
            f" {str(self.new_initial_values)} ; auxiliary value: "
            f"{str(self.current_initial_values)} ; returns values:"
            f" {str(self.returns_values)} ; update mode:"
            f" {str(self.update_mode)}, use decorator values: "
            f"{str(self.use_decorator_values)} >"
        )

    def load_current_initial_values(self, request: InputYaml):
        """
        Load the current initial values from the execution context.

        Args:
            request: The request information.
        """
        logger.debug("Loading current initial values")
        system_input = request.system_input
        logger.debug(f"System input: {system_input}")
        td_initial_values_table = system_input[INITIAL_VALUES_LIST_POSITION]
        logger.debug(f"TD initial values table: {td_initial_values_table}")
        logger.debug(f"TD initial values location: {td_initial_values_table.location}")
        td_initial_values_uri = td_initial_values_table.uri
        logger.debug(f"TD initial values URI: {td_initial_values_uri}")
        if td_initial_values_uri:
            try:
                td_initial_values_frame = pl.read_parquet(td_initial_values_uri)
                logger.debug(f"TD initial values: {td_initial_values_frame}")
                df_dict = td_initial_values_frame.to_dict(as_series=False)
                self.loaded_initial_values = (
                    dict(
                        zip(
                            df_dict[INITIAL_VALUES_VARIABLE_COLUMN],
                            df_dict[INITIAL_VALUES_VALUE_COLUMN],
                        )
                    )
                    if df_dict
                    else {}
                )
            except Exception as e:
                logger.error(
                    f"Error retrieving initial values from {td_initial_values_uri}: {e}"
                )
                raise
            self.current_initial_values = copy.deepcopy(self.loaded_initial_values)
            self.update_mode = NEW_MODE
            self.use_decorator_values = False
        logger.debug(f"Current initial values: {self.current_initial_values}")

    @property
    def changed(self) -> bool:
        """
        Indicates whether the initial values have changed or not.

        Returns:
            True if the initial values have changed, False otherwise.
        """
        logger.debug("Checking if initial values have changed")
        if self.update_mode in [NONE_MODE, SAME_MODE]:
            logger.debug(
                "Initial values have not changed. Update mode is NONE or SAME."
            )
            return False
        return self.new_initial_values != self.loaded_initial_values

    def store(self, execution_context: InputYaml):
        """
        Store the initial values in the global variable.

        Args:
            execution_context: The execution context.

        Returns:
            True if the initial values were stored successfully, False otherwise.
        """
        logger.info(pad_string("[Storing execution information]"))
        logger.info(f"Storing initial values {str(self)}")
        system_output = execution_context.system_output
        initial_values_output_table = system_output[INITIAL_VALUES_LIST_POSITION]
        self.output_table_name = initial_values_output_table.name
        destination_table_uri = initial_values_output_table.uri
        logger.info(
            f"Found the table '{initial_values_output_table.name}' with "
            f"URI '{destination_table_uri}'"
        )

        if not self.changed:
            logger.info("Values have not changed. No values were stored.")
            return

        if destination_table_uri:
            variables_column = []
            values_column = []
            for variable_name, value in self.new_initial_values.items():
                variables_column.append(variable_name)
                values_column.append(value)
            initial_values_aux_dict = {
                INITIAL_VALUES_VARIABLE_COLUMN: variables_column,
                INITIAL_VALUES_VALUE_COLUMN: values_column,
            }
            df = pl.LazyFrame(initial_values_aux_dict)
            logger.info(
                f"Storing the initial values {initial_values_aux_dict} in the table "
                f"'{initial_values_output_table.name}' with URI "
                f"'{destination_table_uri}'"
            )

            if destination_table_uri.startswith("file://"):
                try:
                    os.makedirs(
                        os.path.dirname(convert_uri_to_path(destination_table_uri)),
                        exist_ok=True,
                    )
                except Exception as e:
                    logger.warning(
                        "Error creating the directory for the initial values table"
                        f" with URI '{destination_table_uri}': {e}"
                    )
                    logger.warning("This might be because file is not local")

            df.sink_parquet(destination_table_uri, mkdir=True)
            logger.debug("Initial values stored successfully.")
            return
        else:
            raise ValueError(
                f"The URI of the '{initial_values_output_table.name}' table was None. "
                "No values were stored"
            )

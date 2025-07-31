#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import copy
import logging
from enum import Enum
from typing import TYPE_CHECKING, Literal

import polars as pl

from tabsdata._tabsserver.function.native_tables_utils import (
    scan_lf_from_location,
    sink_lf_to_location,
)
from tabsdata._tabsserver.function.store_results_utils import (
    get_table_meta_info_from_lf,
)

if TYPE_CHECKING:
    from tabsdata._tabsserver.function.execution_context import ExecutionContext
    from tabsdata._tabsserver.function.yaml_parsing import InputYaml

logger = logging.getLogger(__name__)

OFFSET_LAST_MODIFIED_VARIABLE_NAME = "last_modified"
OFFSET_LIST_POSITION = 0

NEW_MODE = "NEW"
NONE_MODE = "NONE"
SAME_MODE = "SAME"
VALID_UPDATE_MODES = [
    NEW_MODE,
    NONE_MODE,
    SAME_MODE,
]
VALID_UPDATE_MODES_HINT = Literal["NEW", "NONE", "SAME"]


class OffsetReturn(Enum):
    """
    Enum to indicate how the new offset is obtained.
    """

    ATTRIBUTE = "object_attribute"
    FUNCTION = "function_return"


class Offset:
    """
    A class to represent the offset to store at the end of the function
    execution.
    """

    def __init__(self):
        self.new_offset = {}
        self.loaded_offset = {}
        self.current_offset = {}
        self.returns_values = False
        self.update_mode = NONE_MODE
        self.output_table_name = None
        self.use_decorator_values = True
        self.meta_info = {}

    @property
    def loaded_offset(
        self,
    ) -> dict:
        """
        The offset loaded from the request. This should NEVER be
        modified after loading from the request, using instead current_offset
        during function execution.
        """
        return self._loaded_offset

    @loaded_offset.setter
    def loaded_offset(
        self,
        loaded_offset: dict,
    ):
        """
        The offset loaded from the request. This should NEVER be
        modified after loading from the request, using instead current_offset
        during function execution.
        """
        logger.debug(f"Setting loaded offset to {loaded_offset}")
        self._loaded_offset = loaded_offset

    @property
    def update_mode(
        self,
    ) -> VALID_UPDATE_MODES_HINT:
        """
        Indicates what mode will be used to update the offset.
            Currently, this is hidden from the end-user as this is purely internal,
            but it sets the foundation to show it eventually.
        """
        logger.debug(f"Getting offset 'update_mode': {self._update_mode}")
        return self._update_mode

    @update_mode.setter
    def update_mode(
        self,
        update_mode: VALID_UPDATE_MODES_HINT,
    ):
        """
        Indicates what mode will be used to update the offset.
            Currently, this is hidden from the end-user as this is purely internal,
            but it sets the foundation to show it eventually.
        """
        logger.debug(f"Setting offset 'update_mode' to {update_mode}")
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
        logger.debug(f"Updating offset with {new_values}")
        if new_values == SAME_MODE:
            # If new_values is "SAME", we want to 'freeze' the value of
            # the current execution and use it in the next. Therefore, we mark
            # update_mode as 'SAME', so that no new value is sent in the response
            # yaml, and the one used in this execution is used in the next.
            self.update_mode = SAME_MODE
        elif isinstance(new_values, dict):
            # If new_values is a non-empty dictionary, we want to update the offset
            # for the next execution. Therefore, we will store the new values.
            if all(isinstance(k, str) for k, v in new_values.items()):
                self.new_offset.update(new_values)
                self.update_mode = NEW_MODE
            else:
                logger.error(
                    f"Invalid type for new offset: {new_values}. "
                    "The dictionary provided must have all keys of type 'str'."
                )
                raise TypeError(
                    f"Invalid type for new offset: {new_values}. "
                    "The dictionary provided must have all keys of type 'str'."
                )
        else:
            # If we are in none of the above cases, we have an invalid type or value for
            # new_values. We will log an error and raise a TypeError.
            logger.error(
                "Invalid type or value for new offset:"
                f" type = {type(new_values)}, value = {new_values}."
                " No offset stored."
            )
            raise TypeError(
                "Invalid type or value for new offset:"
                f" type = {type(new_values)}, value = {new_values}."
                " No offset stored."
            )

    @property
    def returns_values(self) -> bool:
        """
        Indicates whether the function will return an offset after
            execution or not. Note that even if false, the function might still
            use or update the offset, but they will not be obtained after
            function execution (for example the 'last_modified' variable for certain
            file sources is not returned after execution, but the new offset for a
            mysql is part of the return of the function).
        """
        return self._returns_values

    @returns_values.setter
    def returns_values(self, returns_values: bool):
        """
        Indicates whether the function will return an offset after
            execution or not. Note that even if false, the function might still
            use or update the offset, but they will not be obtained after
            function execution (for example the 'last_modified' variable for certain
            file sources is not returned after execution, but the new offset for a
            mysql is part of the return of the function).
        """
        logger.debug(f"Setting offset 'returns_values' to {returns_values}")
        self._returns_values = returns_values

    def __str__(self):
        return (
            f"< Old value: {str(self.loaded_offset)} ; new value:"
            f" {str(self.new_offset)} ; auxiliary value: "
            f"{str(self.current_offset)} ; returns values:"
            f" {str(self.returns_values)} ; update mode:"
            f" {str(self.update_mode)}, use decorator values: "
            f"{str(self.use_decorator_values)} >"
        )

    def load_current_offset(
        self, request: InputYaml, execution_context: ExecutionContext
    ):
        """
        Load the current offset from the execution context.

        Args:
            request: The request information.
        """
        logger.debug("Loading current offset")
        system_input = request.system_input
        logger.debug(f"System input: {system_input}")
        td_offset_table = system_input[OFFSET_LIST_POSITION]
        logger.debug(f"TD offset table: {td_offset_table}")
        logger.debug(f"TD offset table location: {td_offset_table.location}")
        td_offset_uri = td_offset_table.uri
        logger.debug(f"TD offset table URI: {td_offset_uri}")
        if td_offset_uri:
            try:
                td_offset_frame = scan_lf_from_location(
                    execution_context, td_offset_table.location
                ).collect()
                logger.debug(f"TD offset value: {td_offset_frame}")
                if len(td_offset_frame) != 1:
                    # We have more than one row, which is not allowed in the current
                    # scheme of having each variable in a column of name the name of
                    # the variable and value the value of the variable
                    raise ValueError(
                        f"Offset table {td_offset_frame} has more than one row. "
                        "This is not allowed in the current scheme of having "
                        "each variable in a column of name the name of the variable "
                        "and value the value of the variable."
                    )
                df_dict = td_offset_frame.to_dict(as_series=False)
                self.loaded_offset = (
                    {key: value[0] for key, value in df_dict.items()} if df_dict else {}
                )
            except Exception as e:
                logger.error(f"Error retrieving offset from {td_offset_uri}: {e}")
                raise
            self.current_offset = copy.deepcopy(self.loaded_offset)
            self.update_mode = NEW_MODE
            self.use_decorator_values = False
        else:
            # If the URI is None, we will not load any offset, and we will
            # use the current offset as an empty dictionary.
            logger.debug(
                f"The URI of the offset table '{td_offset_table.name}' is None. "
                "No values were loaded."
            )
            self.loaded_offset = {}
            self.current_offset = {}
            self.update_mode = NONE_MODE
            self.use_decorator_values = True
        logger.debug(f"Current offset: {self.current_offset}")

    @property
    def changed(self) -> bool:
        """
        Indicates whether the offset has changed or not.

        Returns:
            True if the offset has changed, False otherwise.
        """
        logger.debug("Checking if the offset has changed")
        if self.update_mode in [NONE_MODE, SAME_MODE]:
            logger.debug("Update mode is NONE or SAME, so the offset has not changed.")
            return False
        changed = self.new_offset != self.loaded_offset
        if changed:
            logger.debug("The offset has changed.")
        else:
            logger.debug("The offset has not changed.")
        return changed

    def store(self, request: InputYaml, execution_context: ExecutionContext):
        """
        Store the offset in the global variable.

        Args:
            request: The execution context.

        """
        logger.info(f"Storing offset {str(self)}")
        system_output = request.system_output
        offset_output_table = system_output[OFFSET_LIST_POSITION]
        self.output_table_name = offset_output_table.name
        destination_table_uri = offset_output_table.uri
        logger.info(
            f"Found the table '{offset_output_table}' with "
            f"URI '{destination_table_uri}'"
        )

        if not self.changed:
            logger.info("Values have not changed. No values were stored.")
            return

        if destination_table_uri:
            df = pl.DataFrame(self.new_offset)
            logger.info(
                f"Storing the offset {df} in the table "
                f"'{offset_output_table.name}' with URI "
                f"'{destination_table_uri}'"
            )

            lf = pl.LazyFrame(df)

            self.meta_info = get_table_meta_info_from_lf(lf)

            logger.debug(f"Performing sink to file {destination_table_uri}")
            sink_lf_to_location(lf, execution_context, offset_output_table.location)
            logger.debug("Offset stored successfully.")
            return
        else:
            raise ValueError(
                f"The URI of the '{offset_output_table.name}' table was None. "
                "No values were stored"
            )

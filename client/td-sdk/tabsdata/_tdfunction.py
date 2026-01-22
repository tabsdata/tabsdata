#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import inspect
import logging
import os
from typing import TYPE_CHECKING, Any, Callable, Type

import polars as pl

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._helpers as td_helpers
import tabsdata.tableframe.lazyframe.frame as td_frame
from tabsdata._crontab import Crontab
from tabsdata._io.inputs.stage_input import Stage
from tabsdata._io.inputs.table_inputs import TableInput
from tabsdata._io.outputs.table_outputs import TableOutput
from tabsdata._io.plugin import DestinationPlugin, SourcePlugin
from tabsdata._io.stage_trigger import StageTrigger
from tabsdata._table_actions._typing import OnTablesSpec
from tabsdata._table_actions.actions import (
    OnTablesAction,
    _build_actions_list_or_raise,
)
from tabsdata._tableuri import build_table_uri_object

# noinspection PyProtectedMember
from tabsdata._utils.tableframe._translator import _unwrap_table_frame
from tabsdata.exceptions import (
    ErrorCode,
    FunctionConfigurationError,
)
from tabsdata.tableframe.lazyframe.properties import TableFramePropertiesBuilder

if TYPE_CHECKING:
    import pandas as pd

# noinspection PyProtectedMember

logger = logging.getLogger(__name__)


class TabsdataFunction:
    """
    Class to decorate a function with metadata and methods for use in a Tabsdata
        environment.

    Attributes:

    """

    def __init__(
        self,
        func: Callable,
        name: str | None,
        input: SourcePlugin = None,
        output: DestinationPlugin = None,
        trigger_by: str | list[str] | Crontab | StageTrigger | None = None,
        # input_actions: OnTablesSpec = None,TODO: Uncomment this when reverting TD-1089
        output_actions: OnTablesSpec = None,
    ):
        """
        Initializes the TabsDataFunction with the given function, input, output and
        trigger.

        Args:
            func: The function to decorate.
            name: The name with which the function will
                be registered. If None, the original_function name will be used.
            input: The data to be used when
                running the function. Must be an instance of SourcePlugin.
            output (DestinationPlugin, optional): The location where the
                function results will be saved when run. Must be an instance of
                DestinationPlugin.
            trigger_by: The trigger(s) that will cause the
                function to execute. It can be a table in the system, a list of
                tables or None (in which case it will be inferred from the
                dependencies).
            output_actions: Actions to perform on the
                output tables.
        Raises:
            FunctionConfigurationError
            InputConfigurationError
            OutputConfigurationError
            FormatConfigurationError
        """
        self.original_function = func
        self.output = output
        self.input = input
        self._func_original_folder, self._func_original_file = os.path.split(
            inspect.getfile(func)
        )
        self.trigger_by = trigger_by
        self.name = name
        # self.input_actions = input_actions TODO: Uncomment this when reverting TD-1089
        self.output_actions = output_actions

    def __repr__(self) -> str:
        """
        Returns a string representation of the TabsDataFunction.

        Returns:
            str: A string representation of the TabsDataFunction.
        """
        information_to_give = [
            f"type='{self.type}'",
            f"input='{self.input}'",
            f"output='{self.output}'",
            f"original_file='{self.original_file}'",
            f"original_folder='{self.original_folder}'",
            f"trigger='{self.trigger_by}'",
        ]
        # if self.input_actions: TODO: Uncomment this when reverting TD-1089
        #    information_to_give.append(f"input_actions='{self.input_actions}'")
        if self.output_actions:
            information_to_give.append(f"output_actions='{self.output_actions}'")
        information_to_give_str = ", ".join(information_to_give)
        return (
            f"{self.__class__.__name__}({self._func.__name__})"
            f"({information_to_give_str})"
        )

    def __call__(self, *args, **kwargs):
        """
        Calls the original function with the given arguments and keyword arguments.

        Args:
            *args: Positional arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            Any: The return value of the original function
        """
        new_args = _convert_recursively_to_tableframe(args)
        new_kwargs = _convert_recursively_to_tableframe(kwargs)
        result = self._func(*new_args, **new_kwargs)
        data_type = _recursively_obtain_datatype(args) or _recursively_obtain_datatype(
            kwargs
        )
        if data_type:
            return _clean_recursively_and_convert_to_datatype(result, data_type)
        else:
            return result

    @property
    def type(self) -> str:
        """
        str: The type of the function. It can be "publisher", "transformer" or
            "subscriber".
        """
        table_input = isinstance(self.input, TableInput)
        table_output = isinstance(self.output, TableOutput)
        if table_input and table_output:
            return "transformer"
        elif table_input and not table_output:
            return "subscriber"
        elif not table_input and table_output:
            return "publisher"
        else:
            self._verify_valid_input_output()
            raise ValueError("Unable to determine function type.")

    @property
    def input(self) -> SourcePlugin | None:
        """
        SourcePlugin | None: The data to be used when running the function.
        """
        return self._input

    @input.setter
    def input(self, input: SourcePlugin | None):
        """
        Sets the input data for the function.

        Args:
            input (SourcePlugin | None): The data to be used when running the
                function. Can be an instance of
                SourcePlugin or None.
        """
        if isinstance(input, SourcePlugin):
            self._input = input
        elif input is None:
            self._input = None
        else:
            raise FunctionConfigurationError(ErrorCode.FCE7, type(input))
        self._verify_valid_input_output()
        self._verify_valid_action_plugin_combination()
        self._validate_trigger_by()

    @property
    def original_folder(self) -> str:
        """
        str: The folder where the original function is defined, as a local path in the
            user's computer.
        """
        return self._func_original_folder

    @property
    def original_file(self):
        """
        str: The file where the original function is defined in the user's computer
        """
        return self._func_original_file

    @property
    def original_function(self) -> Callable:
        """
        Callable: The original function that was decorated, without any behaviour
            modifications.
        """
        return self._func

    @original_function.setter
    def original_function(self, func: Callable):
        """
        Sets the original function for the TabsDataFunction.

        Args:
            func (Callable): The original function that was decorated, without any
                behaviour modifications.
        """
        if not callable(func):
            raise FunctionConfigurationError(ErrorCode.FCE1, type(func))
        self._func = func

    @property
    def output(self) -> DestinationPlugin | None:
        """
        DestinationPlugin: The location where the function results will be saved when
            run.
        """
        return self._output

    @output.setter
    def output(self, output: DestinationPlugin | None):
        """
        Sets the output location for the function.

        Args:
            output (DestinationPlugin | None): The location where the
                function results will be saved when run.
        """
        if isinstance(output, DestinationPlugin):
            self._output = output
        elif output is None:
            self._output = None
        else:
            raise FunctionConfigurationError(ErrorCode.FCE8, type(output))
        self._verify_valid_input_output()
        self._verify_valid_action_plugin_combination()

    @property
    def name(self) -> str:
        """
        str: The name with which the function will be registered.
        """
        return self._name or self.original_function.__name__

    @name.setter
    def name(self, name: str | None):
        """
        Sets the name with which the function will be registered.

        Args:
            name (str | None): The name with which the function will be
                registered. If None, the original_function name will be used.
        """
        if isinstance(name, str) or name is None:
            self._name = name
        else:
            raise FunctionConfigurationError(ErrorCode.FCE6, type(name))

    @property
    def trigger_by(self) -> list[str] | Crontab | StageTrigger | None:
        """
        List[str]: The trigger(s) that will cause the function to execute. It must be
            another table or tables in the system, a Crontab, or a Watchdog in the
            case of a publisher.
        """
        return self._trigger_by

    @trigger_by.setter
    def trigger_by(self, trigger_by: str | list[str] | Crontab | StageTrigger | None):
        """
        Sets the trigger(s) that will cause the function to execute

        Args:
            trigger_by (str | list[str] | StageTrigger | Crontab | None): The trigger(s)
                that will
                cause the function to execute. It must be another table or tables in
                the system, a Crontab, or a Watchdog in the case of a publisher. If
                None, all the tables in the dependencies will be used.
        """
        if isinstance(trigger_by, str):
            trigger_by = [trigger_by]

        if trigger_by is None:
            self._trigger_by = None
        elif isinstance(trigger_by, list):
            self._trigger_by = trigger_by
        elif isinstance(trigger_by, Crontab):
            self._trigger_by = trigger_by
        elif isinstance(trigger_by, StageTrigger):
            self._trigger_by = trigger_by
        else:
            raise FunctionConfigurationError(ErrorCode.FCE2, type(trigger_by))

        if isinstance(self._trigger_by, list):
            for trigger in self._trigger_by:
                if not isinstance(trigger, str):
                    raise FunctionConfigurationError(ErrorCode.FCE2, type(trigger))
                trigger_uri = build_table_uri_object(trigger)
                if not trigger_uri.table:
                    raise FunctionConfigurationError(ErrorCode.FCE3, trigger)

        self._validate_trigger_by()

        # @property TODO: Uncomment this when reverting TD-1089
        # def input_actions(self) -> list[OnTablesAction]:
        """
        list[OnTablesAction]: Actions to perform on the input tables.
        """
        #    return self._input_actions

        # @input_actions.setter TODO: Uncomment this when reverting TD-1089
        # def input_actions(self, actions: OnTablesSpec):
        """
        Sets the actions to perform on the input tables.

        Args:
            actions (OnTablesSpec): Actions to perform on the input
                tables.
        """
        # self._input_actions = _build_actions_list_or_raise(
        #    actions, ErrorCode.FCE9, FunctionConfigurationError
        # )
        # self._verify_valid_action_plugin_combination()

    @property
    def output_actions(self) -> list[OnTablesAction]:
        """
        list[OnTablesAction]: Actions to perform on the output tables.
        """
        return self._output_actions

    @output_actions.setter
    def output_actions(self, actions: OnTablesSpec):
        """
        Sets the actions to perform on the output tables.

        Args:
            actions (OnTablesSpec): Actions to perform on the output
                tables.
        """
        self._output_actions = _build_actions_list_or_raise(
            actions, ErrorCode.FCE10, FunctionConfigurationError
        )
        self._verify_valid_action_plugin_combination()

    def _validate_trigger_by(self):
        if (
            hasattr(self, "_trigger_by")
            and hasattr(self, "_input")
            and self._input
            and self._trigger_by
        ):
            if isinstance(self._trigger_by, StageTrigger):
                if not isinstance(self._input, Stage):
                    raise FunctionConfigurationError(
                        ErrorCode.FCE14, self.input.__class__.__name__
                    )

    def _verify_valid_input_output(self):
        """
        Verifies that the input and output are valid for the function.

        Raises:
            FunctionConfigurationError
        """
        if hasattr(self, "_input") and hasattr(self, "_output"):
            is_not_table_input = self.input and not isinstance(self.input, TableInput)
            is_not_table_output = self.output and not isinstance(
                self.output, TableOutput
            )
            if is_not_table_input and is_not_table_output:
                raise FunctionConfigurationError(
                    ErrorCode.FCE5, type(self.input), type(self.output)
                )

    def _verify_valid_action_plugin_combination(self):
        """
        Verifies that the input and output actions are valid for the function.

        Raises:
            FunctionConfigurationError
        """
        if hasattr(self, "_input") and hasattr(self, "_input_actions"):
            if (
                self.input
                and not isinstance(self.input, TableInput)
                and self.input_actions
            ):
                # raise error if input actions are provided for non-table input
                raise FunctionConfigurationError(
                    ErrorCode.FCE11,
                    self.input_actions,
                    type(self.input),
                )
        if hasattr(self, "_output") and hasattr(self, "_output_actions"):
            if (
                self.output
                and not isinstance(self.output, TableOutput)
                and self.output_actions
            ):
                # raise error if output actions are provided for non-table output
                raise FunctionConfigurationError(
                    ErrorCode.FCE12,
                    self.output_actions,
                    type(self.output),
                )


def _convert_recursively_to_tableframe(arguments: Any):

    import pandas as pd

    if isinstance(arguments, dict):
        return {k: _convert_recursively_to_tableframe(v) for k, v in arguments.items()}
    elif isinstance(arguments, list):
        return [_convert_recursively_to_tableframe(v) for v in arguments]
    elif isinstance(arguments, tuple):
        return tuple(_convert_recursively_to_tableframe(v) for v in arguments)
    elif isinstance(arguments, td_frame.TableFrame):
        return arguments
    # From here this is only for testing; thus, using always index None does the trick.
    elif isinstance(arguments, pl.DataFrame):
        return td_frame.TableFrame.__build__(
            df=arguments,
            mode="raw",
            idx=None,
            properties=TableFramePropertiesBuilder.empty(),
        )
    elif isinstance(arguments, pl.LazyFrame):
        return td_frame.TableFrame.__build__(
            df=arguments,
            mode="raw",
            idx=None,
            properties=TableFramePropertiesBuilder.empty(),
        )
    elif isinstance(arguments, pd.DataFrame):
        return td_frame.TableFrame.__build__(
            df=pl.DataFrame(arguments),
            mode="raw",
            idx=None,
            properties=TableFramePropertiesBuilder.empty(),
        )
    return arguments


def _clean_recursively_and_convert_to_datatype(
    result,
    datatype: (
        Type[pl.DataFrame]
        | Type[pl.LazyFrame]
        | Type[td_frame.TableFrame]
        | Type[pd.DataFrame]
    ),
) -> Any:

    import pandas as pd

    if isinstance(result, dict):
        return {
            k: _clean_recursively_and_convert_to_datatype(v, datatype)
            for k, v in result.items()
        }
    elif isinstance(result, list):
        return [_clean_recursively_and_convert_to_datatype(v, datatype) for v in result]
    elif isinstance(result, tuple):
        return tuple(
            _clean_recursively_and_convert_to_datatype(v, datatype) for v in result
        )
    elif isinstance(result, td_frame.TableFrame):
        try:
            if datatype == pl.DataFrame:
                # noinspection PyProtectedMember
                return _unwrap_table_frame(result).collect()
            elif datatype == pl.LazyFrame:
                # noinspection PyProtectedMember
                return _unwrap_table_frame(result)
            elif datatype == pd.DataFrame:
                # noinspection PyProtectedMember
                return _unwrap_table_frame(result).collect().to_pandas()
            else:
                return result
        except pl.exceptions.ColumnNotFoundError as e:
            raise ValueError(
                "Missing one of the following system columns: "
                f"'{td_helpers.SYSTEM_COLUMNS}'. "
                "This indicates tampering in the data. "
                "Ensure you are not modifying system columns in your data."
            ) from e
    else:
        return result


def _recursively_obtain_datatype(
    arguments,
) -> (
    Type[pl.DataFrame]
    | Type[pd.DataFrame]
    | Type[pl.LazyFrame]
    | Type[td_frame.TableFrame]
    | None
):

    import pandas as pd

    if isinstance(
        arguments, (pl.DataFrame, pl.LazyFrame, td_frame.TableFrame, pd.DataFrame)
    ):
        return type(arguments)
    elif not arguments:
        return None

    types = []
    if isinstance(arguments, dict):
        types = [_recursively_obtain_datatype(v) for v in arguments.values()]
    elif isinstance(arguments, (list, tuple)):
        types = [_recursively_obtain_datatype(v) for v in arguments]
    if pl.DataFrame in types:
        return pl.DataFrame
    elif pl.LazyFrame in types:
        return pl.LazyFrame
    elif td_frame.TableFrame in types:
        return td_frame.TableFrame
    elif pd.DataFrame in types:
        return pd.DataFrame
    else:
        return None

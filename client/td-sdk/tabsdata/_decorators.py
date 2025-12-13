#
# Copyright 2024 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import List, ParamSpec, TypeVar

from tabsdata._io.inputs.file_inputs import AzureSource, LocalFileSource, S3Source
from tabsdata._io.inputs.sql_inputs import (
    MariaDBSource,
    MySQLSource,
    OracleSource,
    PostgresSource,
)
from tabsdata._io.inputs.table_inputs import TableInput
from tabsdata._io.outputs.file_outputs import (
    AzureDestination,
    LocalFileDestination,
    S3Destination,
)
from tabsdata._io.outputs.sql_outputs import (
    MariaDBDestination,
    MySQLDestination,
    OracleDestination,
    PostgresDestination,
)
from tabsdata._io.outputs.table_outputs import TableOutput
from tabsdata._io.plugin import DestinationPlugin, SourcePlugin
from tabsdata._table_actions._typing import OnTablesSpec
from tabsdata._table_actions.actions import (
    _build_actions_list_or_raise,
)
from tabsdata._tdfunction import TabsdataFunction
from tabsdata.exceptions import DecoratorConfigurationError, ErrorCode

P = ParamSpec("P")
T = TypeVar("T")

ALL_DEPS = "*"

logger = logging.getLogger(__name__)


def transformer(
    input_tables: TableInput | str | List[str],
    output_tables: TableOutput | str | List[str],
    name: str = None,
    trigger_by: str | List[str] | None = ALL_DEPS,
    # on_input_tables: OnTablesSpec = None, TODO: Uncomment this when reverting TD-1089
    on_output_tables: OnTablesSpec = None,
) -> callable:
    """
    Decorator to set the data and destination parameters of a function and
        convert it to a TabsdataFunction.

    Args:
        input_tables (TableInput | str | List[str]): Where to obtain the data that
            will be provided as an input to the function.
        output_tables (TableOutput | str | List[str]): Where to store the
            output of the function.
        name (str, optional): The name with which the function will be registered.
            If not provided, the current function name will be used.
        trigger_by (str | list[str] | None, optional): The trigger that will cause
            the function to execute. It can be a table in the system, a list of
            tables, or None (in which case it must be triggered manually). Defaults to
            all dependencies.
        on_output_tables (OnTablesSpec, optional):
            Actions to perform on output tables after executing the function.
            Can be a single action or a list of them. Defaults to None.
    Returns:
        callable: The function converted to a TabsdataFunction.
    """

    input_tables = _build_transformer_input(input_tables)
    output_tables = _build_transformer_output(output_tables)
    # input_actions = _build_actions_list_or_raise(
    #    on_input_tables,
    #    ErrorCode.DCE7,
    #    DecoratorConfigurationError,
    # ) TODO: Uncomment this when reverting TD-1089
    output_actions = _build_actions_list_or_raise(
        on_output_tables,
        ErrorCode.DCE8,
        DecoratorConfigurationError,
    )

    # Note: this counterintuitive logic is to allow the user to pass None as a trigger
    # meaning "no triggers", while the API takes None as "all dependencies". In the
    # future this logic might be moved to another function.
    if trigger_by == ALL_DEPS:
        trigger_by = None
    elif trigger_by is None:
        trigger_by = []

    def decorator_tabset(func):
        return TabsdataFunction(
            func,
            name,
            input=input_tables,
            output=output_tables,
            trigger_by=trigger_by,
            # input_actions=input_actions, TODO: Uncomment this when reverting TD-1089
            output_actions=output_actions,
        )

    return decorator_tabset


def _build_transformer_input(input_tables: TableInput | str | List[str]) -> TableInput:
    if not isinstance(input_tables, TableInput):
        if not isinstance(input_tables, (str, list)):
            raise DecoratorConfigurationError(ErrorCode.DCE1, type(input_tables))
        if isinstance(input_tables, list) and not all(
            isinstance(table, str) for table in input_tables
        ):
            raise DecoratorConfigurationError(ErrorCode.DCE1, type(input_tables))
        input_tables = TableInput(input_tables)
    return input_tables


def _build_transformer_output(
    output_tables: TableOutput | str | List[str],
) -> TableOutput:
    if not isinstance(output_tables, TableOutput):
        if not isinstance(output_tables, (str, list)):
            raise DecoratorConfigurationError(ErrorCode.DCE2, type(output_tables))
        if isinstance(output_tables, list) and not all(
            isinstance(table, str) for table in output_tables
        ):
            raise DecoratorConfigurationError(ErrorCode.DCE2, type(output_tables))
        output_tables = TableOutput(output_tables)
    return output_tables


def publisher(
    source: (
        AzureSource
        | LocalFileSource
        | MariaDBSource
        | MySQLSource
        | OracleSource
        | PostgresSource
        | S3Source
        | SourcePlugin
    ),
    tables: TableOutput | str | List[str],
    name: str = None,
    trigger_by: str | List[str] | None = None,
    on_tables: OnTablesSpec = None,
) -> callable:
    """
    Decorator to set the data and destination parameters of a function and
        convert it to a TabsdataFunction.

    Args:
        source (AzureSource | LocalFileSource | MariaDBSource | MySQLSource |
            OracleSource | PostgresSource | S3Source | SourcePlugin): Where to obtain
            the data that will be provided as an input to the function.
        tables (TableOutput | str | List[str]): Where to store the
            output of the function.
        name (str, optional): The name with which the function will be registered.
            If not provided, the current function name will be used.
        trigger_by (str | list[str] | None, optional): The trigger that will cause
            the function to execute. It can be a table in the system, a list of
            tables, or None (in which case it must be triggered manually).
        on_tables (OnTablesSpec, optional): Actions
            to perform on output tables after executing the function. Can be a
            single action or a list of them. Defaults to None.
    Returns:
        callable: The function converted to a TabsdataFunction.
    """
    if not isinstance(source, SourcePlugin) or isinstance(source, TableInput):
        raise DecoratorConfigurationError(ErrorCode.DCE3, type(source))

    if not isinstance(tables, TableOutput):
        if not isinstance(tables, (str, list)):
            raise DecoratorConfigurationError(ErrorCode.DCE4, type(tables))
        if isinstance(tables, list) and not all(
            isinstance(table, str) for table in tables
        ):
            raise DecoratorConfigurationError(ErrorCode.DCE4, type(tables))
        tables = TableOutput(tables)

    output_actions = _build_actions_list_or_raise(
        on_tables,
        ErrorCode.DCE9,
        DecoratorConfigurationError,
    )

    def decorator_tabset(func):
        return TabsdataFunction(
            func,
            name,
            input=source,
            output=tables,
            trigger_by=trigger_by,
            output_actions=output_actions,
        )

    return decorator_tabset


def subscriber(
    tables: TableInput | str | List[str],
    destination: (
        AzureDestination
        | LocalFileDestination
        | MariaDBDestination
        | MySQLDestination
        | OracleDestination
        | PostgresDestination
        | S3Destination
        | DestinationPlugin
    ),
    name: str = None,
    trigger_by: str | List[str] | None = ALL_DEPS,
    # on_tables: OnTablesSpec = None, TODO: Uncomment this when reverting TD-1089
) -> callable:
    """
    Decorator to set the data and destination parameters of a function and
        convert it to a TabsdataFunction.

    Args:
        tables (TableInput | str | List[str]): Where to obtain the data that will be
            provided as an input to the function.
        destination (AzureDestination | LocalFileDestination | MariaDBDestination |
            MySQLDestination | OracleDestination | PostgresDestination | S3Destination
            | DestinationPlugin): Where to store the output of the function.
        name (str, optional): The name with which the function will be registered.
            If not provided, the current function name will be used.
        trigger_by (str | list[str] | None, optional): The trigger that will cause
            the function to execute. It can be a table in the system, a list of
            tables, or None (in which case it must be triggered manually). Defaults to
            all dependencies.
    Returns:
        callable: The function converted to a TabsdataFunction.
    """

    if not isinstance(tables, TableInput):
        if not isinstance(tables, (str, list)):
            raise DecoratorConfigurationError(ErrorCode.DCE5, type(tables))
        if isinstance(tables, list) and not all(
            isinstance(table, str) for table in tables
        ):
            raise DecoratorConfigurationError(ErrorCode.DCE5, type(tables))
        tables = TableInput(tables)

    if not isinstance(destination, DestinationPlugin) or isinstance(
        destination, TableOutput
    ):
        raise DecoratorConfigurationError(ErrorCode.DCE6, type(destination))

    # input_actions = _build_actions_list_or_raise(
    #    on_tables, ErrorCode.DCE10, DecoratorConfigurationError
    # ) TODO: Uncomment this when reverting TD-1089

    # Note: this counterintuitive logic is to allow the user to pass None as a trigger
    # meaning "no triggers", while the API takes None as "all dependencies". In the
    # future this logic might be moved to another function.
    if trigger_by == ALL_DEPS:
        trigger_by = None
    elif trigger_by is None:
        trigger_by = []

    def decorator_tabset(func):
        return TabsdataFunction(
            func,
            name,
            input=tables,
            output=destination,
            trigger_by=trigger_by,
            # input_actions=input_actions, TODO: Uncomment this when reverting TD-1089
        )

    return decorator_tabset

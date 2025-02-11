#
# Copyright 2024 Tabs Data Inc.
#

import logging
from typing import List, ParamSpec, TypeVar

from tabsdata.exceptions import DecoratorConfigurationError, ErrorCode
from tabsdata.plugin import DestinationPlugin, SourcePlugin
from tabsdata.tabsdatafunction import (
    AzureDestination,
    AzureSource,
    LocalFileDestination,
    LocalFileSource,
    MariaDBDestination,
    MariaDBSource,
    MySQLDestination,
    MySQLSource,
    OracleDestination,
    OracleSource,
    PostgresDestination,
    PostgresSource,
    S3Destination,
    S3Source,
    TableInput,
    TableOutput,
    TabsdataFunction,
)

P = ParamSpec("P")
T = TypeVar("T")

ALL_DEPS = "*"

logger = logging.getLogger(__name__)


def transformer(
    input_tables: TableInput | str | List[str],
    output_tables: TableOutput | str | List[str],
    name: str = None,
    trigger_by: str | List[str] | None = ALL_DEPS,
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

    Returns:
        callable: The function converted to a TabsdataFunction.
    """

    if not isinstance(input_tables, TableInput):
        if not isinstance(input_tables, (str, list)):
            raise DecoratorConfigurationError(ErrorCode.DCE1, type(input_tables))
        if isinstance(input_tables, list) and not all(
            isinstance(table, str) for table in input_tables
        ):
            raise DecoratorConfigurationError(ErrorCode.DCE1, type(input_tables))
        input_tables = TableInput(input_tables)

    if not isinstance(output_tables, TableOutput):
        if not isinstance(output_tables, (str, list)):
            raise DecoratorConfigurationError(ErrorCode.DCE2, type(output_tables))
        if isinstance(output_tables, list) and not all(
            isinstance(table, str) for table in output_tables
        ):
            raise DecoratorConfigurationError(ErrorCode.DCE2, type(output_tables))
        output_tables = TableOutput(output_tables)

    # Note: this counterintuitive logic is to allow the user to pass None as a trigger
    # meaning "no triggers", while the API takes None as "all dependencies". In the
    # future this logic might be moved to another function.
    if trigger_by == ALL_DEPS:
        trigger_by = None
    elif trigger_by is None:
        trigger_by = []

    def decorator_tabset(func):
        return TabsdataFunction(
            func, name, input=input_tables, output=output_tables, trigger_by=trigger_by
        )

    return decorator_tabset


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

    Returns:
        callable: The function converted to a TabsdataFunction.
    """
    if not isinstance(
        source,
        (
            AzureSource,
            LocalFileSource,
            MariaDBSource,
            MySQLSource,
            OracleSource,
            PostgresSource,
            S3Source,
            SourcePlugin,
        ),
    ):
        raise DecoratorConfigurationError(ErrorCode.DCE3, type(source))

    if not isinstance(tables, TableOutput):
        if not isinstance(tables, (str, list)):
            raise DecoratorConfigurationError(ErrorCode.DCE4, type(tables))
        if isinstance(tables, list) and not all(
            isinstance(table, str) for table in tables
        ):
            raise DecoratorConfigurationError(ErrorCode.DCE4, type(tables))
        tables = TableOutput(tables)

    def decorator_tabset(func):
        return TabsdataFunction(
            func, name, input=source, output=tables, trigger_by=trigger_by
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

    if not isinstance(
        destination,
        (
            AzureDestination,
            LocalFileDestination,
            MariaDBDestination,
            MySQLDestination,
            OracleDestination,
            PostgresDestination,
            S3Destination,
            DestinationPlugin,
        ),
    ):
        raise DecoratorConfigurationError(ErrorCode.DCE6, type(destination))

    # Note: this counterintuitive logic is to allow the user to pass None as a trigger
    # meaning "no triggers", while the API takes None as "all dependencies". In the
    # future this logic might be moved to another function.
    if trigger_by == ALL_DEPS:
        trigger_by = None
    elif trigger_by is None:
        trigger_by = []

    def decorator_tabset(func):
        return TabsdataFunction(
            func, name, input=tables, output=destination, trigger_by=trigger_by
        )

    return decorator_tabset

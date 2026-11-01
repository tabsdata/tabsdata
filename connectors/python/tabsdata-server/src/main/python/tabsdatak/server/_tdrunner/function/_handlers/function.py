#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import inspect
import typing
from typing import Any, Callable

from tabsdatak._api import _is_array_slot, _is_single_slot, has_transaction_ctx_param
from tabsdatak._connectors import get_dest
from tabsdatak._info import (
    PublisherInfo,
    StreamPublisherInfo,
    SubscriberInfo,
    _unwrap_info,
)
from tabsdatak.api import OnTablesActionSpec
from tabsdatak.server._tdrunner.function._commons.solver.secrets import (
    StorageSecrets,
)
from tabsdatak.server._tdrunner.function._commons.symbols.function import (
    FunctionInfo,
)
from tabsdatak.spi import TableMode
from tabsdatak.tableframe import TableFrame

SUBSCRIBER_OUTPUT_PREFIX = "out_"

Output = TableFrame | None


class UserFunctionError(Exception):
    pass


class TypeHintsError(UserFunctionError):
    detail: str

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class InputModeError(UserFunctionError):
    parameter: str

    def __init__(self, parameter: str) -> None:
        super().__init__(f"Parameter {parameter!r} has no input mode")
        self.parameter = parameter


class UnknownDestinationError(UserFunctionError):
    destination_type: str
    detail: str

    def __init__(self, destination_type: str, detail: str) -> None:
        super().__init__(detail)
        self.destination_type = destination_type
        self.detail = detail


class FunctionFailedError(UserFunctionError):
    detail: str

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class NotATupleError(UserFunctionError):
    actual_type: str

    def __init__(self, actual_type: str) -> None:
        super().__init__(f"The result is a {actual_type}, not a tuple")
        self.actual_type = actual_type


class ResultCountError(UserFunctionError):
    returned: int
    tables: list[str]

    def __init__(self, returned: int, tables: list[str]) -> None:
        super().__init__(f"{returned} value(s) returned for {len(tables)} table(s)")
        self.returned = returned
        self.tables = tables


class ResultTypeError(UserFunctionError):
    slot: int
    table: str
    actual_type: str

    def __init__(self, slot: int, table: str, actual_type: str) -> None:
        super().__init__(f"Output {slot} ({table!r}) is a {actual_type}")
        self.slot = slot
        self.table = table
        self.actual_type = actual_type


class UserFunction:
    definition: FunctionInfo
    function: Callable

    def __init__(self, definition: FunctionInfo) -> None:
        self.definition = definition
        self.function = _unwrap_info(definition.func)

    @property
    def publisher(self) -> bool:
        return isinstance(self.definition, (PublisherInfo, StreamPublisherInfo))

    @property
    def subscriber(self) -> bool:
        return isinstance(self.definition, SubscriberInfo)

    @property
    def on_tables(self) -> OnTablesActionSpec | None:
        if isinstance(self.definition, SubscriberInfo):
            return None
        return self.definition.on_tables

    @property
    def takes_context(self) -> bool:
        return has_transaction_ctx_param(self.function)

    def table_modes(self) -> list[TableMode]:
        try:
            hints = typing.get_type_hints(self.function)
        except Exception as error:
            raise TypeHintsError(str(error)) from error
        parameters = list(inspect.signature(self.function).parameters.values())
        if self.takes_context and parameters:
            parameters = parameters[:-1]
        modes: list[TableMode] = []
        for parameter in parameters:
            hint = hints.get(parameter.name)
            if hint is not None and _is_single_slot(hint):
                modes.append(TableMode.SINGLE)
            elif hint is not None and _is_array_slot(hint):
                modes.append(TableMode.ARRAY)
            else:
                raise InputModeError(parameter.name)
        return modes

    def output_tables(self) -> list[str]:
        if isinstance(self.definition, SubscriberInfo):
            destination_type = type(self.definition.destination)
            try:
                destination = get_dest(destination_type)
            except KeyError as error:
                raise UnknownDestinationError(
                    destination_type.__name__,
                    str(error),
                ) from error
            count = destination.cardinality(self.definition.destination)
            return [
                f"{SUBSCRIBER_OUTPUT_PREFIX}{position}" for position in range(count)
            ]
        return list(self.definition.output_tables)

    def call(self, arguments: list[Any], tables: list[str]) -> list[Output]:
        try:
            with StorageSecrets().managed():
                result = self.function(*arguments)
        except Exception as error:
            raise FunctionFailedError(str(error)) from error
        if not isinstance(result, tuple):
            if len(tables) != 1:
                raise NotATupleError(type(result).__name__)
            result = (result,)
        if len(result) != len(tables):
            raise ResultCountError(len(result), list(tables))
        for position, value in enumerate(result):
            if value is not None and not isinstance(value, TableFrame):
                raise ResultTypeError(
                    position,
                    tables[position],
                    type(value).__name__,
                )
        return list(result)

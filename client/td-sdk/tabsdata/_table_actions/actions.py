#
# Copyright 2025 Tabs Data Inc.
#

from typing import TypeAlias, Union

from tabsdata.exceptions import ErrorCode, TabsDataException


class OnTablesAction:
    """
    Base class for classes that perform declarative tasks on the `TableFrames` tables
    consumed or produced by publishers, subscribers and transformers.
    """


OnTablesActionSpec: TypeAlias = Union[OnTablesAction | list[OnTablesAction] | None]


def _build_actions_list_or_raise(
    actions: OnTablesActionSpec,
    error_code: ErrorCode,
    error_type: type[TabsDataException],
) -> list[OnTablesActionSpec]:
    match actions:
        case OnTablesAction():
            return [actions]
        case list() if all(isinstance(action, OnTablesAction) for action in actions):
            return actions
        case None:
            return []
        case _:
            raise error_type(error_code, actions, type(actions))

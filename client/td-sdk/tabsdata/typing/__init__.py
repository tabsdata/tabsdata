#
# Copyright 2025 Tabs Data Inc.
#

from typing import TypeAlias, Union

from tabsdata._crontab import Crontab
from tabsdata._io.outputs.shared_enums import (
    IfTableExistStrategySpec,
    SchemaStrategySpec,
)
from tabsdata._io.watchdog import WatchDog
from tabsdata._table_actions._typing import OnTablesSpec
from tabsdata._table_actions.actions import OnTablesAction

TableSpec: TypeAlias = Union[str, list[str], None]
TransformerTriggerBySpec: TypeAlias = Union[TableSpec, Crontab]
PublisherTriggerBySpec: TypeAlias = Union[TableSpec, Crontab, WatchDog]
SubscriberTriggerBySpec: TypeAlias = Union[TableSpec, Crontab]

__all__ = [
    # from tabsdata._io.outputs.shared_enums
    "IfTableExistStrategySpec",
    "SchemaStrategySpec",
    # from tabsdata._table_actions.actions
    "OnTablesAction",
    # from tabsdata._table_actions._typing
    "OnTablesSpec",
    # from tabsdata.typing.__init__
    "PublisherTriggerBySpec",
    "SubscriberTriggerBySpec",
    "TableSpec",
    "TransformerTriggerBySpec",
]

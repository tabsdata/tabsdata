#
# Copyright 2025 Tabs Data Inc.
#

from typing import TypeAlias, Union

from tabsdata._crontab import Crontab
from tabsdata._io.watchdog import WatchDog

TableName: TypeAlias = str
TableSpec: TypeAlias = Union[TableName, list[TableName], None]
TransformerTriggerBySpec: TypeAlias = Union[TableSpec, Crontab]
PublisherTriggerBySpec: TypeAlias = Union[TableSpec, Crontab, WatchDog]
SubscriberTriggerBySpec: TypeAlias = Union[TableSpec, Crontab]

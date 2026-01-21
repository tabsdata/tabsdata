#
# Copyright 2025 Tabs Data Inc.
#

from typing import TypeAlias, Union

from tabsdata._crontab import Crontab
from tabsdata._io.stage_trigger import StageTrigger

TableName: TypeAlias = str
TableSpec: TypeAlias = Union[TableName, list[TableName], None]
TransformerTriggerBySpec: TypeAlias = Union[TableSpec, Crontab]
PublisherTriggerBySpec: TypeAlias = Union[TableSpec, Crontab, StageTrigger]
SubscriberTriggerBySpec: TypeAlias = Union[TableSpec, Crontab]

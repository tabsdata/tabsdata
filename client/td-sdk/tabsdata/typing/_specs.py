#
# Copyright 2025 Tabs Data Inc.
#

from typing import TypeAlias, Union

from tabsdata._cron_trigger import CronTrigger
from tabsdata._io.stage_trigger import StageTrigger

TableName: TypeAlias = str
TableSpec: TypeAlias = Union[TableName, list[TableName], None]
TransformerTriggerBySpec: TypeAlias = Union[TableSpec, CronTrigger]
PublisherTriggerBySpec: TypeAlias = Union[TableSpec, CronTrigger, StageTrigger]
SubscriberTriggerBySpec: TypeAlias = Union[TableSpec, CronTrigger]

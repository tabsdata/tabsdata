#
# Copyright 2025 Tabs Data Inc.
#

import tabsdata as td
from tabsdata import SourcePlugin
from tabsdata._io.stage_trigger import (
    StageTrigger,
    StageTriggerContext,
    StageTriggerStatus,
)


class MyStageTrigger(StageTrigger):

    def run(self, ctx: StageTriggerContext) -> StageTriggerStatus:
        pass

    def _to_dict(self) -> dict:
        return super()._to_dict()


class MyStageTriggerPlugin(SourcePlugin):

    def __init__(self):
        pass

    @property
    def allows_listener(self) -> bool:
        return True


def testing_tabsdata_function():

    @td.publisher(
        source=MyStageTriggerPlugin(),
        tables=["output1", "output2"],
        name="test_input_stagetrigger",
        trigger_by=MyStageTrigger(),
    )
    def input_stagetrigger(
        headers: td.TableFrame, items: td.TableFrame
    ) -> tuple[td.TableFrame, td.TableFrame]:
        return headers, items

    return input_stagetrigger

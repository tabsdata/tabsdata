import tabsdata as td
from tabsdata._io.stage_trigger import (
    StageTrigger,
    StageTriggerContext,
    StageTriggerStatus,
)


class MyStageTrigger(StageTrigger):

    def run(self, ctx: StageTriggerContext) -> StageTriggerStatus:
        ctx.trigger()
        ctx.stage([])
        ctx.trigger_monitor()

    def _to_dict(self) -> dict:
        d = super()._to_dict()
        d["foo"] = "bar"
        return d


@td.publisher(trigger_by=MyStageTrigger(), source=td.Stage(), tables="output")
def my_testing_fn(*args):
    return None

import os
import time
from pathlib import Path

import polars as pl

import tabsdata as td
from tabsdata._io.stage_trigger import (
    StageTrigger,
    StageTriggerContext,
    StageTriggerStatus,
)


class MyStageTrigger(StageTrigger):

    def run(self, ctx: StageTriggerContext) -> StageTriggerStatus:
        working_dir = ctx.working_dir
        idx = 0
        df = pl.DataFrame(
            {
                "id": range(1, 7),
                "name": ["Arvind", "Tucu", "Dimas", "Joaquin", "Jennifer", "Aleix"],
            }
        )
        file = Path(os.path.join(working_dir, f"dummy_file_{idx}.parquet"))
        df.write_parquet(file)
        # ctx.stage(file)
        while True:
            idx += 1
            ctx.trigger_monitor()
            time.sleep(10)
            ctx.trigger()
        return StageTriggerStatus.SUCCESS


@td.publisher(trigger_by=MyStageTrigger(), source=td.Stage(), tables="output")
def my_testing_fn(dataframes):
    return None

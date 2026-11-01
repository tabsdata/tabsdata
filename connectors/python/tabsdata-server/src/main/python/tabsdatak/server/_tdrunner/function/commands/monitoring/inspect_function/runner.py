#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os

from tabsdatak.server._tdrunner.function._commons.monitor.sysinfo import SystemReport
from tabsdatak.server._tdrunner.function._commons.run.command import Command
from tabsdatak.server._tdrunner.function.commands.monitoring.inspect_function.options import (  # noqa: E501
    InspectFunctionArguments,
)

UNSET = "<unset>"


class InspectFunction(Command[InspectFunctionArguments]):
    worker: str
    run_id: str
    decorator: str

    def __init__(self, arguments: InspectFunctionArguments) -> None:
        super().__init__(arguments)
        self.worker = os.environ.get("TD_WORKER", UNSET)
        self.run_id = os.environ.get("TD_RUNID", UNSET)
        self.decorator = os.environ.get("TD_DECORATOR", UNSET)

    def run(self) -> int:
        print("--- inspect-function (start) ---", flush=True)
        print(f"worker={self.worker}", flush=True)
        print(f"runid={self.run_id}", flush=True)
        print(f"decorator={self.decorator}", flush=True)
        SystemReport().show()
        print("--- inspect-function (end) ---", flush=True)
        return 0

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from tabsdatak.server._tdrunner.function._commons.document.journal import Journal
from tabsdatak.server._tdrunner.function._commons.run.command import Command
from tabsdatak.server._tdrunner.function.commands.lifecycle.begin_execution.error import (  # noqa: E501
    BeginExecutionErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.lifecycle.begin_execution.options import (  # noqa: E501
    BeginExecutionArguments,
)

logger = logging.getLogger(__name__)


class BeginExecution(Command[BeginExecutionArguments]):
    journal_file: Path
    start: int

    def __init__(self, arguments: BeginExecutionArguments) -> None:
        super().__init__(arguments)
        self.journal_file = arguments.journal_file

    def run(self) -> int:
        logger.info("Beginning the function run")
        self.start = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        self.record()
        self.responder.running()
        logger.info(f"Function run began at {self.start}")
        return 0

    def record(self) -> None:
        logger.debug(f"Writing start {self.start} to the journal {self.journal_file}")
        try:
            Journal(start=self.start).save(self.journal_file)
        except OSError as error:
            raise BeginExecutionErrorCode.BE_1.exception(
                cause=error,
                path=self.journal_file,
                detail=str(error),
            )

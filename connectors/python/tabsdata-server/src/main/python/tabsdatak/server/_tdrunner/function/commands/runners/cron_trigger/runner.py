#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import uuid

from tabsdatak.server._tdrunner.function._commons.http.request import (
    HttpRequestError,
)
from tabsdatak.server._tdrunner.function._commons.run.command import Command
from tabsdatak.server._tdrunner.function._handlers.trigger import (
    ExecutionRequest,
    ExecutionTrigger,
    TriggerType,
    preview,
)
from tabsdatak.server._tdrunner.function.commands.runners.cron_trigger.error import (
    RunCronTriggerErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.runners.cron_trigger.options import (  # noqa: E501
    RunCronTriggerArguments,
)

logger = logging.getLogger(__name__)

EXTERNAL_UID_PREFIX = "cron-"


class CronTrigger(Command[RunCronTriggerArguments]):
    trigger: ExecutionTrigger
    execution: ExecutionRequest

    def __init__(self, arguments: RunCronTriggerArguments) -> None:
        super().__init__(arguments)
        self.trigger = ExecutionTrigger.resolve(
            arguments.trigger_url,
            arguments.trigger_method,
            arguments.trigger_authorization,
        )
        self.execution = ExecutionRequest(
            trigger_type=TriggerType.CRON,
            external_uid=f"{EXTERNAL_UID_PREFIX}{uuid.uuid4()}",
            skip_if_running=False,
        )

    def run(self) -> int:
        self.fire()
        return 0

    def fire(self) -> None:
        url = self.trigger.url
        logger.info(
            f"Firing {self.trigger.method} {url} ({self.execution.external_uid})"
        )
        try:
            response = self.trigger.post_with_retry(self.execution)
        except HttpRequestError as error:
            raise RunCronTriggerErrorCode.CT_1.exception(
                cause=error,
                url=url,
                detail=str(error),
            )
        if not response.ok:
            raise RunCronTriggerErrorCode.CT_2.exception(
                url=url,
                status=response.status,
                body=repr(preview(response.body)),
            )
        logger.info(f"Execution scheduled (status {response.status})")

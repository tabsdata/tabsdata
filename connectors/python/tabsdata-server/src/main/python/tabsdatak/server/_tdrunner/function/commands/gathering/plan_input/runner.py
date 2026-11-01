#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from pathlib import Path

from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Request,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputManifest,
)
from tabsdatak.server._tdrunner.function._commons.run.command import Command
from tabsdatak.server._tdrunner.function.commands.gathering.plan_input.error import (
    PlanInputErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.gathering.plan_input.options import (
    PlanInputArguments,
)

logger = logging.getLogger(__name__)


class PlanInput(Command[PlanInputArguments]):
    request_file: Path
    inputs_manifest_file: Path
    request: Request
    manifest: InputManifest

    def __init__(self, arguments: PlanInputArguments) -> None:
        super().__init__(arguments)
        self.request_file = arguments.request
        self.inputs_manifest_file = arguments.inputs_manifest

    def run(self) -> int:
        logger.info("Preparing the inputs manifest")
        self.request = Request.load(
            self.request_file,
            PlanInputErrorCode.PI_1,
        )
        logger.debug(
            f"Request has {len(self.request.input)} input slot(s): {self.request.input}"
        )
        self.manifest = InputManifest.from_request(self.request)
        self.write()
        logger.info("Inputs manifest prepared")
        return 0

    def write(self) -> None:
        try:
            self.manifest.write(self.inputs_manifest_file)
        except OSError as error:
            raise PlanInputErrorCode.PI_4.exception(
                cause=error,
                path=self.inputs_manifest_file,
                detail=str(error),
            )
        logger.debug(
            f"Wrote the inputs manifest with {len(self.manifest.inputs)} "
            f"entry(ies) to {self.inputs_manifest_file}: {self.manifest.inputs}"
        )

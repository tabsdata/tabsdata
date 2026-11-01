#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from tabsdatak.server._tdrunner.function._commons.document.journal import Journal
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Request,
)
from tabsdatak.server._tdrunner.function._commons.document.message.response import (
    Response,
)
from tabsdatak.server._tdrunner.function._commons.manifests.manifest import (
    ManifestError,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputManifest,
)
from tabsdatak.server._tdrunner.function._commons.run.command import Command
from tabsdatak.server._tdrunner.function.commands.lifecycle.end_execution.error import (
    EndExecutionErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.lifecycle.end_execution.options import (  # noqa: E501
    EndExecutionArguments,
)

logger = logging.getLogger(__name__)


class EndExecution(Command[EndExecutionArguments]):
    request_file: Path
    outputs_manifest_file: Path
    response_file: Path
    journal_file: Path
    request: Request
    manifest: OutputManifest
    response: Response

    def __init__(self, arguments: EndExecutionArguments) -> None:
        super().__init__(arguments)
        self.request_file = arguments.request
        self.outputs_manifest_file = arguments.outputs_manifest
        self.response_file = arguments.response
        self.journal_file = arguments.journal_file

    def run(self) -> int:
        logger.info("Reporting the function-run response")
        self.load()
        self.build()
        self.write_response()
        self.record_end()
        self.responder.done(output=self.response.function_output().document())
        logger.info("Reported the completion to the apiserver")
        return 0

    def load(self) -> None:
        self.request = Request.load(self.request_file, EndExecutionErrorCode.EE_1)
        try:
            self.manifest = OutputManifest.load(self.outputs_manifest_file)
        except ManifestError as error:
            raise EndExecutionErrorCode.EE_2.exception(
                cause=error,
                path=self.outputs_manifest_file,
                detail=str(error),
            )
        logger.debug(
            f"Outputs manifest lists {len(self.manifest.outputs)} output(s): "
            f"{self.manifest.outputs}; the request declares "
            f"{len(self.request.output)} output table(s): {self.request.output} "
            f"and {len(self.request.system_output)} system output table(s): "
            f"{self.request.system_output}"
        )

    def build(self) -> None:
        self.response = Response.build(
            self.request,
            self.manifest,
            EndExecutionErrorCode.EE_3,
        )
        with_data = sum(1 for entry in self.response.output if entry.info is not None)
        logger.info(
            f"Built the response: {with_data} table(s) with data, "
            f"{len(self.response.output) - with_data} without"
        )

    def write_response(self) -> None:
        try:
            self.response.write(self.response_file)
        except OSError as error:
            raise EndExecutionErrorCode.EE_4.exception(
                cause=error,
                path=self.response_file,
                detail=str(error),
            )
        logger.debug(f"Wrote the response to {self.response_file}")

    def record_end(self) -> None:
        journal = Journal.load(self.journal_file, strict=True)
        journal.end = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        try:
            journal.save(self.journal_file)
        except OSError as error:
            raise EndExecutionErrorCode.EE_4.exception(
                cause=error,
                path=self.journal_file,
                detail=str(error),
            )
        logger.info(f"Function run ended at {journal.end}")

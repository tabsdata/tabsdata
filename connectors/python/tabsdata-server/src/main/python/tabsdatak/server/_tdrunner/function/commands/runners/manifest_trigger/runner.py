#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import signal
import time
from enum import Enum, auto
from pathlib import Path
from types import FrameType
from typing import assert_never
from urllib.parse import urlparse

from tabsdatak.common._storage.transfer import TransferError
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    AnyRequest,
)
from tabsdatak.server._tdrunner.function._commons.http.request import (
    HttpRequestError,
    HttpResponse,
)
from tabsdatak.server._tdrunner.function._commons.manifests.stage import (
    InvalidManifestError,
    StageManifest,
)
from tabsdatak.server._tdrunner.function._commons.run.command import Command
from tabsdatak.server._tdrunner.function._commons.run.shutdown import (
    request_shutdown,
)
from tabsdatak.server._tdrunner.function._commons.storage.stage import (
    FILES_FOLDER,
    MANIFESTS_FOLDER,
    PATH_SEPARATOR,
    Stage,
)
from tabsdatak.server._tdrunner.function._handlers.sweeper import Sweeper
from tabsdatak.server._tdrunner.function._handlers.trigger import (
    ExecutionRequest,
    ExecutionTrigger,
    MalformedAnswerError,
    TriggerOutcome,
    TriggerType,
    UnknownOutcomeError,
    preview,
)
from tabsdatak.server._tdrunner.function.commands.runners.manifest_trigger.error import (  # noqa: E501
    RunManifestTriggerErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.runners.manifest_trigger.options import (  # noqa: E501
    RunManifestTriggerArguments,
)

logger = logging.getLogger(__name__)

SWEEP_ABANDONED_BATCHES = False


class ManifestOutcome(Enum):
    CONSUMED = auto()
    SKIPPED = auto()
    TRIGGERED = auto()
    HELD = auto()
    TRANSIENT = auto()


class ManifestTrigger(Command[RunManifestTriggerArguments]):
    request_file: Path
    trigger: ExecutionTrigger
    back_off_seconds: int
    offset: str | None
    newest: StageManifest | None
    active: bool
    request: AnyRequest
    stage: Stage
    sweeper: Sweeper

    def __init__(self, arguments: RunManifestTriggerArguments) -> None:
        super().__init__(arguments)
        self.request_file = arguments.request
        self.trigger = ExecutionTrigger.resolve(
            arguments.trigger_url,
            arguments.trigger_method,
            arguments.trigger_authorization,
        )
        self.back_off_seconds = arguments.back_off_seconds
        self.offset = None
        self.newest = None
        self.active = True

    def run(self) -> int:
        logger.info(
            f"Starting the manifest trigger, backing off {self.back_off_seconds}s"
        )
        self.load()
        self.listen_for_shutdown()
        while self.active:
            self.trigger_pending()
            if not self.active:
                break
            self.sweep()
        logger.info(f"Stopping with {len(self.sweeper)} item(s) left to delete")
        return 0

    def sweep(self) -> None:
        if SWEEP_ABANDONED_BATCHES:
            self.sweep_abandoned()
        deadline = time.monotonic() + self.back_off_seconds
        self.sweeper.sweep_until(deadline, self.is_active)

    def load(self) -> None:
        self.request = AnyRequest.load(
            self.request_file,
            RunManifestTriggerErrorCode.MT_1,
        )
        function_stage = self.request.info.function_stage
        try:
            self.stage = Stage(function_stage)
        except TransferError as error:
            raise RunManifestTriggerErrorCode.MT_2.exception(
                cause=error,
                uri=function_stage.uri,
                scheme=urlparse(function_stage.uri).scheme,
            )
        self.sweeper = Sweeper(self.stage)
        logger.info(
            f"Scanning {function_stage.uri}; triggering "
            f"{self.trigger.method} {self.trigger.url}"
        )

    def listen_for_shutdown(self) -> None:
        try:
            signal.signal(signal.SIGTERM, self.shutdown)
            signal.signal(signal.SIGINT, self.shutdown)
        except ValueError:
            logger.debug("Not on the main thread; the shutdown signals are not handled")

    def shutdown(self, signal_number: int, frame: FrameType | None) -> None:
        logger.info(f"Received signal {signal_number}; stopping at the next check")
        request_shutdown()
        self.active = False

    def is_active(self) -> bool:
        return self.active

    def trigger_pending(self) -> None:
        while self.active:
            path = self.stage.next(MANIFESTS_FOLDER, self.offset)
            if path is None:
                return
            outcome = self.trigger_manifest(path)
            match outcome:
                case ManifestOutcome.CONSUMED | ManifestOutcome.SKIPPED:
                    self.offset = path
                case (
                    ManifestOutcome.TRIGGERED
                    | ManifestOutcome.HELD
                    | ManifestOutcome.TRANSIENT
                ):
                    return
                case _:
                    assert_never(outcome)

    def trigger_manifest(self, path: str) -> ManifestOutcome:
        try:
            manifest = StageManifest.parse(path)
        except InvalidManifestError as error:
            logger.warning(f"Skipping the malformed manifest {path}: {error}")
            return ManifestOutcome.SKIPPED
        execution = ExecutionRequest(
            trigger_type=TriggerType.STAGE,
            external_uid=manifest.batch,
            skip_if_running=True,
        )
        try:
            response = self.trigger.post(execution)
        except HttpRequestError as error:
            logger.warning(f"Cannot trigger {manifest.filename}; keeping it: {error}")
            return ManifestOutcome.TRANSIENT
        if not response.ok:
            logger.warning(
                f"Trigger of {manifest.filename} answered {response.status}; "
                f"keeping it for a retry. Body: {preview(response.body)!r}"
            )
            return ManifestOutcome.TRANSIENT
        outcome = self.outcome(response)
        match outcome:
            case TriggerOutcome.SCHEDULED:
                logger.info(
                    f"Triggered {manifest.filename}; deleting it once its "
                    f"execution commits"
                )
                return ManifestOutcome.TRIGGERED
            case TriggerOutcome.CONSUMED:
                logger.info(f"{manifest.filename} consumed; queuing it for deletion")
                self.sweeper.enqueue(path)
                return ManifestOutcome.CONSUMED
            case TriggerOutcome.HELD:
                logger.info(
                    f"{manifest.filename} is held by an execution that has not "
                    f"committed; retrying on the next poll"
                )
                return ManifestOutcome.HELD
            case TriggerOutcome.BUSY:
                logger.info(
                    f"{manifest.filename}: the function is busy; retrying on the "
                    f"next poll"
                )
                return ManifestOutcome.TRANSIENT
            case _:
                assert_never(outcome)

    def sweep_abandoned(self) -> None:
        try:
            manifests = self.manifests()
            folders = self.stage.folders(FILES_FOLDER)
        except Exception as error:
            logger.warning(f"Cannot list the stage for abandoned batches: {error}")
            return
        for manifest in manifests:
            if self.newest is None or manifest.timestamp > self.newest.timestamp:
                self.newest = manifest
        if self.newest is None:
            return
        published = {manifest.batch for manifest in manifests}
        for folder in folders:
            try:
                batch = StageManifest.of_batch(folder.rpartition(PATH_SEPARATOR)[2])
            except InvalidManifestError:
                continue
            if batch.batch in published or batch.timestamp >= self.newest.timestamp:
                continue
            if batch.folder not in self.sweeper.folders:
                logger.info(f"Sweeping the abandoned batch {batch.batch}")
            self.sweeper.enqueue_folder(batch.folder)

    def manifests(self) -> list[StageManifest]:
        manifests = []
        for name in self.stage.names(MANIFESTS_FOLDER):
            try:
                manifests.append(StageManifest.parse(name))
            except InvalidManifestError:
                continue
        return manifests

    def outcome(self, response: HttpResponse) -> TriggerOutcome:
        try:
            return TriggerOutcome.parse(response.body)
        except UnknownOutcomeError as error:
            raise RunManifestTriggerErrorCode.MT_3.exception(
                cause=error,
                outcome=error.outcome,
            )
        except MalformedAnswerError as error:
            raise RunManifestTriggerErrorCode.MT_4.exception(
                cause=error,
                body=error.body,
            )

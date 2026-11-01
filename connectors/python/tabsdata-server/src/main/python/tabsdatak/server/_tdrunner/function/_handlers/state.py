#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tabsdatak.server._tdrunner.function._commons.context.request import (
    FromRequestCtx,
)
from tabsdatak.server._tdrunner.function._commons.document.state.function import (
    FunctionState,
    InvalidFunctionStateError,
)
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile

if TYPE_CHECKING:
    from tabsdatak.server._tdrunner.function._commons.document.message.request import (
        Location,
        Request,
    )

logger = logging.getLogger(__name__)


STATE_FILE = "state.parquet"
TEMPORARY_SUFFIX = ".tmp"


class StateError(ValueError):
    pass


class StateReadError(StateError):
    uri: str
    detail: str

    def __init__(self, uri: str, detail: str) -> None:
        super().__init__(f"Cannot read the state {uri}: {detail}")
        self.uri = uri
        self.detail = detail


class StateWriteError(StateError):
    file: Path
    detail: str

    def __init__(self, file: Path, detail: str) -> None:
        super().__init__(f"Cannot write the state {file}: {detail}")
        self.file = file
        self.detail = detail


@dataclass(frozen=True)
class StateOutput:
    name: str
    source: TableFile
    target: TableFile


class State:
    request: Request
    folder: Path

    def __init__(self, request: Request, folder: Path) -> None:
        self.request = request
        self.folder = folder

    @property
    def file(self) -> Path:
        return self.folder / STATE_FILE

    @staticmethod
    def load(source: TableFile) -> FunctionState:
        try:
            frame = source.read()
        except Exception as error:
            raise StateError(f"Cannot read the state {source.uri}: {error}") from error
        try:
            return FunctionState.of(frame, source.uri)
        except InvalidFunctionStateError as error:
            raise StateError(str(error)) from error

    @staticmethod
    def save(state: FunctionState, path: Path) -> None:
        try:
            state.validate()
        except InvalidFunctionStateError as error:
            raise StateError(str(error)) from error.__cause__
        temporary = path.with_name(path.name + TEMPORARY_SUFFIX)
        try:
            state.frame().write_parquet(temporary)
            os.replace(temporary, path)
        except BaseException:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as error:
                logger.warning(f"Cannot delete {temporary}: {error}")
            raise

    def hydrate(
        self,
        context: FromRequestCtx,
        *,
        prefer_local: bool = False,
    ) -> None:
        source = self.source(prefer_local=prefer_local)
        if source is None:
            return
        try:
            state = self.load(source)
        except StateError as error:
            raise StateReadError(source.uri, str(error)) from error
        context.hydrate(state.values)

    def drain(self, context: FromRequestCtx) -> None:
        if self.output_location is None:
            return
        self.folder.mkdir(parents=True, exist_ok=True)
        try:
            self.save(FunctionState(context.drain()), self.file)
        except StateError as error:
            raise StateWriteError(self.file, str(error)) from error

    def output(self) -> StateOutput | None:
        location = self.output_location
        if location is None or not self.file.is_file():
            return None
        return StateOutput(
            name=self.request.system_output[0].table,
            source=TableFile.local(self.file),
            target=TableFile.stored(location),
        )

    def source(self, *, prefer_local: bool = False) -> TableFile | None:
        if prefer_local and self.file.is_file():
            logger.debug("State read from this run's %s", self.file)
            return TableFile.local(self.file)
        if not self.request.system_input:
            logger.debug("State not read: no system input declared")
            return None
        slot = self.request.system_input[0]
        if slot.location is None:
            logger.debug("State not read: slot %r has no location", slot.table)
            return None
        logger.debug("State read from %s for slot %r", slot.location.uri, slot.table)
        return TableFile.stored(slot.location)

    @property
    def output_location(self) -> Location | None:
        if not self.request.system_output:
            return None
        return self.request.system_output[0].location

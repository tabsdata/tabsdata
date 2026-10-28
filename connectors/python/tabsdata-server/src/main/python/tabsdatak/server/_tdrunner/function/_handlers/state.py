#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from pydantic import ConfigDict, TypeAdapter, ValidationError

from tabsdatak.api import BasicTypeSpec, ContextKeySpec
from tabsdatak.error import ErrorCode
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile

if TYPE_CHECKING:
    from tabsdatak.server._tdrunner.function._commons.document.request import (
        Location,
        Request,
    )
    from tabsdatak.server._tdrunner.function._lib.request_ctx import FromRequestCtx

logger = logging.getLogger(__name__)

KEY_ADAPTER = TypeAdapter(ContextKeySpec, config=ConfigDict(strict=True))
VALUE_ADAPTER = TypeAdapter(BasicTypeSpec, config=ConfigDict(strict=True))

USER_NAMESPACE_COLUMN = "user_namespace"
STATE_FILE = "state.parquet"
TEMPORARY_SUFFIX = ".tmp"


class StateError(ValueError):
    pass


def validate_key(key: Any) -> None:
    try:
        KEY_ADAPTER.validate_python(key)
    except ValidationError as error:
        raise StateError(
            f"Key {key!r} is not a valid ContextKeySpec (must be a str)"
        ) from error


def validate_value(key: str, value: Any) -> None:
    try:
        VALUE_ADAPTER.validate_python(value)
    except ValidationError as error:
        raise StateError(
            f"Value for key {key!r} is not a valid BasicTypeSpec "
            f"(str/int/float/bool; got type {type(value).__name__!r})"
        ) from error


def read_state(source: TableFile) -> dict[str, Any]:
    try:
        frame = source.read()
    except Exception as error:
        raise StateError(f"Cannot read the state {source.uri}: {error}") from error
    if frame.width == 0:
        return {}
    if frame.columns != [USER_NAMESPACE_COLUMN]:
        raise StateError(
            f"State {source.uri} must have the single column "
            f"{USER_NAMESPACE_COLUMN!r}; got {frame.columns!r}"
        )
    if frame.height != 1:
        raise StateError(f"State {source.uri} has {frame.height} row(s); expected 1")
    return dict(frame.item(0, USER_NAMESPACE_COLUMN) or {})


def write_state(values: dict[str, Any], path: Path) -> None:
    for key, value in values.items():
        validate_key(key)
        validate_value(key, value)
    temporary = path.with_name(path.name + TEMPORARY_SUFFIX)
    frame = (
        pl.DataFrame({USER_NAMESPACE_COLUMN: [values]}) if values else pl.DataFrame()
    )
    try:
        frame.write_parquet(temporary)
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


@dataclass(frozen=True)
class StateOutput:
    name: str
    source: TableFile
    target: TableFile


class State:
    def __init__(self, request: Request, folder: Path) -> None:
        self.request = request
        self.folder = folder

    @property
    def file(self) -> Path:
        return self.folder / STATE_FILE

    def hydrate(
        self,
        context: FromRequestCtx,
        *,
        error_code: ErrorCode,
        prefer_local: bool = False,
    ) -> None:
        source = self.source(prefer_local=prefer_local)
        if source is None:
            return
        try:
            values = read_state(source)
        except StateError as error:
            raise error_code.exception(
                cause=error, state_file=source.uri, detail=str(error)
            )
        context.hydrate(values)

    def drain(self, context: FromRequestCtx, *, error_code: ErrorCode) -> None:
        if self.output_location is None:
            return
        self.folder.mkdir(parents=True, exist_ok=True)
        try:
            write_state(context.drain(), self.file)
        except StateError as error:
            raise error_code.exception(
                cause=error, state_file=self.file, detail=str(error)
            )

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

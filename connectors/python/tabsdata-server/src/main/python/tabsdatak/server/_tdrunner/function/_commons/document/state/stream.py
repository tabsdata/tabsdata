#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from typing import Literal

from pydantic import ConfigDict, Field, ValidationError

from tabsdatak.common._configuration.document.loader import DocumentModel
from tabsdatak.server._tdrunner.function._commons.storage.stage import (
    STREAM_STATE_FILE,
    Stage,
)
from tabsdatak.server._tdrunner.function._lib.errors import CommandErrorCode

KIND = "streamState"
API_VERSION = 1


class InvalidStreamStateError(ValueError):
    pass


class StreamState(DocumentModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    state: dict
    external_id: str | None = Field(default=None, alias="externalId")
    tabsdata_version: str | None = Field(default=None, alias="tabsdataVersion")
    kind: Literal["streamState"]
    api_version: Literal[1] = Field(alias="apiVersion")

    @classmethod
    def of(
        cls,
        state: dict,
        *,
        external_id: str | None,
        tabsdata_version: str,
    ) -> StreamState:
        return cls(
            state=state,
            external_id=external_id,
            tabsdata_version=tabsdata_version,
            kind=KIND,
            api_version=API_VERSION,
        )

    @classmethod
    def load(cls, stage: Stage) -> StreamState | None:
        try:
            text = stage.read(STREAM_STATE_FILE)
        except Exception as error:
            raise CommandErrorCode.CMD_6.exception(
                cause=error,
                file=STREAM_STATE_FILE,
                detail=str(error),
            )
        if text is None:
            return None
        try:
            envelope = json.loads(text)
        except ValueError as error:
            raise CommandErrorCode.CMD_7.exception(
                cause=error,
                file=STREAM_STATE_FILE,
                detail=f"not JSON: {error}",
            )
        if not isinstance(envelope, dict):
            raise CommandErrorCode.CMD_7.exception(
                file=STREAM_STATE_FILE,
                detail=f"a {type(envelope).__name__} where an envelope object is "
                f"expected",
            )
        try:
            return cls.parse(envelope)
        except InvalidStreamStateError as error:
            raise CommandErrorCode.CMD_7.exception(
                cause=error,
                file=STREAM_STATE_FILE,
                detail=str(error),
            )

    @classmethod
    def parse(cls, envelope: dict) -> StreamState:
        try:
            return cls.model_validate(envelope)
        except ValidationError as error:
            raise InvalidStreamStateError(str(error)) from error

    def envelope(self) -> dict:
        return self.model_dump(by_alias=True)

    def save(self, stage: Stage) -> None:
        stage.write(STREAM_STATE_FILE, json.dumps(self.envelope()))

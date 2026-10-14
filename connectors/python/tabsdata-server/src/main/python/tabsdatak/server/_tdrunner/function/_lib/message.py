#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from tabsdatak.common._enums import (
    DestinationWriteOp,
    MessageAction,
    WorkerCallbackStatus,
)

EXIT_STATUS_SUCCESS = 0


class ResponseMessagePayload(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        use_enum_values=True,
    )
    action: MessageAction = MessageAction.NOTIFY
    start: int | None = None
    end: int | None = None
    status: WorkerCallbackStatus
    error_message: str | None = None
    exit_status: int = EXIT_STATUS_SUCCESS
    context: Any | None = None

    def to_body(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude_none=True,
        )


class DestinationWriteMessage(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        use_enum_values=True,
    )
    op: DestinationWriteOp = DestinationWriteOp.DESTINATION_WRITE

    def to_body(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude_none=True,
        )

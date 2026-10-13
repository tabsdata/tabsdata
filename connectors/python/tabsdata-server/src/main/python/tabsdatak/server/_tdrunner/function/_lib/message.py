#
# Copyright 2026 Tabsdata Inc.
#

"""Structured responder callback payload.

:class:`ResponseMessagePayload` mirrors the Rust struct of the same name
in ``td-common/src/server.rs``. It is the JSON body a command POSTs back
to the responder endpoint to report its outcome; using the model instead
of an ad-hoc dict keeps field names, defaults, and the ``None``-omission
behaviour aligned with what the apiserver deserializes.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from tabsdatak.common._enums import MessageAction, WorkerCallbackStatus

EXIT_STATUS_SUCCESS = 0


class ResponseMessagePayload(BaseModel):
    """Mirror of the Rust ``ResponseMessagePayload`` in
    ``td-common/src/server.rs``."""

    model_config = ConfigDict(frozen=True, extra="forbid", use_enum_values=True)

    action: MessageAction = MessageAction.NOTIFY
    start: int | None = None
    end: int | None = None
    status: WorkerCallbackStatus
    error_message: str | None = None
    exit_status: int = EXIT_STATUS_SUCCESS
    context: Any | None = None

    def to_body(self) -> dict[str, Any]:
        """Serialize to the responder POST body.

        ``None`` fields are dropped so the wire shape matches serde's
        ``Option`` handling on the apiserver side.
        """
        return self.model_dump(mode="json", exclude_none=True)


class DestinationWriteMessage(BaseModel):
    """Mirror of the Rust ``ExecutionCallbackRequest::DestinationWrite`` in
    ``td-objects/src/dxo/execution_callback.rs``: the run is about to write
    to its destination. A body without ``op`` is a status report."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    op: Literal["destination_write"] = "destination_write"

    def to_body(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

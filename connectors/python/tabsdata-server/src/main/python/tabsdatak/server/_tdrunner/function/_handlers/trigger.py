#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from enum import Enum

from tabsdatak.common._configuration.document.loader import DocumentModel
from tabsdatak.server._tdrunner.function._commons.http.request import (
    HttpRequest,
    HttpResponse,
    invoke,
)
from tabsdatak.server._tdrunner.function._commons.http.retry import call_with_retry
from tabsdatak.server._tdrunner.function._commons.solver.arguments import (
    resolve_value,
)

ENCODING = "utf-8"
JSON_CONTENT_TYPE = "application/json"
PREVIEW_BYTES = 256


def preview(body: bytes | None) -> bytes:
    return (body or b"")[:PREVIEW_BYTES]


class TriggerType(Enum):
    CRON = "CT"
    STAGE = "ST"


class TriggerError(Exception):
    pass


class MalformedAnswerError(TriggerError):
    body: bytes

    def __init__(self, body: bytes) -> None:
        super().__init__(f"The trigger answer has no data.outcome: {body!r}")
        self.body = body


class UnknownOutcomeError(TriggerError):
    outcome: str

    def __init__(self, outcome: str) -> None:
        super().__init__(f"The trigger answered the unknown outcome {outcome!r}")
        self.outcome = outcome


class TriggerOutcome(Enum):
    SCHEDULED = "Scheduled"
    BUSY = "Busy"
    HELD = "Held"
    CONSUMED = "Consumed"

    @classmethod
    def parse(cls, body: bytes | None) -> TriggerOutcome:
        try:
            answer = json.loads(body or b"")
        except ValueError:
            answer = None
        data = answer.get("data") if isinstance(answer, dict) else None
        outcome = data.get("outcome") if isinstance(data, dict) else None
        if not isinstance(outcome, str):
            raise MalformedAnswerError(preview(body))
        try:
            return cls(outcome)
        except ValueError:
            raise UnknownOutcomeError(outcome) from None


class ExecutionRequest(DocumentModel):
    name: str | None = None
    function_only: bool = False
    skip_if_running: bool
    external_uid: str
    trigger_type: TriggerType

    def body(self) -> bytes:
        return self.model_dump_json().encode(ENCODING)


class ExecutionTrigger:
    url: str
    method: str
    authorization: str

    def __init__(self, url: str, method: str, authorization: str) -> None:
        self.url = url
        self.method = method
        self.authorization = authorization

    @classmethod
    def resolve(cls, url: str, method: str, authorization: str) -> ExecutionTrigger:
        return cls(
            resolve_value(url),
            resolve_value(method),
            resolve_value(authorization),
        )

    def request(self) -> HttpRequest:
        return HttpRequest(
            url=self.url,
            method=self.method,
            headers={
                "Authorization": self.authorization,
                "Content-Type": JSON_CONTENT_TYPE,
            },
        )

    def post(self, execution: ExecutionRequest) -> HttpResponse:
        return invoke(self.request(), body=execution.body())

    def post_with_retry(self, execution: ExecutionRequest) -> HttpResponse:
        return call_with_retry(invoke, self.request(), body=execution.body())

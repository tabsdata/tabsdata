#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import json
import time
from http import HTTPStatus

import pytest

from tabsdatak.server._tdrunner.function._commons.http.request import (
    HttpRequestError,
    HttpResponse,
)
from tabsdatak.server._tdrunner.function.commands.run_cron_trigger import runner

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


class _Apiserver:
    """The internal execute endpoint: a fresh execution per accepted call,
    answering Held for an external_uid already in use, as the server does."""

    def __init__(self, faults: list) -> None:
        self.faults = faults
        self.executions: list[str | None] = []

    def invoke(self, request, body=None, **kwargs):
        external_uid = json.loads(body)["external_uid"]
        fault = self.faults.pop(0) if self.faults else None
        if fault == "unavailable":
            return HttpResponse(status=HTTPStatus.INTERNAL_SERVER_ERROR, body=b"")
        if external_uid is not None and external_uid in self.executions:
            return HttpResponse(
                status=HTTPStatus.OK, body=b'{"data":{"outcome":"Held"}}'
            )
        self.executions.append(external_uid)
        if fault == "timeout after commit":
            raise HttpRequestError("timed out after 30.0s")
        return HttpResponse(status=HTTPStatus.OK, body=b"{}")


def _fire(monkeypatch, apiserver: _Apiserver) -> int:
    monkeypatch.setattr(runner, "invoke", apiserver.invoke)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
    return runner.run(
        argparse.Namespace(
            trigger_url="val:http://api/execute",
            trigger_method="val:POST",
            trigger_authorization="val:Bearer t",
        )
    )


def test_a_cron_fire_survives_one_transient_failure(monkeypatch):
    """REC-6: the apiserver answers the cron pod's execute call with one
    transient 500 (a restart, or LockBusy on the project lock) and then
    recovers. The scheduled fire must still happen, exactly once."""
    apiserver = _Apiserver(["unavailable"])

    assert _fire(monkeypatch, apiserver) == 0
    assert len(apiserver.executions) == 1


def test_a_cron_fire_whose_answer_is_lost_happens_exactly_once(monkeypatch):
    """REC-6: the execution is committed but the answer is lost (client
    timeout). The fire must neither be reported failed nor be repeated as a
    second execution."""
    apiserver = _Apiserver(["timeout after commit"])

    assert _fire(monkeypatch, apiserver) == 0
    assert len(apiserver.executions) == 1

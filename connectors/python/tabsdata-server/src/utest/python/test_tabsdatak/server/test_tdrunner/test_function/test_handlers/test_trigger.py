#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json

import pytest

from tabsdatak.server._tdrunner.function._handlers.trigger import (
    ExecutionRequest,
    ExecutionTrigger,
    MalformedAnswerError,
    TriggerOutcome,
    TriggerType,
    UnknownOutcomeError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def answer(outcome: object) -> bytes:
    return json.dumps({"data": {"outcome": outcome}}).encode("utf-8")


@pytest.mark.parametrize("outcome", list(TriggerOutcome))
def test_every_outcome_the_apiserver_names_is_known(outcome):
    assert TriggerOutcome.parse(answer(outcome.value)) is outcome


def test_an_outcome_the_apiserver_grew_is_refused():
    with pytest.raises(UnknownOutcomeError, match="'Parked'"):
        TriggerOutcome.parse(answer("Parked"))


@pytest.mark.parametrize(
    "body",
    [None, b"", b"not json", b'{"data": {}}', answer(7)],
    ids=["none", "empty", "not json", "no outcome", "not a name"],
)
def test_an_answer_without_an_outcome_is_refused(body):
    with pytest.raises(MalformedAnswerError):
        TriggerOutcome.parse(body)


def test_an_execution_request_is_the_internal_execution_request_body():
    request = ExecutionRequest(
        trigger_type=TriggerType.STAGE,
        external_uid="2026-06-16T10-30-12.345000",
        skip_if_running=True,
    )
    assert json.loads(request.body()) == {
        "name": None,
        "function_only": False,
        "skip_if_running": True,
        "external_uid": "2026-06-16T10-30-12.345000",
        "trigger_type": "ST",
    }


def test_a_trigger_resolves_literal_values():
    trigger = ExecutionTrigger.resolve(
        "http://apiserver/fn_trigger",
        "POST",
        "Bearer abc",
    )
    request = trigger.request()
    assert request.url == "http://apiserver/fn_trigger"
    assert request.method == "POST"
    assert request.headers == {
        "Authorization": "Bearer abc",
        "Content-Type": "application/json",
    }


def test_a_trigger_resolves_environment_references(monkeypatch):
    monkeypatch.setenv("FAKE_URL", "http://from-env/fn_trigger")
    monkeypatch.setenv("FAKE_METHOD", "POST")
    monkeypatch.setenv("FAKE_AUTH", "Bearer from-env")
    trigger = ExecutionTrigger.resolve(
        "env:FAKE_URL", "env:FAKE_METHOD", "env:FAKE_AUTH"
    )
    assert trigger.url == "http://from-env/fn_trigger"
    assert trigger.authorization == "Bearer from-env"

#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the run-cron-trigger one-shot command."""

from __future__ import annotations

import json

import pytest

from tabsdatak.server._tdrunner.function._commons.http import retry
from tabsdatak.server._tdrunner.function._commons.http.request import (
    HttpRequest,
    HttpRequestError,
    HttpResponse,
)
from tabsdatak.server._tdrunner.function.commands.run_cron_trigger import (
    _build_parser,
    _execution_request_body,
    _fire,
    main,
    runner,
)
from tabsdatak.server._tdrunner.function.commands.run_cron_trigger.error import (
    CronTriggerConnectionError,
    CronTriggerRejectedError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def _std_args() -> list[str]:
    """The standard responder/liveness/log/journal args build_parser requires."""
    return [
        "--shared-dir",
        "/s",
        "--work-dir",
        "/w",
        "--log-config",
        "/c",
        "--liveness",
        "/l",
        "--responder-url",
        "val:http://r",
        "--responder-method",
        "val:POST",
        "--responder-authorization",
        "val:Bearer r",
        "--journal-file",
        "/j",
    ]


# --- body shape (the regression that matters) -----------------------------


def test_execution_request_body_is_cron_trigger_shape():
    # A fresh execution every fire (skip_if_running false), keyed by the
    # fire so its retries dedup, tagged TriggerType::CronTrigger ("CT").
    assert json.loads(_execution_request_body("cron-k")) == {
        "name": None,
        "function_only": False,
        "skip_if_running": False,
        "external_uid": "cron-k",
        "trigger_type": "CT",
    }


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td_run_cron_trigger"


def test_build_parser_accepts_trigger_triplet():
    parser = _build_parser()
    ns = parser.parse_args(
        _std_args()
        + [
            "--trigger-url",
            "env:SECRET__TD_TRIGGER_URL",
            "--trigger-method",
            "val:POST",
            "--trigger-authorization",
            "val:Bearer t",
        ]
    )
    assert ns.trigger_url == "env:SECRET__TD_TRIGGER_URL"
    assert ns.trigger_method == "val:POST"
    assert ns.trigger_authorization == "val:Bearer t"


# --- _fire: POST with retries, classify the response ----------------------


@pytest.fixture
def short_budget(monkeypatch):
    monkeypatch.setattr(retry, "WAIT_SECONDS", 0)
    monkeypatch.setattr(retry, "TOTAL_SECONDS", 0.2)


def _capture_invoke(*, response: HttpResponse | None = None, raise_exc=None):
    captured: dict = {"attempts": 0}

    def fake_invoke(request, body=None):
        captured["attempts"] += 1
        captured["request"] = request
        captured["body"] = body
        if raise_exc is not None:
            raise raise_exc
        return response

    return fake_invoke, captured


def test_fire_posts_cron_body_and_returns_on_2xx(monkeypatch):
    fake, captured = _capture_invoke(response=HttpResponse(status=200, body=b"{}"))
    monkeypatch.setattr(runner, "invoke", fake)

    # 2xx -> returns cleanly (no raise); the @command wrapper turns this into
    # exit 0.
    assert _fire("http://api/execute", "POST", "Bearer tok") is None

    request = captured["request"]
    assert isinstance(request, HttpRequest)
    assert request.url == "http://api/execute"
    assert request.method == "POST"
    assert request.headers["Authorization"] == "Bearer tok"
    assert request.headers["Content-Type"] == "application/json"
    assert json.loads(captured["body"])["trigger_type"] == "CT"


def test_fire_raises_ct2_on_non_2xx_without_retrying(monkeypatch):
    fake, captured = _capture_invoke(
        response=HttpResponse(status=403, body=b"forbidden")
    )
    monkeypatch.setattr(runner, "invoke", fake)

    with pytest.raises(CronTriggerRejectedError) as excinfo:
        _fire("http://api/execute", "POST", "Bearer tok")
    assert excinfo.value.exit_code == 83
    assert "403" in str(excinfo.value)
    assert captured["attempts"] == 1


def test_fire_raises_ct2_on_a_server_error_that_outlasts_the_retries(
    monkeypatch, short_budget
):
    fake, captured = _capture_invoke(response=HttpResponse(status=503, body=b""))
    monkeypatch.setattr(runner, "invoke", fake)

    with pytest.raises(CronTriggerRejectedError) as excinfo:
        _fire("http://api/execute", "POST", "Bearer tok")
    assert "503" in str(excinfo.value)
    assert captured["attempts"] > 1


def test_fire_retries_until_the_apiserver_answers(monkeypatch, short_budget):
    responses = [
        HttpRequestError("connection refused"),
        HttpResponse(status=503, body=b""),
        HttpResponse(status=200, body=b"{}"),
    ]

    def fake_invoke(request, body=None):
        outcome = responses.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(runner, "invoke", fake_invoke)

    assert _fire("http://api/execute", "POST", "Bearer tok") is None
    assert responses == []


def test_fire_raises_ct1_on_connection_error(monkeypatch, short_budget):
    fake, captured = _capture_invoke(
        raise_exc=HttpRequestError("connection refused")
    )
    monkeypatch.setattr(runner, "invoke", fake)

    with pytest.raises(CronTriggerConnectionError) as excinfo:
        _fire("http://api/execute", "POST", "Bearer tok")
    assert excinfo.value.exit_code == 82
    assert "http://api/execute" in str(excinfo.value)
    assert captured["attempts"] > 1

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._commons.http import retry
from tabsdatak.server._tdrunner.function._commons.http.request import (
    HttpRequest,
    HttpRequestError,
    HttpResponse,
)
from tabsdatak.server._tdrunner.function._handlers import trigger
from tabsdatak.server._tdrunner.function.commands.runners.cron_trigger import (
    build_command_parser,
    main,
)
from tabsdatak.server._tdrunner.function.commands.runners.cron_trigger.error import (
    CronTriggerConnectionError,
    CronTriggerRejectedError,
)
from tabsdatak.server._tdrunner.function.commands.runners.cron_trigger.options import (  # noqa: E501
    RunCronTriggerArguments,
)
from tabsdatak.server._tdrunner.function.commands.runners.cron_trigger.runner import (  # noqa: E501
    CronTrigger,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def cron_trigger() -> CronTrigger:
    return CronTrigger(
        RunCronTriggerArguments(
            liveness=Path("/l"),
            responder_url="val:http://r",
            responder_method="val:POST",
            responder_authorization="val:Bearer r",
            journal_file=Path("/j"),
            trigger_url="val:http://api/execute",
            trigger_method="val:POST",
            trigger_authorization="val:Bearer tok",
        )
    )


def _std_args() -> list[str]:
    return [
        "--shared-folder",
        "/s",
        "--work-folder",
        "/w",
        "--logging-configuration",
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


def test_a_fire_asks_for_a_fresh_cron_execution_keyed_by_the_fire():
    body = json.loads(cron_trigger().execution.body())
    assert body["external_uid"].startswith("cron-")
    assert body == {
        "name": None,
        "function_only": False,
        "skip_if_running": False,
        "external_uid": body["external_uid"],
        "trigger_type": "CT",
    }


def test_each_fire_has_its_own_key():
    assert cron_trigger().execution.external_uid != (
        cron_trigger().execution.external_uid
    )


def test_main_carries_tabsdata_command_metadata():
    assert getattr(main, "tabsdata_command", False) is True
    assert getattr(main, "tabsdata_command_name", None) == "td-t-src-cron"


def test_build_command_parser_accepts_trigger_triplet():
    arguments = build_command_parser().parse(
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
    assert arguments.trigger_url == "env:SECRET__TD_TRIGGER_URL"
    assert arguments.trigger_method == "val:POST"
    assert arguments.trigger_authorization == "val:Bearer t"


# --- fire: POST with retries, classify the response -----------------------


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
    monkeypatch.setattr(trigger, "invoke", fake)

    # 2xx -> returns cleanly (no raise); the @command wrapper turns this into
    # exit 0.
    assert cron_trigger().fire() is None

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
    monkeypatch.setattr(trigger, "invoke", fake)

    with pytest.raises(CronTriggerRejectedError) as excinfo:
        cron_trigger().fire()
    assert excinfo.value.exit_code == 83
    assert "403" in str(excinfo.value)
    assert captured["attempts"] == 1


def test_fire_raises_ct2_on_a_server_error_that_outlasts_the_retries(
    monkeypatch, short_budget
):
    fake, captured = _capture_invoke(response=HttpResponse(status=503, body=b""))
    monkeypatch.setattr(trigger, "invoke", fake)

    with pytest.raises(CronTriggerRejectedError) as excinfo:
        cron_trigger().fire()
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

    monkeypatch.setattr(trigger, "invoke", fake_invoke)

    assert cron_trigger().fire() is None
    assert responses == []


def test_fire_raises_ct1_on_connection_error(monkeypatch, short_budget):
    fake, captured = _capture_invoke(raise_exc=HttpRequestError("connection refused"))
    monkeypatch.setattr(trigger, "invoke", fake)

    with pytest.raises(CronTriggerConnectionError) as excinfo:
        cron_trigger().fire()
    assert excinfo.value.exit_code == 82
    assert "http://api/execute" in str(excinfo.value)
    assert captured["attempts"] > 1

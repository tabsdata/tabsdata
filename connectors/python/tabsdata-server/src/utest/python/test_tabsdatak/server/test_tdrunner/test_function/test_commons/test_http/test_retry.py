#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import urllib.error
from http import HTTPStatus

import pytest

from tabsdatak.server._tdrunner.function._commons.http import retry
from tabsdatak.server._tdrunner.function._commons.http.request import HttpResponse
from tabsdatak.server._tdrunner.function._commons.http.retry import (
    call_with_retry,
    is_retriable,
    is_retriable_response,
)
from tabsdatak.server._tdrunner.function._commons.run import shutdown

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


def http_error(status: HTTPStatus) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("http://x/p", status, status.phrase, {}, None)


@pytest.fixture
def short_budget(monkeypatch):
    monkeypatch.setattr(retry, "WAIT_SECONDS", 0)
    monkeypatch.setattr(retry, "TOTAL_SECONDS", 0.2)


@pytest.mark.parametrize(
    "status",
    [
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT,
        HTTPStatus.REQUEST_TIMEOUT,
        HTTPStatus.TOO_MANY_REQUESTS,
    ],
)
def test_retriable_statuses(status):
    assert is_retriable(http_error(status))


@pytest.mark.parametrize(
    "status",
    [
        HTTPStatus.BAD_REQUEST,
        HTTPStatus.UNAUTHORIZED,
        HTTPStatus.FORBIDDEN,
        HTTPStatus.NOT_FOUND,
        HTTPStatus.CONFLICT,
        HTTPStatus.GONE,
        HTTPStatus.MOVED_PERMANENTLY,
    ],
)
def test_non_retriable_statuses(status):
    assert not is_retriable(http_error(status))


@pytest.mark.parametrize(
    "error",
    [
        urllib.error.URLError("connection refused"),
        TimeoutError("timed out"),
        ConnectionResetError("reset by peer"),
        RuntimeError("unexpected"),
    ],
)
def test_retriable_failures(error):
    assert is_retriable(error)


@pytest.fixture
def shutdown_requested(monkeypatch):
    monkeypatch.setattr(shutdown, "requested", True)


@pytest.mark.parametrize(
    ("status", "retriable"),
    [
        (HTTPStatus.SERVICE_UNAVAILABLE, True),
        (HTTPStatus.TOO_MANY_REQUESTS, True),
        (HTTPStatus.OK, False),
        (HTTPStatus.CONFLICT, False),
    ],
)
def test_retriable_responses(status, retriable):
    assert is_retriable_response(HttpResponse(status=status, body=b"")) is retriable


def test_malformed_url_is_not_retriable():
    assert not is_retriable(ValueError("unknown url type"))


def test_returns_result_and_forwards_arguments():
    assert call_with_retry(lambda first, second=0: first + second, 1, second=2) == 3


def test_non_retriable_error_fails_on_first_attempt(short_budget):
    attempts = []

    def call():
        attempts.append(1)
        raise http_error(HTTPStatus.BAD_REQUEST)

    with pytest.raises(urllib.error.HTTPError):
        call_with_retry(call)
    assert len(attempts) == 1


def test_retriable_error_is_retried_until_success(short_budget):
    attempts = []

    def call():
        attempts.append(1)
        if len(attempts) < 3:
            raise http_error(HTTPStatus.SERVICE_UNAVAILABLE)
        return "done"

    assert call_with_retry(call) == "done"
    assert len(attempts) == 3


def test_spent_budget_reraises_last_error(short_budget):
    attempts = []
    error = urllib.error.URLError("connection refused")

    def call():
        attempts.append(1)
        raise error

    with pytest.raises(urllib.error.URLError) as raised:
        call_with_retry(call)
    assert raised.value is error
    assert len(attempts) > 1


def test_retriable_response_is_retried_until_success(short_budget):
    responses = [
        HttpResponse(status=HTTPStatus.SERVICE_UNAVAILABLE, body=b""),
        HttpResponse(status=HTTPStatus.OK, body=b"done"),
    ]

    assert call_with_retry(responses.pop, 0).body == b"done"


def test_spent_budget_returns_last_retriable_response(short_budget):
    response = HttpResponse(status=HTTPStatus.BAD_GATEWAY, body=b"")

    assert call_with_retry(lambda: response) is response


def test_shutdown_stops_retrying_after_the_first_attempt(shutdown_requested):
    attempts = []
    error = urllib.error.URLError("connection refused")

    def call():
        attempts.append(1)
        raise error

    with pytest.raises(urllib.error.URLError) as raised:
        call_with_retry(call)
    assert raised.value is error
    assert len(attempts) == 1


def test_shutdown_during_retries_stops_them(monkeypatch):
    monkeypatch.setattr(retry, "WAIT_SECONDS", 0)
    monkeypatch.setattr(shutdown, "requested", False)
    attempts = []

    def call():
        attempts.append(1)
        if len(attempts) == 2:
            shutdown.request_shutdown()
        raise urllib.error.URLError("connection refused")

    with pytest.raises(urllib.error.URLError):
        call_with_retry(call)
    assert len(attempts) == 2


def test_shutdown_keeps_successful_calls(shutdown_requested):
    assert call_with_retry(lambda: "done") == "done"

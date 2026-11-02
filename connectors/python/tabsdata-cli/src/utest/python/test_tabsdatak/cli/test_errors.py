#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests
import rich_click as click
from click.testing import CliRunner

from tabsdatak.cli._errors import ExitCode, TdkException, classify
from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk._apiserver import APIServerError, NoProjectSelectedError, NotFound
from tabsdatak.sdk.tabsdata_server import TabsdataServer


def _wrapped(cause: BaseException) -> click.ClickException:
    try:
        raise cause
    except BaseException as e:
        try:
            raise click.ClickException(f"Failed to do it: {e}")
        except click.ClickException as wrapped:
            return wrapped


def _api(status: int, code: str | None = None, error: str | None = None):
    return APIServerError(
        {"code": code, "error": error, "error_description": "server says no"},
        status=status,
    )


@pytest.mark.parametrize(
    "status, code, error, expected",
    [
        (400, "ListError::1", "invalid_request", "invalid_request"),
        (400, "CollectionError::7", "not_allowed", "conflict"),
        (400, "SqlError::1", "invalid_request", "conflict"),
        (400, "SqlError::2", "invalid_request", "conflict"),
        (401, "AuthError::4000", "unauthorized", "auth_required"),
        (403, "RbacError::3001", "forbidden", "service_token_unsupported"),
        (403, "RbacError::3002", "forbidden", "session_unsupported"),
        (403, "RbacError::1", "forbidden", "forbidden"),
        (404, "FunctionError::1003", "not_found", "not_found"),
        (409, None, None, "conflict"),
        (423, None, None, "conflict"),
        (429, None, None, "rate_limited"),
        (503, None, None, "unavailable"),
        (500, None, "internal_error", "server_error"),
    ],
)
def test_api_errors_classify_by_status_and_code(status, code, error, expected):
    classified = classify(_wrapped(_api(status, code, error)))
    assert classified.type == expected
    assert classified.status == status
    assert classified.code == code
    assert classified.message == "Failed to do it: server says no"


def test_api_error_carries_retry_after():
    error = APIServerError({"code": "X"}, status=429, retry_after=3.0)
    assert classify(_wrapped(error)).retry_after == 3.0


@pytest.mark.parametrize(
    "cause, expected",
    [
        (requests.exceptions.ConnectionError("refused"), "unreachable"),
        (requests.exceptions.ReadTimeout("slow"), "unreachable"),
        (NoProjectSelectedError(), "no_project"),
        (NotFound("gone"), "not_found"),
        (click.Abort(), "aborted"),
        (click.UsageError("bad flag"), "usage"),
        (ValueError("plain"), "internal"),
    ],
)
def test_client_side_causes_classify(cause, expected):
    assert classify(_wrapped(cause)).type == expected


def test_bare_click_exception_is_an_invalid_request():
    assert classify(click.ClickException("bad input")).type == "invalid_request"


def test_click_exception_raised_from_none_is_an_invalid_request():
    try:
        try:
            raise ValueError("bad yaml")
        except ValueError:
            raise click.ClickException("invalid connection YAML") from None
    except click.ClickException as error:
        assert classify(error).type == "invalid_request"


def test_unwrapped_exception_is_internal():
    assert classify(RuntimeError("boom")).type == "internal"


def test_typed_exception_keeps_its_type():
    classified = classify(_wrapped(TdkException("auth_expired", "expired")))
    assert classified.type == "auth_expired"
    assert classified.exit_code == ExitCode.AUTH


def _invoke(argv: list[str], client: MagicMock, tmp_path):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv)


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    return client


@pytest.mark.parametrize(
    "status, code, exit_code",
    [
        (404, "CollectionError::1", ExitCode.NOT_FOUND),
        (401, "AuthError::4000", ExitCode.AUTH),
        (403, "RbacError::3001", ExitCode.FORBIDDEN),
        (400, "SqlError::1", ExitCode.CONFLICT),
        (500, None, ExitCode.FAILURE),
    ],
)
def test_command_failures_exit_with_the_canonical_code(
    tmp_path, status, code, exit_code
):
    client = _client()
    client.get_collection.side_effect = _api(status, code)
    result = _invoke(["collection", "get", "--name", "c1"], client, tmp_path)
    assert result.exit_code == exit_code, result.output
    assert "server says no" in result.output


def test_unexpected_exception_exits_one_with_message(tmp_path):
    client = _client()
    client.get_collection.side_effect = RuntimeError("kaboom")
    result = _invoke(["collection", "get", "--name", "c1"], client, tmp_path)
    assert result.exit_code == ExitCode.FAILURE
    assert "kaboom" in result.output


def test_usage_errors_keep_exit_two(tmp_path):
    result = _invoke(["collection", "get", "--no-such-flag"], _client(), tmp_path)
    assert result.exit_code == ExitCode.USAGE

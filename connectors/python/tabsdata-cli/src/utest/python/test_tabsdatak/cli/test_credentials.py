#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import base64
import json
import time
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli._credentials import (
    EnvCredential,
    env_credential,
    refine_auth_error,
)
from tabsdatak.cli._errors import ExitCode, TdkError, TdkException
from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk._apiserver import APIServerError
from tabsdatak.sdk.tabsdata_server import TabsdataServer

SERVER = "http://127.0.0.1:2457"


def _jwt(exp: float) -> str:
    def part(obj) -> str:
        raw = json.dumps(obj).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    return f"{part({'alg': 'HS256'})}.{part({'jti': 'j', 'exp': exp})}.sig"


def _live() -> str:
    return _jwt(time.time() + 3600)


def test_no_bearer_means_no_env_credential():
    assert env_credential({}) is None
    assert env_credential({"TDK_SERVER": SERVER}) is None


def test_bearer_and_server_make_an_env_credential():
    bearer = _live()
    credential = env_credential({"TDK_BEARER": bearer, "TDK_SERVER": SERVER})
    assert credential.bearer == bearer
    assert credential.server == SERVER
    assert credential.expires_at > time.time()


@pytest.mark.parametrize(
    "env, fragment",
    [
        ({"TDK_BEARER": "tdst_v1_" + "a" * 64, "TDK_SERVER": SERVER}, "TDK_TOKEN"),
        ({"TDK_BEARER": "x.y.z"}, "TDK_SERVER"),
        ({"TDK_BEARER": "not-a-jwt", "TDK_SERVER": SERVER}, "JWT"),
    ],
)
def test_invalid_env_credentials_are_usage_errors(env, fragment):
    if "TDK_SERVER" not in env:
        env["TDK_BEARER"] = _live()
    with pytest.raises(TdkException) as excinfo:
        env_credential(env)
    assert excinfo.value.error.type == "usage"
    assert fragment in excinfo.value.error.message


def test_mid_run_401_near_expiry_is_auth_expired():
    credential = EnvCredential(
        kind="session", bearer="b", server=SERVER, expires_at=1000.0, env="TDK_BEARER"
    )
    refused = TdkError(type="auth_required", message="refused", status=401)
    assert refine_auth_error(refused, credential, now=997.0).type == "auth_expired"
    assert refine_auth_error(refused, credential, now=900.0).type == "auth_required"
    assert refine_auth_error(refused, None, now=997.0).type == "auth_required"


def test_an_expired_login_is_named_and_says_to_log_in_again():
    refused = TdkError(type="auth_required", message="refused", status=401)
    expired = refine_auth_error(refused, None, now=1000.0, login_expiry=900.0)
    assert expired.type == "auth_expired" and "tdk login" in expired.hint
    live = refine_auth_error(refused, None, now=800.0, login_expiry=900.0)
    assert live.type == "auth_required"
    forbidden = TdkError(type="forbidden", message="no", status=403)
    assert refine_auth_error(forbidden, None, now=1000.0, login_expiry=900.0) is forbidden


def test_stored_expiry_reads_the_login_file(tmp_path):
    from tabsdatak.cli._credentials import stored_expiry

    path = tmp_path / "connection.json"
    path.write_text(json.dumps({"url": SERVER, "expiration_time": 1234.5}))
    assert stored_expiry(str(path)) == 1234.5
    assert stored_expiry(str(tmp_path / "missing.json")) is None
    assert stored_expiry(None) is None


def _run(argv, env, tmp_path):
    seen = {}

    def get_collection(self, name):
        seen["connection"] = self.connection
        raise APIServerError({"error_description": "missing"}, status=404)

    home = tmp_path / "home"
    with (
        patch.object(TabsdataServer, "get_collection", get_collection),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(home)),
    ):
        result = CliRunner().invoke(cli, argv, env=env)
    return result, seen.get("connection"), home


def test_stateless_run_uses_the_env_bearer_and_writes_nothing(tmp_path):
    bearer = _live()
    env = {"TDK_BEARER": bearer, "TDK_SERVER": SERVER, "TDK_PROJECT": "sales"}
    result, connection, home = _run(
        ["collection", "get", "--name", "c1"], env, tmp_path
    )
    assert result.exit_code == ExitCode.NOT_FOUND, result.output
    assert connection.bearer_token == bearer
    assert connection.refresh_token is None
    assert connection.credentials_file is None
    assert connection.url.startswith(SERVER)
    assert connection.project == "sales"
    assert not home.exists()


def test_proj_overrides_tdk_project(tmp_path):
    env = {"TDK_BEARER": _live(), "TDK_SERVER": SERVER, "TDK_PROJECT": "sales"}
    _, connection, _ = _run(
        ["collection", "get", "--name", "c1", "--proj", "ops"], env, tmp_path
    )
    assert connection.project == "ops"


def test_expired_bearer_fails_before_any_request(tmp_path):
    env = {"TDK_BEARER": _jwt(time.time() + 2), "TDK_SERVER": SERVER}
    result, connection, _ = _run(["collection", "get", "--name", "c1"], env, tmp_path)
    assert result.exit_code == ExitCode.AUTH, result.output
    assert connection is None
    assert "expired" in result.output


@pytest.mark.parametrize(
    "argv",
    [
        ["login"],
        ["logout"],
        ["set", "--hints", "false"],
        ["unset", "--hints"],
        ["auth", "add-cert", "--server", SERVER, "--pem", "-"],
        ["auth", "delete-cert", "--server", SERVER],
    ],
)
def test_state_changing_commands_are_refused(tmp_path, argv):
    env = {"TDK_BEARER": _live(), "TDK_SERVER": SERVER}
    result, _, home = _run(argv, env, tmp_path)
    assert result.exit_code == ExitCode.USAGE, result.output
    assert "TDK_BEARER" in result.output
    assert not home.exists()


def test_profile_option_is_refused(tmp_path):
    env = {"TDK_BEARER": _live(), "TDK_SERVER": SERVER}
    result, connection, _ = _run(
        ["collection", "get", "--name", "c1", "--profile", "dev"], env, tmp_path
    )
    assert result.exit_code == ExitCode.USAGE, result.output
    assert connection is None


def test_bearer_without_server_is_a_usage_error(tmp_path):
    result, connection, _ = _run(
        ["collection", "get", "--name", "c1"], {"TDK_BEARER": _live()}, tmp_path
    )
    assert result.exit_code == ExitCode.USAGE, result.output
    assert connection is None


TOKEN = "tdst_v1_" + "0123456789abcdef" * 4


def test_token_and_server_make_a_service_token_credential():
    credential = env_credential({"TDK_TOKEN": TOKEN, "TDK_SERVER": SERVER})
    assert credential.kind == "service_token"
    assert credential.bearer == TOKEN
    assert credential.expires_at is None
    assert credential.env == "TDK_TOKEN"


def test_token_without_server_only_feeds_login():
    assert env_credential({"TDK_TOKEN": TOKEN}) is None


@pytest.mark.parametrize(
    "env, error_type",
    [
        (
            {"TDK_TOKEN": TOKEN, "TDK_BEARER": "x", "TDK_SERVER": SERVER},
            "ambiguous_credentials",
        ),
        ({"TDK_TOKEN": "tdst_v1_short", "TDK_SERVER": SERVER}, "usage"),
        ({"TDK_TOKEN": TOKEN.upper(), "TDK_SERVER": SERVER}, "usage"),
    ],
)
def test_invalid_token_env_is_rejected(env, error_type):
    with pytest.raises(TdkException) as excinfo:
        env_credential(env)
    assert excinfo.value.error.type == error_type


def _token_env() -> dict:
    return {"TDK_TOKEN": TOKEN, "TDK_SERVER": SERVER, "TDK_PROJECT": "sales"}


def test_stateless_token_run_uses_the_token(tmp_path):
    result, connection, home = _run(
        ["collection", "get", "--name", "c1"], _token_env(), tmp_path
    )
    assert result.exit_code == ExitCode.NOT_FOUND, result.output
    assert connection.bearer_token == TOKEN
    assert connection.credentials_file is None
    assert not home.exists()


def _failing(status: int, code: str):
    def get_collection(self, name):
        raise APIServerError(
            {"code": code, "error_description": "refused"}, status=status
        )

    return get_collection


@pytest.mark.parametrize(
    "status, code, exit_code, hint",
    [
        (401, "AuthError::4000", ExitCode.AUTH, "expired or been revoked"),
        (403, "RbacError::3001", ExitCode.FORBIDDEN, "TDK_TOKEN"),
    ],
)
def test_token_refusals_carry_a_hint(tmp_path, status, code, exit_code, hint):
    with (
        patch.object(TabsdataServer, "get_collection", _failing(status, code)),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        result = CliRunner().invoke(
            cli, ["collection", "get", "--name", "c1"], env=_token_env()
        )
    assert result.exit_code == exit_code, result.output
    assert hint in result.output


def test_auth_info_marks_a_service_token(tmp_path):
    info = {"name": "bot", "email": "bot@x", "roles": ["ai_agent"]}
    token = {"slot": 1, "hint": "abcd", "expires_on": None, "comment": "ci"}
    with (
        patch.object(TabsdataServer, "auth_info", lambda self: dict(info)),
        patch.object(TabsdataServer, "auth_service_token", lambda self: token),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        result = CliRunner().invoke(
            cli, ["auth", "info", "-o", "json"], env=_token_env()
        )
    assert result.exit_code == 0, result.output
    shown = json.loads(result.stdout)
    assert shown["credential"] == "service_token"
    assert shown["slot"] == 1
    assert shown["hint"] == "abcd"
    assert "comment" not in shown


def test_auth_info_marks_a_session(tmp_path):
    info = {"name": "u", "email": "u@x", "roles": []}
    env = {"TDK_BEARER": _live(), "TDK_SERVER": SERVER}
    with (
        patch.object(TabsdataServer, "auth_info", lambda self: dict(info)),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        result = CliRunner().invoke(cli, ["auth", "info", "-o", "json"], env=env)
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["credential"] == "session"


def test_invalid_env_credential_still_allows_help(tmp_path):
    env = {"TDK_BEARER": "garbage", "TDK_SERVER": SERVER}
    result, _, _ = _run(["collection", "list", "--help"], env, tmp_path)
    assert result.exit_code == 0, result.output
    assert "Usage" in result.output


def test_invalid_env_credential_fails_the_first_server_call(tmp_path):
    env = {"TDK_BEARER": "garbage", "TDK_SERVER": SERVER, "TDK_PROJECT": "p"}
    result, connection, home = _run(
        ["collection", "get", "--name", "c1"], env, tmp_path
    )
    assert result.exit_code == ExitCode.USAGE, result.output
    assert "JWT" in result.output
    assert connection is None
    assert not home.exists()


def test_login_defaults_to_tdk_server(tmp_path):
    seen = {}

    def fake_login(ctx, server_url, user, password, token=None):
        seen["server"] = server_url

    with (
        patch("tabsdatak.cli.td.cli.utils_login", fake_login),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
        patch("tabsdatak.cli.td.config_utils.execute_actions_for_command"),
    ):
        result = CliRunner().invoke(
            cli,
            ["login", "--user", "u", "--password", "p"],
            env={"TDK_SERVER": "http://elsewhere:2457"},
        )
    assert result.exit_code == 0, result.output
    assert seen["server"] == "http://elsewhere:2457"

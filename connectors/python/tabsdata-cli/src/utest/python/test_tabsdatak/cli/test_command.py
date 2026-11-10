#
# Copyright 2026 Tabsdata Inc.
#

"""A tdk command settles its login, then its project, after reading what the
command line gives and before asking for anything."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli._errors import ExitCode
from tabsdatak.cli.td.cli import cli


def _logged_in(ctx):
    server = MagicMock()
    server.connection.project = None
    ctx.obj["tabsdataserver"] = server


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    with patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)):
        yield tmp_path


def test_a_logged_out_person_logs_in_before_being_asked_for_values(home):
    result = CliRunner().invoke(cli, ["table", "schema"], input="")
    assert "Server URL" in result.output
    assert "Name of the table" not in result.output


def test_without_a_login_or_prompts_the_login_is_what_is_missing(home):
    result = CliRunner().invoke(cli, ["--no-prompt", "table", "schema", "-o", "jsonb"])
    assert result.exit_code == ExitCode.AUTH
    assert json.loads(result.stdout)["error"]["type"] == "auth_required"


@pytest.mark.parametrize(
    "argv",
    [["table", "schema", "--name", "t", "--coll", "c"], ["plan", "list"]],
)
def test_a_command_asking_for_nothing_still_needs_the_login(argv, home):
    result = CliRunner().invoke(cli, ["--no-prompt", *argv, "-o", "jsonb"])
    assert result.exit_code == ExitCode.AUTH
    assert json.loads(result.stdout)["error"]["type"] == "auth_required"


def test_an_unexpected_argument_is_reported_before_the_login(home):
    result = CliRunner().invoke(
        cli, ["--no-prompt", "plan", "list", "extra", "-o", "jsonb"]
    )
    assert result.exit_code == ExitCode.USAGE
    assert "unexpected extra argument" in json.loads(result.stdout)["error"]["message"]


@pytest.mark.parametrize(
    "argv", [["plan", "get", "--plan", "x"], ["worker", "logs", "--worker", "x"]]
)
def test_a_malformed_id_is_reported_before_the_login(argv, home):
    result = CliRunner().invoke(cli, ["--no-prompt", *argv, "-o", "jsonb"])
    assert result.exit_code == ExitCode.USAGE
    assert "is not an id" in json.loads(result.stdout)["error"]["message"]


@pytest.mark.parametrize("argv", [["plan", "list"], ["fnrun", "list"], ["worker", "list"]])
def test_a_listing_across_projects_asks_for_no_project(argv, home):
    with patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _logged_in):
        result = CliRunner().invoke(cli, argv, input="")
    assert "Name of the project to target" not in result.output


def test_a_project_command_asks_for_the_project_first(home):
    with patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _logged_in):
        result = CliRunner().invoke(cli, ["table", "schema"], input="")
    assert result.output.index("Name of the project to target") < len(result.output)
    assert "Name of the table" not in result.output.split("Name of the project")[0]


def test_a_command_without_a_login_settles_nothing(home):
    with patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _logged_in):
        result = CliRunner().invoke(cli, ["fn", "check", "--path", "x.py::f"], input="")
    assert "Name of the project to target" not in result.output

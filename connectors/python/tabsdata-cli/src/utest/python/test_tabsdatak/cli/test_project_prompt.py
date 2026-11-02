#
#  Copyright 2026 Tabsdata Inc.
#

"""The project comes first.

Project-scoped commands address a collection / function / table *within* a
project, so when nothing selected one -- no pin, no ``--proj`` -- the project is
prompted for before any other value. Otherwise the command asks for a
collection only to fail afterwards on the missing project.

Same seams as the other mocked-client harnesses: the injected client's
``connection.project`` is left ``None`` to model "no project selected", and
prompting runs off ``CliRunner``'s stdin (not a tty, so ``logical_prompt``
takes the ``click.prompt`` path).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli import cli_utils
from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer

PROJECT_PROMPT = "Name of the project to target"
COLLECTION_PROMPT = "Name of the collection to which the functions belong"
FUNCTION_COLLECTION_PROMPT = "Name of the collection to which the function belongs"


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    # No pin and no --proj: the connection carries no project.
    client.connection.project = None
    return client


def _invoke(argv: list[str], *, client: MagicMock, tmp_path, input: str | None = None):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv, input=input)


def test_project_is_prompted_before_the_collection(tmp_path):
    client = _client()
    result = _invoke(
        ["fn", "get", "--name", "f1"], client=client, tmp_path=tmp_path, input="p1\nc1\n"
    )
    assert result.exit_code == 0, result.output
    assert result.output.index(PROJECT_PROMPT) < result.output.index(FUNCTION_COLLECTION_PROMPT)
    assert client.connection.project == "p1"
    client.get_function.assert_called_once_with("c1", "f1")


def test_a_listing_without_a_collection_spans_the_project(tmp_path):
    client = _client()
    result = _invoke(["fn", "list"], client=client, tmp_path=tmp_path, input="p1\n")
    assert result.exit_code == 0, result.output
    assert COLLECTION_PROMPT not in result.output
    client.list_project_functions_generator.assert_called_once_with(
        filter=None, order_by=None, cursor=None
    )


def test_project_is_prompted_even_when_the_collection_is_given(tmp_path):
    client = _client()
    result = _invoke(
        ["fn", "list", "--coll", "c1"], client=client, tmp_path=tmp_path, input="p1\n"
    )
    assert result.exit_code == 0, result.output
    assert PROJECT_PROMPT in result.output
    assert client.connection.project == "p1"


def test_no_prompt_when_proj_is_given(tmp_path):
    client = _client()
    result = _invoke(
        ["fn", "list", "--proj", "p1", "--coll", "c1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    assert PROJECT_PROMPT not in result.output
    assert client.connection.project == "p1"


def test_no_prompt_with_prompting_disabled(tmp_path):
    """``--no-prompt`` leaves the project unset, so the request fails with the
    SDK's own message, which names both ways to supply a project."""
    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import NoProjectSelectedError

    client = _client()
    client.list_functions_generator.side_effect = NoProjectSelectedError()
    result = _invoke(
        ["--no-prompt", "fn", "list", "--coll", "c1"], client=client, tmp_path=tmp_path
    )
    assert result.exit_code != 0
    assert PROJECT_PROMPT not in result.output
    assert "No project selected" in result.output


def test_no_prompt_for_a_cross_project_listing(tmp_path):
    """``collection list`` spans every visible project when none is selected --
    it offers ``--all-projects`` precisely because it needs no project."""
    client = _client()
    result = _invoke(["collection", "list"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert PROJECT_PROMPT not in result.output
    assert client.connection.project is None


@pytest.mark.parametrize("argv", [["user", "list"], ["project", "list"]])
def test_no_prompt_for_a_command_outside_a_project(argv, tmp_path):
    client = _client()
    result = _invoke(argv, client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert PROJECT_PROMPT not in result.output


def test_proj_survives_an_interactive_login(tmp_path):
    """``--proj`` is parsed before the login builds a connection to put it on,
    so it is applied to the one login creates."""
    client = _client()

    def _no_credentials(ctx):
        ctx.obj["tabsdataserver"] = None

    def _inject_after_login(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch(
            "tabsdatak.cli.td.cli.initialise_tabsdata_server_connection",
            _no_credentials,
        ),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
        patch.object(
            cli_utils, "request_login_information", lambda ctx: ("url", "u", "p")
        ),
        patch.object(cli_utils, "utils_login", lambda *args: None),
        patch.object(
            cli_utils, "initialise_tabsdata_server_connection", _inject_after_login
        ),
    ):
        result = CliRunner().invoke(cli, ["fn", "list", "--proj", "p1", "--coll", "c1"])

    assert result.exit_code == 0, result.output
    assert client.connection.project == "p1"


@pytest.mark.parametrize("error", [EOFError, KeyboardInterrupt])
def test_aborted_tty_prompt_raises_abort(error, monkeypatch):
    import rich_click as click

    monkeypatch.setattr("sys.stdin", MagicMock(isatty=lambda: True))
    monkeypatch.setattr(cli_utils, "prompt_with_escape", MagicMock(side_effect=error))
    ctx = click.Context(click.Command("cmd"), obj={"no_prompt": False})
    with pytest.raises(click.Abort):
        cli_utils.logical_prompt(ctx, PROJECT_PROMPT)


@pytest.mark.parametrize("error", [EOFError, KeyboardInterrupt])
def test_aborted_prompt_exits_non_zero(error, tmp_path):
    client = _client()
    with patch.object(cli_utils.click, "prompt", side_effect=error):
        result = _invoke(["fn", "list"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 1, result.output
    assert "Aborted" in result.stderr
    client.list_functions_generator.assert_not_called()

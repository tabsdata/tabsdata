#
#  Copyright 2026 Tabsdata Inc.
#

"""Session ``tdk`` commands (``logout``, ``status``) and the persistent settings
(``tdk set`` / ``tdk unset`` / ``tdk settings``).

``login`` / ``shell`` are excluded: they drive real network auth and an
interactive prompt loop respectively. Same mock-client + tmp-dir seam as the
render / mutating harnesses; the settings commands are fully offline (they only
read/write ``options.json`` in the tabsdata dir).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    return client


def _invoke(argv, *, client, tmp_path):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with patch(
        "tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject
    ), patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)):
        return CliRunner().invoke(cli, argv)


def _saved(tmp_path) -> dict:
    return json.loads((tmp_path / "options.json").read_text())


# --- set/unset --hints/--prompt: persist the values to options.json ---------


@pytest.mark.parametrize(
    "option,key,value,stored,message",
    [
        ("--hints", "hints", "true", "enabled", "Hints enabled"),
        ("--hints", "hints", "false", "disabled", "Hints disabled"),
        ("--prompt", "prompt", "true", "enabled", "Prompting enabled"),
        ("--prompt", "prompt", "false", "disabled", "Prompting disabled"),
    ],
)
def test_set_persists_the_value(option, key, value, stored, message, tmp_path):
    result = _invoke(["set", option, value], client=_client(), tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert message in result.output
    assert _saved(tmp_path)[key] == stored


@pytest.mark.parametrize("option,key", [("--hints", "hints"), ("--prompt", "prompt")])
def test_unset_removes_the_stored_value(option, key, tmp_path):
    # Unsetting drops the key entirely, so the shipped default applies again --
    # it does not store the opposite value.
    _invoke(["set", option, "false"], client=_client(), tmp_path=tmp_path)
    assert _saved(tmp_path)[key] == "disabled"

    result = _invoke(["unset", option], client=_client(), tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "back to its default" in result.output
    assert key not in _saved(tmp_path)


@pytest.mark.parametrize("option", ["--hints", "--prompt"])
def test_unset_says_when_it_was_already_default(option, tmp_path):
    result = _invoke(["unset", option], client=_client(), tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "already at its default" in result.output


@pytest.mark.parametrize("option", ["--hints", "--prompt"])
def test_set_requires_a_value(option, tmp_path):
    result = _invoke(["set", option], client=_client(), tmp_path=tmp_path)
    assert result.exit_code != 0
    assert "requires an argument" in result.output


def test_several_settings_in_one_command(tmp_path):
    result = _invoke(
        ["set", "--hints", "false", "--server", "http://srv:2457", "--proj", "sales"],
        client=_client(),
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    saved = _saved(tmp_path)
    assert saved["hints"] == "disabled"
    assert saved["server"] == "http://srv:2457"
    assert saved["pins"]["project"] == "sales"


def test_unset_all_leaves_every_setting_at_its_default(tmp_path):
    _invoke(
        [
            "set", "--hints", "false", "--prompt", "false",
            "--server", "http://srv:2457", "--proj", "sales", "--coll", "orders",
        ],
        client=_client(),
        tmp_path=tmp_path,
    )
    result = _invoke(["unset", "--all"], client=_client(), tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    # Nothing stored but the (emptied) pins: every setting is back to default.
    assert _saved(tmp_path) == {"pins": {"project": None, "collection": None}}


@pytest.mark.parametrize(
    "command,message",
    [("set", "Nothing to set"), ("unset", "Nothing to unset")],
)
def test_no_setting_is_a_usage_error(command, message, tmp_path):
    # A bare command has nothing to do; it must not silently pass.
    result = _invoke([command], client=_client(), tmp_path=tmp_path)
    assert result.exit_code != 0
    assert message in result.output
    assert not (tmp_path / "options.json").exists()


# --- set --prompt false: prompting is off for every later command -----------


def test_prompt_false_fails_instead_of_prompting(tmp_path):
    assert (
        _invoke(
            ["set", "--prompt", "false"], client=_client(), tmp_path=tmp_path
        ).exit_code
        == 0
    )
    # ``collection create`` prompts for the name it was not given; with the
    # setting off it fails and names the setting to flip back.
    result = _invoke(["collection", "create"], client=_client(), tmp_path=tmp_path)
    assert result.exit_code != 0
    assert "Prompting is disabled by the 'prompt' setting" in result.output
    assert "tdk set --prompt true" in result.output


# --- set/unset --server: the URL `tdk login` defaults to --------------------


def test_server_set_and_unset(tmp_path):
    result = _invoke(
        ["set", "--server", "http://srv:2457"],
        client=_client(),
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert _saved(tmp_path)["server"] == "http://srv:2457"

    result = _invoke(["unset", "--server"], client=_client(), tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "http://srv:2457" in result.output
    assert "server" not in _saved(tmp_path)


def test_login_defaults_to_the_configured_server(tmp_path):
    _invoke(
        ["set", "--server", "http://cfg:2457"],
        client=_client(),
        tmp_path=tmp_path,
    )
    with patch("tabsdatak.cli.td.cli.utils_login") as login, patch(
        "tabsdatak.cli.td.config_utils.execute_actions_for_command"
    ):
        result = _invoke(
            ["login", "--user", "u", "--password", "p"],
            client=_client(),
            tmp_path=tmp_path,
        )
    assert result.exit_code == 0, result.output
    # No --server given, so the configured one is used without prompting.
    assert login.call_args.args[1:] == ("http://cfg:2457", "u", "p")


# --- settings: current settings, in every output format ----------------------


def test_settings_reports_current_settings(tmp_path):
    _invoke(["set", "--hints", "false"], client=_client(), tmp_path=tmp_path)
    _invoke(
        ["set", "--server", "http://srv:2457"],
        client=_client(),
        tmp_path=tmp_path,
    )
    _invoke(
        ["set", "--proj", "sales", "--coll", "orders"],
        client=_client(),
        tmp_path=tmp_path,
    )

    result = _invoke(["settings"], client=_client(), tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    # The view uses the same words the option takes, not on/off.
    assert "false" in result.output  # hints
    assert "http://srv:2457" in result.output
    assert "sales" in result.output and "orders" in result.output

    result = _invoke(["settings", "-o", "json"], client=_client(), tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "hints": "disabled",
        "prompt": "enabled",
        "server": "http://srv:2457",
        "pins": {"project": "sales", "collection": "orders"},
    }


# --- logout: calls the SDK, then clears local credentials + pins ------------


def test_logout_calls_sdk_and_clears_local_state(tmp_path):
    (tmp_path / "connection.json").write_text(json.dumps({"url": "http://x"}))
    (tmp_path / "options.json").write_text(
        json.dumps({"pins": {"project": "p", "collection": "c"}})
    )
    client = _client()

    result = _invoke(["logout"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "Logout successful" in result.output
    client.logout.assert_called_once_with()
    # The stored credentials are removed and the pins are cleared.
    assert not (tmp_path / "connection.json").exists()
    assert _saved(tmp_path)["pins"] == {}


def test_logout_surfaces_sdk_error_but_still_clears_credentials(tmp_path):
    (tmp_path / "connection.json").write_text(json.dumps({"url": "http://x"}))
    client = _client()
    client.logout.side_effect = RuntimeError("boom-xyz")

    result = _invoke(["logout"], client=client, tmp_path=tmp_path)
    # A failed server logout is a clean CLI error, not a traceback...
    assert result.exit_code != 0
    assert "boom-xyz" in result.output
    # ...but the local credentials are still removed (the finally block).
    assert not (tmp_path / "connection.json").exists()


# --- status: prints the server status -----------------------------------


def test_status_prints_server_status(tmp_path):
    client = _client()
    client.status = "SERVER-OK"  # `status` is a property on the real SDK
    result = _invoke(["status"], client=client, tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "Obtaining server status" in result.output
    assert "SERVER-OK" in result.output

#
# Copyright 2026 Tabsdata Inc.
#

"""Profiles: a per-command ``--profile NAME`` that runs a command under
``<NAME>-connection.json`` / ``<NAME>-options.json``, and the default profile
``tdk set --profile`` / ``tdk unset --profile`` keep in ``profile.json``.

Same mock-client + tmp-dir seam as the other harnesses. The injected
connection initialiser also records which connection file each activation
resolved, which is how the tests see the profile a command ran under without a
server.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    return client


def _invoke(argv, *, tmp_path, client=None):
    connection_files = []

    def _inject(ctx):
        connection_files.append(ctx.obj["connection_file"])
        ctx.obj["tabsdataserver"] = client or _client()

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        result = CliRunner().invoke(cli, argv)
    return result, connection_files


def _json(path) -> dict:
    return json.loads(path.read_text())


def _leaf_commands(command, path=()):
    if isinstance(command, click.Group):
        for name, sub in command.commands.items():
            yield from _leaf_commands(sub, path + (name,))
    else:
        yield path, command


def _profile_param(command):
    return next((p for p in command.params if p.name == "profile"), None)


# --- every command takes --profile --------------------------------------------


def test_every_command_takes_profile():
    for path, command in _leaf_commands(cli):
        param = _profile_param(command)
        assert param is not None, f"tdk {' '.join(path)} lacks --profile"
        assert "--profile" in param.opts, path
        if path in (("set",), ("unset",)):
            continue
        # The shared option selects the profile before the command runs and
        # ahead of the options that act on the loaded connection.
        assert param.expose_value is False, path
        assert param.is_eager, path


def test_set_and_unset_manage_the_default_profile_instead():
    # `tdk set --profile NAME` takes a name; `tdk unset --profile` is a flag.
    assert not _profile_param(cli.commands["set"]).is_flag
    assert _profile_param(cli.commands["unset"]).is_flag


# --- selecting a profile for one invocation -----------------------------------


def test_default_profile_is_the_bare_files(tmp_path):
    result, connection_files = _invoke(["settings", "-o", "json"], tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert connection_files == [str(tmp_path / "connection.json")]
    assert json.loads(result.output)["profile"] is None


def test_profile_option_selects_the_profile_files(tmp_path):
    (tmp_path / "options.json").write_text(json.dumps({"hints": "enabled"}))
    (tmp_path / "qa-options.json").write_text(json.dumps({"hints": "disabled"}))

    result, connection_files = _invoke(
        ["settings", "--profile", "qa", "-o", "json"], tmp_path=tmp_path
    )
    assert result.exit_code == 0, result.output
    # The root group loads the default profile; the option then re-resolves
    # everything under the named one.
    assert connection_files == [
        str(tmp_path / "connection.json"),
        str(tmp_path / "qa-connection.json"),
    ]
    reported = json.loads(result.output)
    assert reported["profile"] == "qa"
    assert reported["hints"] == "disabled"


def test_profile_option_is_reported_in_the_table_view(tmp_path):
    result, _ = _invoke(["settings", "--profile", "qa"], tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "Profile:           qa" in result.output

    result, _ = _invoke(["settings"], tmp_path=tmp_path)
    assert "Profile:           (default)" in result.output


@pytest.mark.parametrize("name", ["../x", "a/b", "", "-dev", "d e"])
def test_profile_option_rejects_bad_names(name, tmp_path):
    result, _ = _invoke(["settings", "--profile", name], tmp_path=tmp_path)
    assert result.exit_code == 2
    assert "not a valid profile name" in result.output


def test_logout_removes_the_profile_connection_only(tmp_path):
    (tmp_path / "connection.json").write_text(json.dumps({"url": "http://d"}))
    (tmp_path / "qa-connection.json").write_text(json.dumps({"url": "http://q"}))
    (tmp_path / "qa-options.json").write_text(
        json.dumps({"pins": {"project": "p", "collection": "c"}})
    )

    result, _ = _invoke(["logout", "--profile", "qa"], tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert not (tmp_path / "qa-connection.json").exists()
    assert (tmp_path / "connection.json").exists()
    assert _json(tmp_path / "qa-options.json")["pins"] == {}
    assert not (tmp_path / "options.json").exists()


def test_login_runs_under_the_profile(tmp_path):
    with (
        patch("tabsdatak.cli.td.cli.utils_login") as login,
        patch("tabsdatak.cli.td.config_utils.execute_actions_for_command"),
    ):
        result, connection_files = _invoke(
            [
                "login",
                "--profile",
                "qa",
                "--server",
                "s",
                "--user",
                "u",
                "--password",
                "p",
            ],
            tmp_path=tmp_path,
        )
    assert result.exit_code == 0, result.output
    assert connection_files[-1] == str(tmp_path / "qa-connection.json")
    assert login.call_args.args[0].obj["connection_file"] == str(
        tmp_path / "qa-connection.json"
    )


# --- tdk set --profile / tdk unset --profile: the default profile -------------


def test_set_profile_records_the_default(tmp_path):
    result, _ = _invoke(["set", "--profile", "dev"], tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "Default profile set to dev" in result.output
    assert _json(tmp_path / "profile.json") == {"profile": "dev"}
    # Recording the default alone stores nothing else.
    assert not (tmp_path / "dev-options.json").exists()
    assert not (tmp_path / "options.json").exists()


def test_default_profile_applies_to_later_commands(tmp_path):
    _invoke(["set", "--profile", "dev"], tmp_path=tmp_path)

    result, connection_files = _invoke(["set", "--hints", "false"], tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert connection_files == [str(tmp_path / "dev-connection.json")]
    assert _json(tmp_path / "dev-options.json")["hints"] == "disabled"
    assert not (tmp_path / "options.json").exists()

    result, _ = _invoke(["settings", "-o", "json"], tmp_path=tmp_path)
    assert json.loads(result.output)["profile"] == "dev"


def test_set_profile_stores_the_other_settings_in_that_profile(tmp_path):
    result, _ = _invoke(
        ["set", "--profile", "dev", "--server", "http://dev:2457", "--proj", "sales"],
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    saved = _json(tmp_path / "dev-options.json")
    assert saved["server"] == "http://dev:2457"
    assert saved["pins"]["project"] == "sales"
    assert not (tmp_path / "options.json").exists()


def test_set_profile_rejects_bad_names(tmp_path):
    result, _ = _invoke(["set", "--profile", "a/b"], tmp_path=tmp_path)
    assert result.exit_code == 2
    assert "not a valid profile name" in result.output
    assert not (tmp_path / "profile.json").exists()


def test_unset_profile_drops_the_default(tmp_path):
    _invoke(["set", "--profile", "dev"], tmp_path=tmp_path)

    result, _ = _invoke(["unset", "--profile"], tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "Default profile unset (was dev)" in result.output
    assert not (tmp_path / "profile.json").exists()

    result, connection_files = _invoke(["settings", "-o", "json"], tmp_path=tmp_path)
    assert connection_files == [str(tmp_path / "connection.json")]
    assert json.loads(result.output)["profile"] is None


def test_unset_profile_says_when_none_was_set(tmp_path):
    result, _ = _invoke(["unset", "--profile"], tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert "No default profile was set" in result.output


def test_unset_all_leaves_the_default_profile(tmp_path):
    _invoke(["set", "--profile", "dev", "--hints", "false"], tmp_path=tmp_path)

    result, _ = _invoke(["unset", "--all"], tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert _json(tmp_path / "profile.json") == {"profile": "dev"}
    assert "hints" not in _json(tmp_path / "dev-options.json")


def test_unset_profile_alone_stores_no_settings(tmp_path):
    result, _ = _invoke(["unset", "--profile"], tmp_path=tmp_path)
    assert result.exit_code == 0, result.output
    assert not (tmp_path / "options.json").exists()

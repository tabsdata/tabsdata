#
# Copyright 2026 Tabsdata Inc.
#

"""The ``tdk`` CLI refuses to run inside a supported AI agent.

Any of ``GEMINI_CLI`` / ``CLAUDECODE`` / ``CODEX_THREAD_ID`` being set marks an
agent shell; tdk then exits and points the user at the Tabsdata MCP server. The
guard runs before argument parsing, so ``--help`` / ``--version`` are gated too.
The ``TDK_IN_AGENT`` escape hatch downgrades the failure to a warning."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import (
    _AI_AGENT_ERROR_MESSAGE,
    cli,
    detect_ai_agent,
    enforce_not_in_ai_agent,
)


def _result_text(result) -> str:
    out = result.output or ""
    try:
        err = result.stderr or ""
    except ValueError:  # stderr not captured separately
        err = ""
    return out + err


def test_error_message_wording():
    assert "does not support running within an AI agent" in _AI_AGENT_ERROR_MESSAGE
    assert "Tabsdata MCP" in _AI_AGENT_ERROR_MESSAGE


# ---------------------------------------------------------------------------
# detect_ai_agent
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "env,expected",
    [
        ({"GEMINI_CLI": "1"}, "GEMINI_CLI"),
        ({"CLAUDECODE": "1"}, "CLAUDECODE"),
        ({"CODEX_THREAD_ID": "abc-123"}, "CODEX_THREAD_ID"),
        ({}, None),
        ({"GEMINI_CLI": ""}, None),  # present but empty does not count
        ({"CLAUDECODE": "   "}, None),  # whitespace-only does not count
    ],
)
def test_detect_ai_agent(env, expected):
    assert detect_ai_agent(env) == expected


def test_detect_ai_agent_returns_first_match_in_order():
    env = {"CODEX_THREAD_ID": "x", "CLAUDECODE": "1", "GEMINI_CLI": "1"}
    assert detect_ai_agent(env) == "GEMINI_CLI"


# ---------------------------------------------------------------------------
# enforce_not_in_ai_agent (unit)
# ---------------------------------------------------------------------------


def test_enforce_noop_outside_agent(capsys):
    # Returns without exiting and prints nothing.
    assert enforce_not_in_ai_agent({}) is None
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize("var", ["GEMINI_CLI", "CLAUDECODE", "CODEX_THREAD_ID"])
def test_enforce_exits_inside_agent(var, capsys):
    with pytest.raises(SystemExit) as exc:
        enforce_not_in_ai_agent({var: "1"})
    assert exc.value.code == 1
    assert _AI_AGENT_ERROR_MESSAGE in capsys.readouterr().err


@pytest.mark.parametrize("bypass", ["1", "true", "TRUE", "yes", "Yes", " yes "])
def test_enforce_bypass_warns_and_continues(bypass, capsys):
    # With the escape hatch set truthy, the guard warns instead of exiting.
    assert enforce_not_in_ai_agent({"CLAUDECODE": "1", "TDK_IN_AGENT": bypass}) is None
    captured = capsys.readouterr()
    assert "TDK_IN_AGENT" in captured.err
    assert "Tabsdata MCP" in captured.err
    assert captured.out == ""


@pytest.mark.parametrize("bypass", ["0", "false", "no", "", "maybe"])
def test_enforce_non_truthy_bypass_still_exits(bypass):
    with pytest.raises(SystemExit):
        enforce_not_in_ai_agent({"CLAUDECODE": "1", "TDK_IN_AGENT": bypass})


# ---------------------------------------------------------------------------
# End-to-end through the click entry point (guard runs before parsing)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "env",
    [
        {"GEMINI_CLI": "1"},
        {"CLAUDECODE": "1"},
        {"CODEX_THREAD_ID": "thread-xyz"},
    ],
)
@pytest.mark.parametrize("args", [["pin", "show"], ["--help"], ["--version"]])
def test_cli_blocks_when_run_in_agent(env, args):
    # Even --help / --version are gated: the guard runs before click parses args.
    result = CliRunner().invoke(cli, args, env=env)
    assert result.exit_code != 0
    assert _AI_AGENT_ERROR_MESSAGE in _result_text(result)


def test_cli_proceeds_with_bypass():
    result = CliRunner().invoke(
        cli, ["--help"], env={"CLAUDECODE": "1", "TDK_IN_AGENT": "1"}
    )
    assert result.exit_code == 0, _result_text(result)
    assert _AI_AGENT_ERROR_MESSAGE not in _result_text(result)
    assert "TDK_IN_AGENT" in _result_text(result)  # the bypass warning


def test_cli_bypass_runs_a_real_command(tmp_path):
    with (
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection"),
    ):
        result = CliRunner().invoke(
            cli, ["pin", "show"], env={"CODEX_THREAD_ID": "t1", "TDK_IN_AGENT": "yes"}
        )
    assert result.exit_code == 0, _result_text(result)
    assert "TDK_IN_AGENT" in _result_text(result)


def test_cli_unaffected_outside_agent():
    # Sanity: with a clean env (see conftest autouse) the CLI is unaffected.
    result = CliRunner().invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert _AI_AGENT_ERROR_MESSAGE not in _result_text(result)

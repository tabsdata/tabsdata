#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from tabsdatak.cli._unattended import (
    UNATTENDED_MARKERS,
    coding_agent,
    unattended_cause,
)

_OS_ONLY = (
    "import sys\n"
    "from tabsdatak.cli import _registry\n"
    "_registry._CLI_EXTENSIONS.clear()\n"
    "_registry._VARIANT_CLI_COMMANDS = 'tabsdatak_no_such_variant.cli.commands'\n"
    "from tabsdatak.cli.td.cli import cli\n"
    "cli.main(sys.argv[1:], prog_name='tdk')\n"
)


@pytest.mark.parametrize(
    "env, cause",
    [
        ({}, None),
        ({"TDK_AGENT": "1"}, "TDK_AGENT=1"),
        ({"TDK_AGENT": "0"}, None),
        ({"TDK_AGENT": "0", "CLAUDECODE": "1"}, "CLAUDECODE is set"),
        ({"CLAUDECODE": "1"}, "CLAUDECODE is set"),
        ({"GEMINI_CLI": "1"}, "GEMINI_CLI is set"),
        ({"CODEX_THREAD_ID": "t-1"}, "CODEX_THREAD_ID is set"),
        ({"CURSOR_AGENT": "1"}, "CURSOR_AGENT is set"),
        ({"VSCODE_AGENT": "1"}, "VSCODE_AGENT is set"),
        ({"OPENCODE": "1"}, "OPENCODE is set"),
        ({"ANTIGRAVITY_AGENT": "1"}, "ANTIGRAVITY_AGENT is set"),
        ({"COPILOT_CLI": "1"}, "COPILOT_CLI is set"),
        ({"CLINE_ACTIVE": "true"}, "CLINE_ACTIVE is set"),
        ({"AI_AGENT": "claude-code_2-1-322_agent"}, "AI_AGENT is set"),
        ({"AGENT": "goose"}, "AGENT is set"),
        (
            {"CLAUDECODE": "0", "AI_AGENT": "claude-code_2-1-322_agent"},
            "AI_AGENT is set",
        ),
        ({"CLAUDECODE": " "}, None),
        ({"CLAUDECODE": "0"}, None),
        ({"CLAUDECODE": "False"}, None),
        ({"CLAUDECODE": "false", "OPENCODE": "1"}, "OPENCODE is set"),
    ],
)
def test_unattended_cause_reads_the_environment(env, cause):
    assert unattended_cause(env) == cause


@pytest.mark.parametrize(
    "env, agent",
    [
        ({}, None),
        ({"TDK_AGENT": "1"}, None),
        ({"TDK_AGENT": "1", "CURSOR_AGENT": "1"}, "CURSOR_AGENT"),
        ({"VSCODE_AGENT": "1"}, "VSCODE_AGENT"),
        ({"CLAUDECODE": " "}, None),
    ],
)
def test_coding_agent_names_only_a_host_marker(env, agent):
    assert coding_agent(env) == agent


def _user_env(home, **extra) -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("TDK_")
        and key
        not in (*UNATTENDED_MARKERS, "FORCE_COLOR", "PY_COLORS", "GITHUB_ACTIONS")
    }
    env.update({
        "HOME": str(home),
        "USERPROFILE": str(home),
        "COLUMNS": "250",
        "PYTHONIOENCODING": "utf-8",
        **extra,
    })
    return env


def test_an_unattended_session_never_waits_on_open_stdin(tmp_path):
    env = _user_env(tmp_path, CLAUDECODE="1")
    process = subprocess.Popen(
        [sys.executable, "-c", _OS_ONLY, "login"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        encoding="utf-8",
    )
    try:
        code = process.wait(timeout=60)
    except subprocess.TimeoutExpired:
        process.kill()
        pytest.fail("tdk prompted on an open stdin in an unattended session.")
    finally:
        process.stdin.close()
    stderr = process.stderr.read()
    assert code != 0
    assert "unattended session (CLAUDECODE is set)" in stderr
    assert "Missing required value: --server." in stderr
    assert "Pass --server." in stderr


def test_a_closed_stdin_names_the_flag_instead_of_aborting(tmp_path):
    done = subprocess.run(
        [sys.executable, "-c", _OS_ONLY, "login"],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        env=_user_env(tmp_path),
        encoding="utf-8",
        timeout=60,
    )
    assert done.returncode == 2, done.stderr
    assert "Missing required value: --server." in done.stderr
    assert "Standard input is not a terminal" in done.stderr
    assert "Pass --server." in done.stderr
    assert "Aborted" not in done.stderr

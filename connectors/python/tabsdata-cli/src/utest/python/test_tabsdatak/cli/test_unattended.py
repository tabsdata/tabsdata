#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from tabsdatak.cli._unattended import unattended_cause

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
        ({"CLAUDECODE": " "}, None),
    ],
)
def test_unattended_cause_reads_the_environment(env, cause):
    assert unattended_cause(env) == cause


def test_an_unattended_session_never_waits_on_open_stdin(tmp_path):
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("TDK_") and key not in ("GEMINI_CLI", "CODEX_THREAD_ID")
    }
    env.update({"HOME": str(tmp_path), "CLAUDECODE": "1", "COLUMNS": "250"})
    process = subprocess.Popen(
        [sys.executable, "-c", _OS_ONLY, "login"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True,
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

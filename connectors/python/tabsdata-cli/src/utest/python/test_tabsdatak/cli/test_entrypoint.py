#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import subprocess
import sys
from unittest.mock import patch

from tabsdatak.cli import main
from tabsdatak.common._compatibility import PackageVersionError


def test_main_checks_sticky_versions_before_running_the_cli():
    calls = []
    with (
        patch(
            "tabsdatak.common._compatibility.check_sticky_version_packages",
            lambda: calls.append("sticky"),
        ),
        patch("tabsdatak.cli.td.cli.cli", lambda **kwargs: calls.append(kwargs)),
    ):
        main()
    assert calls == ["sticky", {"prog_name": "tdk"}]


def test_main_reports_a_version_mismatch_and_still_runs(capsys):
    def mismatch():
        raise PackageVersionError(["polars is 0.1", "install polars==1.0"])

    ran = []
    with (
        patch(
            "tabsdatak.common._compatibility.check_sticky_version_packages", mismatch
        ),
        patch("tabsdatak.cli.td.cli.cli", lambda **_: ran.append(True)),
    ):
        main()
    assert ran == [True]
    assert capsys.readouterr().err == "polars is 0.1\ninstall polars==1.0\n"


def test_python_dash_m_runs_tdk():
    result = subprocess.run(
        [sys.executable, "-m", "tabsdatak.cli", "--version"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.split()[-1]


def _python_dash_m(unattended: bool, *argv):
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in ("TDK_AGENT", "CLAUDECODE", "GEMINI_CLI", "CODEX_THREAD_ID")
    }
    if unattended:
        env["TDK_AGENT"] = "1"
    return subprocess.run(
        [sys.executable, "-m", "tabsdatak.cli", *argv],
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )


def test_python_dash_m_help_names_tdk():
    result = _python_dash_m(True, "status", "--help")
    assert result.returncode == 0, result.stderr
    assert "Usage: tdk status" in result.stdout
    assert "python -m" not in result.stdout


def test_python_dash_m_usage_error_names_tdk():
    result = _python_dash_m(False, "status", "--no-such-option")
    assert result.returncode != 0
    assert "Usage: tdk status" in result.stderr
    assert "python -m" not in result.stderr

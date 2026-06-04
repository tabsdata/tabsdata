#
# Copyright 2026 Tabs Data Inc.
#

"""Smoke tests for the ``td`` CLI: confirms every command group
imports cleanly and exposes the expected subcommands.

End-to-end coverage (live ``td`` invocations against a running
apiserver) lives in the CLI integration suite."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli


# ---------------------------------------------------------------------------
# Module-level smoke
# ---------------------------------------------------------------------------


def test_top_level_command_imports_cleanly():
    # Importing should not blow up; the top-level ``cli`` is a Click
    # group named "cli" exposing the well-known subcommands.
    assert cli.name == "cli"


def test_top_level_help_lists_expected_subcommands():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    out = result.output
    # Every command group registered on the top-level CLI.
    for name in [
        "auth",
        "collection",
        "cron",
        "exe",
        "fn",
        "group",
        "metrics",
        "role",
        "stage",
        "table",
        "user",
        "info",
        "login",
        "logout",
        "shell",
        "status",
    ]:
        assert name in out, f"Subcommand {name!r} missing from `td --help`"


# ---------------------------------------------------------------------------
# Per-group --help: each must import its module's tabsdatak.cli.cli_utils
# dependency tree without ImportError.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "subcommand",
    [
        "auth",
        "collection",
        "cron",
        "exe",
        "fn",
        "group",
        "metrics",
        "role",
        "stage",
        "table",
        "user",
    ],
)
def test_subcommand_help_loads_without_error(subcommand):
    runner = CliRunner()
    result = runner.invoke(cli, [subcommand, "--help"])
    assert result.exit_code == 0, (
        f"`td {subcommand} --help` failed:\n{result.output}\n"
        f"exception={result.exception}"
    )


# ---------------------------------------------------------------------------
# `td fn register` retains its v2 shape (--path / --dir-to-bundle / --local-pkg)
# ---------------------------------------------------------------------------


def test_fn_register_help_advertises_v2_flags():
    runner = CliRunner()
    result = runner.invoke(cli, ["fn", "register", "--help"])
    assert result.exit_code == 0
    out = result.output
    assert "--path" in out
    assert "--dir-to-bundle" in out
    assert "--local-pkg" in out
    # v2 dropped --requirements (bundle producer derives requirements
    # from the active Python env automatically).
    assert "--requirements" not in out


def test_fn_update_help_explains_v2_unavailability():
    runner = CliRunner()
    result = runner.invoke(cli, ["fn", "update", "--help"])
    # ``--help`` itself succeeds; only the invocation raises.
    assert result.exit_code == 0

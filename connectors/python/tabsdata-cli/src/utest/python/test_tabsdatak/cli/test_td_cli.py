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
# `td fn register` v2 surface (--path / --dir-to-bundle / --local-pkg /
# --requirements / --update)
# ---------------------------------------------------------------------------


def test_fn_register_help_advertises_v2_flags():
    runner = CliRunner()
    result = runner.invoke(cli, ["fn", "register", "--help"])
    assert result.exit_code == 0
    out = result.output
    assert "--path" in out
    assert "--dir-to-bundle" in out
    assert "--local-pkg" in out
    # ``--requirements`` is restored in v2: it now points to a
    # user-supplied requirements.yaml that the bundle embeds
    # verbatim. ``--update`` is also restored as an idempotent
    # register-or-update flag.
    assert "--requirements" in out
    assert "--update" in out


def test_fn_update_help_describes_v2_command():
    runner = CliRunner()
    result = runner.invoke(cli, ["fn", "update", "--help"])
    # ``--help`` itself succeeds.
    assert result.exit_code == 0

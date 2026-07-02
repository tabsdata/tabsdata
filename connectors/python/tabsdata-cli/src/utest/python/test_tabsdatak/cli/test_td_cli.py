#
# Copyright 2026 Tabs Data Inc.
#

"""Smoke tests for the ``td`` CLI: confirms every command group
imports cleanly and exposes the expected subcommands.

End-to-end coverage (live ``td`` invocations against a running
apiserver) lives in the CLI integration suite."""

from __future__ import annotations

import json
from unittest.mock import patch

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
        "pin",
        "project",
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
        "pin",
        "project",
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


# ---------------------------------------------------------------------------
# projects / multitenancy surface
# ---------------------------------------------------------------------------


def test_pin_group_exposes_expected_subcommands():
    pin = cli.commands.get("pin")
    assert pin is not None
    assert sorted(pin.commands) == ["set", "show", "unset"]


def test_project_group_has_crud_only():
    # pin/unpin/pinned moved to the unified `td pin` group.
    project = cli.commands.get("project")
    assert project is not None
    assert sorted(project.commands) == [
        "create",
        "delete",
        "info",
        "list",
        "update",
    ]


def test_root_has_no_project_option():
    # The active project is targeted per-command via `--proj`, never as a
    # global root option.
    opts = [o for p in cli.params for o in p.opts]
    assert "--project" not in opts and "-P" not in opts


def test_project_scoped_command_exposes_proj():
    # Project-scoped subcommands carry the per-command `--proj`/`-P`.
    collection = cli.commands.get("collection")
    list_cmd = collection.commands.get("list")
    opts = [o for p in list_cmd.params for o in p.opts]
    assert "--proj" in opts and "-P" in opts


class TestPinSurface:
    """`td pin set/unset/show` over the persisted pinned-objects store. A
    collection pin requires a pinned project; unpinning or switching the
    project clears the collection. ``td pin unset`` (or ``--proj``) clears
    both; ``--coll`` clears only the collection."""

    def _runner(self, tmp_path):
        return patch(
            "tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)
        ), patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection")

    def _pinned(self, tmp_path):
        return json.loads((tmp_path / "pinned.json").read_text())

    def test_set_with_no_flags_fails(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            result = CliRunner().invoke(cli, ["pin", "set"])
            assert result.exit_code != 0
            assert "Nothing to pin" in result.output

    def test_set_collection_blocked_without_pinned_project(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            result = CliRunner().invoke(cli, ["pin", "set", "--coll", "c1"])
            assert result.exit_code != 0
            assert "No project pinned" in result.output
            if (tmp_path / "pinned.json").exists():
                assert self._pinned(tmp_path).get("collection") is None

    def test_set_collection_allowed_with_pinned_project(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            assert runner.invoke(cli, ["pin", "set", "--proj", "gamma"]).exit_code == 0
            result = runner.invoke(cli, ["pin", "set", "--coll", "c1"])
            assert result.exit_code == 0, result.output
            assert self._pinned(tmp_path).get("collection") == "c1"

    def test_set_project_and_collection_together(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            result = CliRunner().invoke(
                cli, ["pin", "set", "--proj", "gamma", "--coll", "c1"]
            )
            assert result.exit_code == 0, result.output
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") == "gamma"
            assert pinned.get("collection") == "c1"

    def test_unset_coll_clears_only_collection(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            runner.invoke(cli, ["pin", "set", "--proj", "gamma", "--coll", "c1"])
            unset = runner.invoke(cli, ["pin", "unset", "--coll"])
            assert unset.exit_code == 0, unset.output
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") == "gamma"
            assert pinned.get("collection") is None

    def test_unset_proj_clears_both(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            runner.invoke(cli, ["pin", "set", "--proj", "gamma", "--coll", "c1"])
            unset = runner.invoke(cli, ["pin", "unset", "--proj"])
            assert unset.exit_code == 0, unset.output
            assert "c1" in unset.output  # cascade surfaced
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") is None
            assert pinned.get("collection") is None

    def test_bare_unset_clears_both(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            runner.invoke(cli, ["pin", "set", "--proj", "gamma", "--coll", "c1"])
            unset = runner.invoke(cli, ["pin", "unset"])
            assert unset.exit_code == 0, unset.output
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") is None
            assert pinned.get("collection") is None

    def test_switching_project_clears_collection(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            runner.invoke(cli, ["pin", "set", "--proj", "gamma", "--coll", "c1"])
            switched = runner.invoke(cli, ["pin", "set", "--proj", "delta"])
            assert switched.exit_code == 0, switched.output
            assert "c1" in switched.output  # cascade surfaced
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") == "delta"
            assert pinned.get("collection") is None

    def test_repinning_same_project_keeps_collection(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            runner.invoke(cli, ["pin", "set", "--proj", "gamma", "--coll", "c1"])
            again = runner.invoke(cli, ["pin", "set", "--proj", "gamma"])
            assert again.exit_code == 0, again.output
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") == "gamma"
            assert pinned.get("collection") == "c1"

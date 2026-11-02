#
# Copyright 2026 Tabsdata Inc.
#

"""Smoke tests for the ``td`` CLI: confirms every command group
imports cleanly and exposes the expected subcommands.

End-to-end coverage (live ``td`` invocations against a running
apiserver) lives in the CLI integration suite."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
import rich_click as click
from click.testing import CliRunner

from tabsdatak.cli.td import cli as cli_module
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
        "fn",
        "fnrun",
        "group",
        "metrics",
        "plan",
        "project",
        "set",
        "settings",
        "stream",
        "table",
        "trx",
        "unset",
        "user",
        "worker",
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
        "fn",
        "fnrun",
        "group",
        "metrics",
        "plan",
        "project",
        "set",
        "settings",
        "stream",
        "table",
        "trx",
        "unset",
        "user",
        "worker",
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
# `tdk fn register` surface (--path / --dir-to-bundle / --requirements /
# --local-pkg / --storage-path / --update / --dry-run).
# ---------------------------------------------------------------------------


def test_fn_register_help_advertises_flags():
    runner = CliRunner()
    result = runner.invoke(cli, ["fn", "register", "--help"])
    assert result.exit_code == 0
    out = result.output
    assert "--path" in out
    assert "--dir-to-bundle" in out
    assert "--update" in out
    assert "--storage-path" in out
    assert "--requirements" in out
    assert "--local-pkg" in out
    assert "--dry-run" in out
    assert "--base-path" not in out


def test_fn_update_help_describes_v2_command():
    runner = CliRunner()
    result = runner.invoke(cli, ["fn", "update", "--help"])
    # ``--help`` itself succeeds.
    assert result.exit_code == 0


def test_fn_bundle_help_advertises_options_and_archive_format():
    runner = CliRunner()
    result = runner.invoke(cli, ["fn", "bundle", "--help"])
    assert result.exit_code == 0
    out = result.output
    assert "--proj" in out
    assert "--fn-version" in out
    assert "--file" in out
    # The help must make clear the downloaded file is a zstd tar archive.
    assert "tar.zst" in out


# ---------------------------------------------------------------------------
# `tdk --help --full --format text|json`: recursive help over every command.
# ---------------------------------------------------------------------------


def test_full_help_text_lists_nested_commands():
    result = CliRunner().invoke(cli, ["--help", "--full", "--format", "text"])
    assert result.exit_code == 0, result.output
    out = result.output
    # Every group and a representative renamed subcommand is present.
    assert "tdk collection get" in out
    assert "tdk fn patches" in out
    assert "tdk fn bundle" in out


def test_full_help_defaults_to_text():
    result = CliRunner().invoke(cli, ["--help", "--full"])
    assert result.exit_code == 0, result.output
    assert "tdk collection get" in result.output


def test_full_help_requires_help_flag():
    # --full only applies together with --help; on its own it is not the
    # full-help path (Click falls through to a usage error).
    result = CliRunner().invoke(cli, ["--full"])
    assert result.exit_code != 0
    assert "tdk collection get" not in result.output


def test_full_help_json_is_a_command_tree():
    result = CliRunner().invoke(cli, ["--help", "--full", "--format", "json"])
    assert result.exit_code == 0, result.output
    tree = json.loads(result.output)
    assert tree["name"] == "tdk"
    top = {c["name"] for c in tree["commands"]}
    assert {"collection", "fn", "group", "project", "user"} <= top
    fn = next(c for c in tree["commands"] if c["name"] == "fn")
    fn_subs = {c["name"] for c in fn["commands"]}
    assert "get" in fn_subs and "info" not in fn_subs
    assert "patches" in fn_subs and "list-patches" not in fn_subs


def test_full_help_rejects_unknown_output():
    result = CliRunner().invoke(cli, ["--help", "--full", "--format", "yaml"])
    assert result.exit_code == 2


def test_full_help_marks_options_required_or_optional():
    # Text form tags every option; the tree carries the same as a boolean.
    text = CliRunner().invoke(cli, ["--help", "--full"]).output
    assert "(optional)" in text
    assert "(required)" in text

    tree = json.loads(
        CliRunner().invoke(cli, ["--help", "--full", "--format", "json"]).output
    )

    def _options(node):
        yield from node.get("options", [])
        for sub in node.get("commands", []):
            yield from _options(sub)

    required_values = {opt["required"] for opt in _options(tree)}
    # Both mandatory and optional options exist across the command tree.
    assert required_values == {True, False}


class _LazyStubGroup(click.RichGroup):
    def __init__(self, **kwargs):
        self.materialized = False
        super().__init__(**kwargs)

    def _real(self):
        self.materialized = True

        @click.group()
        def real():
            pass

        @real.command()
        def leaf():
            """A lazily loaded leaf."""

        return real

    def make_context(self, info_name, args, parent=None, **extra):
        return self._real().make_context(info_name, args, parent=parent, **extra)

    def get_command(self, ctx, cmd_name):
        return self._real().get_command(ctx, cmd_name)

    def list_commands(self, ctx):
        return self._real().list_commands(ctx)


def _root_with_lazy_stub():
    @click.group(cls=cli_module._RootGroup)
    def root():
        pass

    lazy = _LazyStubGroup(name="lazy")
    root.add_command(lazy)
    cli_module._attach_profile_option(root)
    return root, lazy


def test_profile_option_does_not_materialize_lazy_groups():
    _, lazy = _root_with_lazy_stub()
    assert lazy.materialized is False


def test_full_help_walks_lazy_groups_and_gives_them_profile():
    root, _ = _root_with_lazy_stub()
    tree = json.loads(cli_module.build_full_help(root, "json"))
    lazy = next(c for c in tree["commands"] if c["name"] == "lazy")
    leaf = next(c for c in lazy["commands"] if c["name"] == "leaf")
    assert leaf["path"] == "tdk lazy leaf"
    assert ["--profile"] in [opt["opts"] for opt in leaf["options"]]


def test_invoked_lazy_leaf_takes_profile():
    root, _ = _root_with_lazy_stub()
    result = CliRunner().invoke(root, ["lazy", "leaf", "--help"])
    assert result.exit_code == 0, result.output
    assert "--profile" in result.output


def test_non_tty_help_is_not_padded():
    result = CliRunner().invoke(cli, ["fn", "--help"])
    assert result.exit_code == 0, result.output
    assert max(len(line) for line in result.output.splitlines()) <= 100


# ---------------------------------------------------------------------------
# projects / multitenancy surface
# ---------------------------------------------------------------------------


def test_settings_commands_expose_every_setting():
    # One flag per setting on both `tdk set` and `tdk unset`: the flag settings
    # (hints, prompt) and the value ones (server, project, collection).
    for name in ("set", "unset"):
        command = cli.commands.get(name)
        assert command is not None, f"`tdk {name}` missing"
        opts = {o for p in command.params for o in p.opts}
        assert {
            "--hints",
            "--prompt",
            "--server",
            "--proj",
            "--coll",
            "--profile",
        } <= opts

    settings = cli.commands.get("settings")
    assert settings is not None
    assert "--format" in {o for p in settings.params for o in p.opts}


def test_config_pin_and_hints_are_not_commands():
    # The settings live on `tdk set` / `tdk unset` / `tdk settings`; there is no
    # `tdk config` group and no top-level `pin` / `hints`.
    for name in ("config", "pin", "hints"):
        assert cli.commands.get(name) is None


def test_project_group_has_crud_secrets_graph_topology_and_tags_only():
    # pin/unpin/pinned moved to `tdk set --proj`.
    project = cli.commands.get("project")
    assert project is not None
    assert sorted(project.commands) == [
        "create",
        "delete",
        "get",
        "graph",
        "list",
        "secrets",
        "tag",
        "topology",
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
    """`tdk set --proj/--coll` over the persisted settings store. A collection
    pin requires a pinned project; unpinning or switching the project clears the
    collection. ``tdk unset --proj`` clears both; ``tdk unset --coll`` clears
    only the collection."""

    def _runner(self, tmp_path):
        return patch(
            "tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)
        ), patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection")

    def _pinned(self, tmp_path):
        return json.loads((tmp_path / "options.json").read_text())["pins"]

    def test_set_with_no_flags_fails(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            result = CliRunner().invoke(cli, ["set"])
            assert result.exit_code != 0
            assert "Nothing to set" in result.output

    def test_set_collection_blocked_without_pinned_project(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            result = CliRunner().invoke(cli, ["set", "--coll", "c1"])
            assert result.exit_code != 0
            assert "No project pinned" in result.output
            if (tmp_path / "options.json").exists():
                assert self._pinned(tmp_path).get("collection") is None

    def test_set_collection_allowed_with_pinned_project(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            assert runner.invoke(cli, ["set", "--proj", "gamma"]).exit_code == 0
            result = runner.invoke(cli, ["set", "--coll", "c1"])
            assert result.exit_code == 0, result.output
            assert self._pinned(tmp_path).get("collection") == "c1"

    def test_set_project_and_collection_together(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            result = CliRunner().invoke(cli, ["set", "--proj", "gamma", "--coll", "c1"])
            assert result.exit_code == 0, result.output
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") == "gamma"
            assert pinned.get("collection") == "c1"

    def test_unset_coll_clears_only_collection(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            runner.invoke(cli, ["set", "--proj", "gamma", "--coll", "c1"])
            unset = runner.invoke(cli, ["unset", "--coll"])
            assert unset.exit_code == 0, unset.output
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") == "gamma"
            assert pinned.get("collection") is None

    def test_unset_proj_clears_both(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            runner.invoke(cli, ["set", "--proj", "gamma", "--coll", "c1"])
            unset = runner.invoke(cli, ["unset", "--proj"])
            assert unset.exit_code == 0, unset.output
            assert "c1" in unset.output  # cascade surfaced
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") is None
            assert pinned.get("collection") is None

    def test_unset_with_no_flags_fails(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            result = CliRunner().invoke(cli, ["unset"])
            assert result.exit_code != 0
            assert "Nothing to unset" in result.output

    def test_unset_both_pins_together(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            runner.invoke(cli, ["set", "--proj", "gamma", "--coll", "c1"])
            unset = runner.invoke(cli, ["unset", "--proj", "--coll"])
            assert unset.exit_code == 0, unset.output
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") is None
            assert pinned.get("collection") is None

    def test_switching_project_clears_collection(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            runner.invoke(cli, ["set", "--proj", "gamma", "--coll", "c1"])
            switched = runner.invoke(cli, ["set", "--proj", "delta"])
            assert switched.exit_code == 0, switched.output
            assert "c1" in switched.output  # cascade surfaced
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") == "delta"
            assert pinned.get("collection") is None

    def test_repinning_same_project_keeps_collection(self, tmp_path):
        dir_patch, init_patch = self._runner(tmp_path)
        with dir_patch, init_patch:
            runner = CliRunner()
            runner.invoke(cli, ["set", "--proj", "gamma", "--coll", "c1"])
            again = runner.invoke(cli, ["set", "--proj", "gamma"])
            assert again.exit_code == 0, again.output
            pinned = self._pinned(tmp_path)
            assert pinned.get("project") == "gamma"
            assert pinned.get("collection") == "c1"

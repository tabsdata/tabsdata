#
# Copyright 2026 Tabsdata Inc.
#

"""The internal commands (``x:...``) and options (``--x:...``): a person's
help and completion leave them out unless ``-x`` asks for them, a program's
shows them, and they run either way."""

from __future__ import annotations

import re

import pytest
import rich_click as click
from click.shell_completion import ShellComplete
from click.testing import CliRunner

from tabsdatak.cli._command import TdkCommand, TdkGroup
from tabsdatak.cli._internal import (
    INTERNAL_COMMAND_PREFIX,
    INTERNAL_NOTE,
    INTERNAL_PREFIX,
    SHOW_INTERNAL_FLAG,
    InternalOption,
    internal,
    internal_option,
)
from tabsdatak.cli.td.cli import cli

PROBE = f"{INTERNAL_COMMAND_PREFIX}probe"


def _commands(command: click.Command, ctx: click.Context, path=()):
    yield path, command
    if isinstance(command, click.Group):
        for name in sorted(command.list_commands(ctx)):
            sub = command.get_command(ctx, name)
            yield from _commands(
                sub, click.Context(sub, info_name=name, parent=ctx), (*path, name)
            )


COMMANDS = list(_commands(cli, click.Context(cli, info_name="tdk")))


@pytest.fixture
def probe(monkeypatch):
    ran = []
    group = TdkGroup(PROBE, help="An internal group.")

    @group.command("run", cls=TdkCommand, login=False)
    def run():
        ran.append(True)

    monkeypatch.setitem(cli.commands, PROBE, group)
    return ran


def test_a_person_s_help_hides_the_internal_options_and_adds_no_note():
    for argv in (["table", "data", "--help"], ["--help"], ["plan", "logs", "--help"]):
        result = CliRunner().invoke(cli, argv)
        assert result.exit_code == 0, result.output
        assert not re.search(rf"^\s*{INTERNAL_PREFIX}", result.output, re.MULTILINE)
        assert INTERNAL_NOTE not in result.output


def test_the_root_help_names_the_flag_that_shows_them():
    result = CliRunner().invoke(cli, ["--help"])
    assert re.search(rf"^\s*{SHOW_INTERNAL_FLAG}\s", result.output, re.MULTILINE)


def test_a_program_s_help_shows_the_internal_options():
    result = CliRunner().invoke(cli, ["table", "data", "--help", "--machine"])
    assert "--x:columns" in result.output and "--x:row-format" in result.output
    assert INTERNAL_NOTE not in result.output
    assert f"  {SHOW_INTERNAL_FLAG}  " not in result.output


@pytest.mark.parametrize(
    "argv",
    [
        ["table", "data", "--help", SHOW_INTERNAL_FLAG],
        [SHOW_INTERNAL_FLAG, "table", "data", "--help"],
        ["table", SHOW_INTERNAL_FLAG, "data", "--help"],
    ],
)
def test_a_person_sees_the_internal_options_on_asking(argv):
    result = CliRunner().invoke(cli, argv)
    assert result.exit_code == 0, result.output
    assert "--x:columns" in result.output
    assert result.output.rstrip().endswith(INTERNAL_NOTE)
    again = CliRunner().invoke(cli, ["table", "data", "--help"])
    assert "--x:columns" not in again.output


@pytest.mark.parametrize(
    "argv", [[SHOW_INTERNAL_FLAG, "about"], ["table", "list", SHOW_INTERNAL_FLAG]]
)
def test_a_command_that_runs_refuses_the_flag(argv):
    result = CliRunner().invoke(cli, argv)
    assert result.exit_code == 2, result.output


def test_a_group_given_no_command_shows_them_with_the_flag(probe):
    bare = CliRunner().invoke(cli, [])
    asked = CliRunner().invoke(cli, [SHOW_INTERNAL_FLAG])
    assert PROBE not in bare.output and PROBE in asked.output
    inside = CliRunner().invoke(cli, [SHOW_INTERNAL_FLAG, PROBE])
    assert "run" in inside.output and inside.output.rstrip().endswith(INTERNAL_NOTE)


def test_the_flag_after_the_separator_is_the_command_s():
    result = CliRunner().invoke(cli, ["table", "data", "--help", "--", "-x"])
    assert "--x:columns" not in result.output


def test_a_person_s_help_hides_an_internal_command(probe):
    assert PROBE not in CliRunner().invoke(cli, ["--help"]).output
    assert PROBE in CliRunner().invoke(cli, ["--help", SHOW_INTERNAL_FLAG]).output
    assert PROBE in CliRunner().invoke(cli, ["--help", "--machine"]).output


def test_an_internal_command_runs_and_shows_its_own_help(probe):
    result = CliRunner().invoke(cli, [PROBE, "run"])
    assert result.exit_code == 0, result.output
    assert probe == [True]
    assert "run" in CliRunner().invoke(cli, [PROBE, "--help"]).output


def test_the_full_help_lists_an_internal_command_only_on_asking(probe):
    leaf = f"{PROBE} run"
    assert leaf not in CliRunner().invoke(cli, ["--help", "--full"]).output
    assert leaf in CliRunner().invoke(cli, ["--help", "--full", "-x"]).output
    paged = CliRunner().invoke(cli, [PROBE, "--help", "--full", "--machine"])
    assert f"tdk {leaf}" in paged.output
    asked = CliRunner().invoke(cli, [PROBE, "--help", "--full", "-o", "jsonb"])
    assert f"tdk {leaf}" in asked.output
    whole = ["--help", "--full", "--max-result", "0"]
    for paged in (["-o", "jsonb"], ["--machine"]):
        shown = CliRunner().invoke(cli, ["-x", *whole, *paged])
        assert shown.exit_code == 0, shown.output
        assert f"tdk {leaf}" in shown.output
    hidden = CliRunner().invoke(cli, [*whole, "-o", "jsonb"])
    assert f"tdk {leaf}" not in hidden.output


def test_completion_leaves_the_internal_commands_and_options_out(probe):
    complete = ShellComplete(cli, {}, "tdk", "_TDK_COMPLETE")
    commands = {item.value for item in complete.get_completions([], "")}
    options = {item.value for item in complete.get_completions(["table", "data"], "--")}
    assert "table" in commands and PROBE not in commands
    assert "--len" in options and "--x:columns" not in options


def test_nothing_internal_is_hidden_outside_a_person_s_help(probe):
    data = cli.commands["table"].commands["data"]
    columns = next(param for param in data.params if "--x:columns" in param.opts)
    assert not cli.commands[PROBE].hidden and not columns.hidden


def test_every_internal_option_is_declared_as_one():
    for path, command in COMMANDS:
        for param in command.params:
            assert internal(param) == isinstance(param, InternalOption), (path, param)


def test_an_internal_option_needs_the_prefix():
    with pytest.raises(ValueError):
        internal_option("--columns")


def test_a_command_refuses_an_internal_option_declared_as_a_plain_one():
    with pytest.raises(TypeError):
        TdkCommand("probe", params=[click.Option(["--x:columns"])])


def test_no_command_takes_the_flag_that_shows_the_internal_ones():
    for path, command in COMMANDS[1:]:
        for param in command.params:
            assert SHOW_INTERNAL_FLAG not in param.opts, path

#
# Copyright 2026 Tabsdata Inc.
#

"""In a machine run every command's ``--help`` is plain text within a fixed
size, and ``--help --full`` pages the commands of a group."""

from __future__ import annotations

import json
import re

import pytest
import rich_click as click
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli

HELP_MAX_BYTES = 4096


def _paths(command: click.Command, ctx: click.Context, path=()):
    yield path
    if isinstance(command, click.Group):
        for name in sorted(command.list_commands(ctx)):
            sub = command.get_command(ctx, name)
            if sub is not None and not sub.hidden:
                yield from _paths(
                    sub, click.Context(sub, info_name=name, parent=ctx), (*path, name)
                )


PATHS = list(_paths(cli, click.Context(cli, info_name="tdk")))


@pytest.mark.parametrize("path", PATHS, ids=lambda path: " ".join(path) or "tdk")
def test_every_help_is_plain_and_within_its_size(path):
    result = CliRunner().invoke(cli, [*path, "--help", "--machine"])
    assert result.exit_code == 0, result.output
    assert result.stdout.startswith(f"Usage: {' '.join(['cli', *path])} ")
    assert len(result.stdout.encode("utf-8")) <= HELP_MAX_BYTES
    assert "╭" not in result.stdout and "│" not in result.stdout
    for option in ("--browse", "--wrap", "--max-width", "--format"):
        assert not re.search(rf"(?m)^  (?:-\w, )?{option}(?![\w-])", result.stdout)


def test_a_terminal_gets_the_rich_help(monkeypatch):
    monkeypatch.setattr("tabsdatak.cli.cli_utils.is_console", lambda stream: True)
    stdout = CliRunner().invoke(cli, ["plan", "list", "--help"]).stdout
    assert "╭" in stdout or "┌" in stdout


def test_help_without_a_terminal_is_plain_and_keeps_every_option():
    result = CliRunner().invoke(cli, ["fn", "trigger", "--help"])
    assert result.exit_code == 0, result.output
    assert result.stdout.startswith("Usage: cli fn trigger ")
    assert "╭" not in result.stdout
    assert "--wait/--no-wait" in result.stdout
    for option in ("--format", "--wrap"):
        assert option in CliRunner().invoke(cli, ["plan", "list", "--help"]).stdout


def test_machine_help_marks_the_options_the_command_asks_for():
    machine = CliRunner().invoke(cli, ["table", "schema", "--help", "--machine"]).stdout
    person = CliRunner().invoke(cli, ["table", "schema", "--help"]).stdout
    assert re.search(r"^  --name, -n TEXT  .*\[required\]$", machine, re.M)
    assert not re.search(r"^  --coll, -c TEXT  .*\[required\]$", machine, re.M)
    assert "[required]" not in person


def test_machine_help_shows_the_machine_wait_default(machine_settings):
    machine_settings.update(wait_seconds=60.0)
    argv = ["fn", "trigger", "--help"]
    machine = CliRunner().invoke(cli, [*argv, "--machine"]).stdout
    person = CliRunner().invoke(cli, argv).stdout
    assert "  --timeout S  Stop waiting after S seconds. Default: 60." in machine
    assert "  --timeout S  Stop waiting after S seconds. Default: no limit." in person


def test_full_help_as_jsonb_pages_for_a_person():
    result = CliRunner().invoke(cli, ["plan", "--help", "--full", "-o", "jsonb"])
    assert result.exit_code == 0, result.output
    page = json.loads(result.stdout)
    assert page["next"] is None and page["items"]
    assert all(item["path"].startswith("tdk plan ") for item in page["items"])


def test_a_bad_full_help_as_jsonb_is_an_error_document():
    result = CliRunner().invoke(cli, ["nope", "--help", "--full", "-o", "jsonb"])
    assert result.exit_code == 2, result.output
    assert json.loads(result.stdout)["error"]["type"] == "usage"


def _leaf_paths(command, ctx, path=("tdk",)):
    if not isinstance(command, click.Group):
        yield " ".join(path)
        return
    for name in sorted(command.list_commands(ctx)):
        sub = command.get_command(ctx, name)
        if sub is not None and not sub.hidden:
            yield from _leaf_paths(
                sub, click.Context(sub, info_name=name, parent=ctx), (*path, name)
            )


def _pages(argv: list[str]) -> list[dict]:
    pages, cursor = [], None
    while True:
        more = ["--cursor", cursor] if cursor else []
        result = CliRunner().invoke(cli, [*argv, *more, "--machine"])
        assert result.exit_code == 0, result.output
        pages.append(json.loads(result.stdout))
        cursor = pages[-1]["next"]
        if cursor is None:
            return pages


def test_full_help_pages_every_command_once_within_the_size(machine_settings):
    machine_settings.update(max_bytes=8192)
    pages = _pages(["--help", "--full"])
    paths = [item["path"] for page in pages for item in page["items"]]
    assert paths == list(_leaf_paths(cli, click.Context(cli, info_name="tdk")))
    assert len(pages) > 1
    assert all(len(json.dumps(page)) <= 8192 for page in pages)
    assert "'--fields path,help'" in pages[0]["hint"]


def test_full_help_of_a_group_lists_only_its_commands(machine_settings):
    (page,) = _pages(["plan", "--help", "--full"])
    paths = [item["path"] for item in page["items"]]
    assert paths and all(path.startswith("tdk plan ") for path in paths)
    assert {"options", "arguments", "help"} <= set(page["items"][0])


def test_full_help_keeps_only_the_fields_asked_for(machine_settings):
    pages = _pages(["--help", "--full", "--fields", "path,help"])
    items = [item for page in pages for item in page["items"]]
    assert items and all(set(item) == {"path", "help"} for item in items)


def test_a_full_help_cursor_continues_with_other_fields(machine_settings):
    machine_settings.update(max_bytes=8192)
    first = CliRunner().invoke(cli, ["--help", "--full", "--machine"])
    cursor = json.loads(first.stdout)["next"]
    argv = ["--help", "--full", "--fields", "path", "--cursor", cursor, "--machine"]
    result = CliRunner().invoke(cli, argv)
    assert result.exit_code == 0, result.output
    page = json.loads(result.stdout)
    assert page["items"] and all(set(item) == {"path"} for item in page["items"])


@pytest.mark.parametrize(
    "argv",
    [
        ["--help", "--full", "--wrap"],
        ["nope", "--help", "--full"],
        ["--help", "--full", "--format", "text"],
    ],
)
def test_full_help_takes_only_its_paging_options(argv):
    result = CliRunner().invoke(cli, [*argv, "--machine"])
    assert result.exit_code == 2, result.output
    assert json.loads(result.stdout)["error"]["type"] == "usage"


def test_the_hidden_aliases_accept_an_agent_s_guess_without_showing():
    data = cli.get_command(click.Context(cli), "table").get_command(
        click.Context(cli), "data"
    )
    aliases = {opt: param for param in data.params for opt in param.opts}
    assert aliases["--max-result"].hidden and aliases["--fields"].hidden

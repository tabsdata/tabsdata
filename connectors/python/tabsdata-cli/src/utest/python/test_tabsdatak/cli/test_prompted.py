#
# Copyright 2026 Tabsdata Inc.
#

"""Every option a command asks for when it is missing is declared so, as an
option with a prompt (``prompted``), unless the command asks for it
only on some paths: a machine run, which never prompts, is the probe. The
commands only a person at a terminal may run are left out."""

from __future__ import annotations

import json
import re
from unittest.mock import patch

import pytest
import rich_click as click
from click.testing import CliRunner

from tabsdatak.cli._prompted import PromptedOption
from tabsdatak.cli.extension import capability_of
from tabsdatak.cli.td.cli import cli

_MISSING = re.compile(r"Missing required value: (--[\w-]+)\.")


def _leaves(command, ctx, path=()):
    if not isinstance(command, click.Group):
        yield path, command
        return
    for name in sorted(command.list_commands(ctx)):
        sub = command.get_command(ctx, name)
        if sub is not None and not sub.hidden:
            yield from _leaves(
                sub, click.Context(sub, info_name=name, parent=ctx), (*path, name)
            )


LEAVES = [
    (path, command)
    for path, command in _leaves(cli, click.Context(cli, info_name="tdk"))
    if not getattr(capability_of(command), "interactive", False)
]


# Asked only on some paths, so not required: '--file' unless '--extract' or
# '--print', '--name' unless '--conn-file', '--path' unless '--source', ...
ON_SOME_PATHS = {
    "auth service-token create": {"--slot"},
    "auth service-token revoke": {"--slot"},
    "connection check": {"--name"},
    "fn bundle": {"--fn-version", "--file"},
    "fn patch": {"--path"},
    "login": {"--server", "--user", "--password"},
    "user create": {"--password"},
}


class _Unreachable:
    """A logged-in server, and its connection, whose every call fails, so a
    command stops at its first request: only what it asks for before one is
    probed."""

    project = "p"

    @property
    def connection(self):
        return self

    def __getattr__(self, name):
        raise RuntimeError(f"the probe makes no request ({name})")


def _inject(ctx):
    ctx.obj["tabsdataserver"] = _Unreachable()


def _error(stdout: str) -> dict:
    lines = stdout.strip().splitlines()
    try:
        document = json.loads(lines[-1]) if lines else None
    except ValueError:
        return {}
    return (document.get("error") if isinstance(document, dict) else None) or {}


def _asked(path: tuple[str, ...], command: click.Command) -> set[str]:
    """Run ``path`` with nothing given, give each option it asks for, and
    run it again, until it asks for nothing new. Every command runs, those
    that write local files too, so the caller confines it to a temporary
    home and working directory."""
    asked = []
    given = [
        "x"
        for param in command.params
        if isinstance(param, click.Argument) and param.required
    ]
    for param in command.params:
        if isinstance(param, click.Option) and param.required and not param.prompt:
            given += [param.opts[0], "x"]
    while True:
        with patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject):
            result = CliRunner().invoke(cli, [*path, *given, "--machine"])
        error = _error(result.stdout)
        found = _MISSING.match(error.get("message") or "")
        if error.get("type") != "missing_argument" or found is None:
            return set(asked)
        option = found.group(1)
        if option == "--confirm" or option in asked:
            return set(asked)
        asked.append(option)
        param = next(p for p in command.params if option in p.opts)
        given += [option] + ([] if getattr(param, "is_flag", False) else ["x"])


@pytest.mark.parametrize(
    "path, command", LEAVES, ids=[" ".join(path) for path, _ in LEAVES]
)
def test_a_command_declares_the_options_it_asks_for(
    path, command, machine_settings, tmp_path, monkeypatch
):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    machine_settings.update(
        connect_attempts=0,
        request_timeout=(1, 1),
        wait_seconds=0.01,
        max_wait=(0.01, "the probe"),
    )
    with patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)):
        asked = _asked(path, command)
    declared = {
        next(opt for opt in param.opts if opt.startswith("--"))
        for param in command.params
        if isinstance(param, PromptedOption)
    }
    assert declared <= asked
    assert asked - declared == ON_SOME_PATHS.get(" ".join(path), set())

#
# Copyright 2026 Tabsdata Inc.
#

"""Every tdk command declares what it can do with ``@capability``, next to
its definition. ``capabilities.snapshot.json`` holds each core command's
declaration with its options, so a change to either shows in the same diff;
regenerate it with ``TDK_CAPABILITIES_UPDATE=1``."""

from __future__ import annotations

import inspect
import json
import os
from pathlib import Path

import pytest
import rich_click as click

from tabsdatak.cli._capability import (
    CapabilityDeclarationError,
    Effect,
    capability,
    capability_of,
)
from tabsdatak.cli.td.cli import cli

SNAPSHOT = Path(__file__).with_name("capabilities.snapshot.json")
UPDATE_ENV = "TDK_CAPABILITIES_UPDATE"


def _leaves(command=cli, path=("tdk",), parent=None):
    if isinstance(command, click.Group):
        ctx = click.Context(command, info_name=path[-1], parent=parent)
        for name in sorted(command.list_commands(ctx)):
            yield from _leaves(command.get_command(ctx, name), (*path, name), ctx)
    else:
        yield " ".join(path), command


def _is_core(command: click.Command) -> bool:
    return inspect.unwrap(command.callback).__module__.startswith("tabsdatak.cli.")


CORE = {path: command for path, command in _leaves() if _is_core(command)}


def _param(command: click.Command, key: str) -> click.Parameter:
    return next(
        param
        for param in command.params
        if key in param.opts
        or (isinstance(param, click.Argument) and key == param.name.upper())
    )


def _parameters(command: click.Command) -> list[str]:
    return sorted(
        param.name.upper() if isinstance(param, click.Argument) else param.opts[0]
        for param in command.params
        if param.name != "help"
    )


def test_every_core_command_declares_a_capability():
    assert [path for path, command in CORE.items() if not capability_of(command)] == []


def test_the_declarations_and_options_match_the_snapshot():
    current = {
        path: {
            "capability": capability_of(command).to_json(),
            "parameters": _parameters(command),
        }
        for path, command in CORE.items()
    }
    if os.environ.get(UPDATE_ENV):
        SNAPSHOT.write_text(json.dumps(current, indent=1, sort_keys=True) + "\n")
    assert current == json.loads(SNAPSHOT.read_text()), (
        f"A command's declaration or parameters changed: check its @capability "
        f"still says what it does, then regenerate with {UPDATE_ENV}=1."
    )


def test_every_path_parameter_is_declared():
    mismatched = {}
    for path, command in CORE.items():
        declared = {_param(command, key).name for key in capability_of(command).paths}
        path_params = {
            param.name
            for param in command.params
            if isinstance(param.type, (click.Path, click.File))
        }
        if declared != path_params:
            mismatched[path] = (sorted(declared), sorted(path_params))
    assert mismatched == {}


def test_only_stdio_path_options_allow_a_dash():
    stdio = {
        (path, option)
        for path, command in CORE.items()
        for option in capability_of(command).paths
        if _param(command, option).type.allow_dash
    }
    assert stdio == {
        ("tdk auth add-cert", "--pem"),
        ("tdk collection create", "--conn-file"),
        ("tdk collection update", "--conn-file"),
        ("tdk connection check", "--conn-file"),
        ("tdk connection template", "--file"),
        ("tdk connection validate", "--file"),
        ("tdk fnrun logs", "--file"),
        ("tdk plan logs", "--file"),
        ("tdk project create", "--secrets-file"),
        ("tdk project update", "--secrets-file"),
        ("tdk trx logs", "--file"),
        ("tdk worker logs", "--file"),
    }


def test_commands_take_options_not_positional_arguments():
    positional = {
        path
        for path, command in CORE.items()
        if any(isinstance(param, click.Argument) for param in command.params)
    }
    assert positional == {"tdk auth password-change"}


def test_destructive_commands_ask_for_their_confirmation_word():
    unconfirmed = {
        path
        for path, command in CORE.items()
        if capability_of(command).effect is Effect.DESTRUCTIVE
        and not any("--confirm" in param.opts for param in command.params)
    }
    assert unconfirmed == {"tdk auth service-token revoke", "tdk permission revoke"}


def test_only_shell_and_password_change_need_a_person():
    interactive = {
        path for path, command in CORE.items() if capability_of(command).interactive
    }
    assert interactive == {"tdk shell", "tdk auth password-change"}


def _command(*params: click.Parameter) -> click.Command:
    return click.Command("probe", params=list(params), callback=lambda: None)


def test_an_undeclared_path_argument_is_caught():
    command = _command(click.Argument(["paths"], nargs=-1, type=click.Path()))
    declared = capability("knowledge", "read", "read", paths={"PATHS": "read"})
    assert capability_of(declared(command)).paths
    with pytest.raises(CapabilityDeclarationError, match="PATH"):
        capability("knowledge", "read", paths={"PATH": "read"})(command)


def test_declaring_an_unknown_option_fails():
    with pytest.raises(CapabilityDeclarationError, match="--nope"):
        capability("analytics", "read", escalate={"--nope": {"effect": "write"}})(
            _command()
        )


def test_a_lowering_option_must_exist_and_lower_the_effect():
    with pytest.raises(CapabilityDeclarationError, match="--nope"):
        capability("development", "write", lower={"--nope": "read"})(_command())
    with pytest.raises(CapabilityDeclarationError, match="below 'write'"):
        capability("development", "write", lower={"--dry-run": "write"})(
            _command(click.Option(["--dry-run"], is_flag=True))
        )


def test_declared_paths_must_be_click_paths():
    with pytest.raises(CapabilityDeclarationError, match="click.Path"):
        capability("analytics", "read", paths={"--file": "write"})(
            _command(click.Option(["--file"]))
        )


@pytest.mark.parametrize(
    "arguments",
    [
        {"domain": "weather", "effect": "read"},
        {"domain": "analytics", "effect": "obliterate"},
        {"domain": "analytics", "effect": "read", "credential": "root"},
    ],
)
def test_values_outside_the_model_fail(arguments):
    with pytest.raises(CapabilityDeclarationError):
        capability(**arguments)(_command())

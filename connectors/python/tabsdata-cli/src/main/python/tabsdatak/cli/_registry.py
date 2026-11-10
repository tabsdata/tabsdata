#
# Copyright 2026 Tabsdata Inc.
#

"""Decorator + registry for top-level ``tdk`` CLI command groups.

Annotating a Click command/group with :func:`cli_command` appends it to
:data:`_CLI_COMMAND_REGISTRY` at import time; :func:`collect_cli_commands`
returns everything registered. The root ``tdk`` group (see
:mod:`tabsdatak.cli.td.cli`) imports the built-in command modules for their
``@cli_command`` side effects and then attaches whatever the registry holds, so
there is no manual ``add_command`` list to keep in sync -- a new command just
needs the annotation.

Enterprise-tier variants contribute the same way: their command modules
annotate their own groups with :func:`cli_command`, and a
``tabsdatak.extensions.variant.cli.commands`` module (which ships only when the
variant is installed) imports them for the side effect.
:func:`collect_cli_commands` imports that module when it exists, so a variant's
commands surface only where the variant is installed -- open source names
nothing edition-specific.
This rides the same ``tabsdatak.extensions.variant.cli`` package that
``variant_action_classes()`` in :mod:`tabsdatak.cli.td.config_utils` already
uses for variant *actions*; commands live in a sibling ``commands`` module so
the actions path never depends on whatever the command module imports.

The same module may register a :class:`CliExtension` with
:func:`cli_extension`: it wraps every ``tdk`` invocation and adds to the root
options and the ``--help --full`` tree. An extension that runs tdk for a
program sets a :class:`tabsdatak.cli._machine.Machine` for the invocation.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    import click

# Populated by @cli_command at import time; read by collect_cli_commands().
_CLI_COMMAND_REGISTRY: list["click.Command"] = []
_CLI_EXTENSIONS: list["CliExtension"] = []

# Importing this module registers the installed variant's annotated commands.
# Present only when an enterprise-tier variant is installed.
_VARIANT_CLI_COMMANDS = "tabsdatak.extensions.variant.cli.commands"


def cli_command(command: "click.Command") -> "click.Command":
    """Register a top-level ``tdk`` CLI command/group for auto-collection.

    Apply *above* ``@click.group(...)`` / ``@click.command(...)`` so the
    decorated object is the built Click command::

        @cli_command
        @click.group(cls=TdkGroup)
        def auth():
            ...

    Returns the command unchanged so it stays usable directly. It must be a
    :class:`tabsdatak.cli._command.TdkCommand` or ``TdkGroup``, which settle
    the login and project before anything is asked.
    """
    from tabsdatak.cli._command import TdkCommand, TdkGroup

    if not isinstance(command, (TdkCommand, TdkGroup)):
        raise TypeError(
            f"'{command.name}' must be built with cls=TdkCommand or cls=TdkGroup."
        )
    _CLI_COMMAND_REGISTRY.append(command)
    return command


def collect_cli_commands() -> list["click.Command"]:
    """Return every ``@cli_command``-annotated command to attach to ``tdk``.

    Built-in commands self-register when their modules are imported (the root
    CLI imports them). Variant commands self-register when the variant command
    module is imported here. Only the absence of that module means "no variant
    installed"; any other failure while importing it, a missing dependency of
    the variant included, propagates so a broken variant is never mistaken
    for open source.
    """
    try:
        importlib.import_module(_VARIANT_CLI_COMMANDS)
    except ModuleNotFoundError as error:
        if not _is_variant_module(error.name):
            raise
    return list(_CLI_COMMAND_REGISTRY)


def _is_variant_module(name: str | None) -> bool:
    return name is not None and (
        name == _VARIANT_CLI_COMMANDS or _VARIANT_CLI_COMMANDS.startswith(f"{name}.")
    )


class CliExtension(Protocol):
    def main(
        self, root: "click.Group", argv: list[str], run: Callable[..., Any]
    ) -> Any:
        """Run one invocation of ``root``: ``run(argv, **main_kwargs)`` is the
        plain one, taking the argv to parse and any override of the keyword
        arguments of :meth:`click.BaseCommand.main`."""

    def describe_root(self) -> dict:
        """Keys added to the root of the ``--help --full`` JSON tree."""

    def describe(self, command: "click.Command", path: str) -> dict:
        """Keys added to a leaf of the ``--help --full`` JSON tree; ``path`` is
        its command words after ``tdk``."""


def cli_extension(extension: "CliExtension") -> "CliExtension":
    """Register an extension of every ``tdk`` invocation. Extensions wrap the
    invocation in registration order, the first outermost."""
    _CLI_EXTENSIONS.append(extension)
    return extension


def cli_extensions() -> list["CliExtension"]:
    return list(_CLI_EXTENSIONS)

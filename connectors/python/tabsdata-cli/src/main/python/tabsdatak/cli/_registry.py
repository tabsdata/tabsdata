#
# Copyright 2026 Tabs Data Inc.
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
:func:`collect_cli_commands` imports that module best-effort, so a variant's
commands surface only where the variant is installed -- open source names
nothing edition-specific.
This rides the same ``tabsdatak.extensions.variant.cli`` package that
``variant_action_classes()`` in :mod:`tabsdatak.cli.td.config_utils` already
uses for variant *actions*; commands live in a sibling ``commands`` module so
the actions path never depends on whatever the command module imports.
"""

from __future__ import annotations

import importlib
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import click

logger = logging.getLogger(__name__)

# Populated by @cli_command at import time; read by collect_cli_commands().
_CLI_COMMAND_REGISTRY: list["click.Command"] = []

# Importing this module registers the installed variant's annotated commands.
# Present only when an enterprise-tier variant is installed.
_VARIANT_CLI_COMMANDS = "tabsdatak.extensions.variant.cli.commands"


def cli_command(command: "click.Command") -> "click.Command":
    """Register a top-level ``tdk`` CLI command/group for auto-collection.

    Apply *above* ``@click.group(...)`` / ``@click.command(...)`` so the
    decorated object is the built Click command::

        @cli_command
        @click.group()
        def auth():
            ...

    Returns the command unchanged so it stays usable directly.
    """
    _CLI_COMMAND_REGISTRY.append(command)
    return command


def collect_cli_commands() -> list["click.Command"]:
    """Return every ``@cli_command``-annotated command to attach to ``tdk``.

    Built-in commands self-register when their modules are imported (the root
    CLI imports them). Variant commands self-register when the variant command
    module is imported here -- best-effort, absent in open source. A broken
    variant is logged and skipped so it can never break the core CLI.
    """
    try:
        importlib.import_module(_VARIANT_CLI_COMMANDS)
    except ImportError:
        pass  # no variant installed -- open source has no edition-specific commands
    except Exception as e:  # never let a variant break the core CLI
        logger.warning("Skipping variant CLI commands: %s", e)
    return list(_CLI_COMMAND_REGISTRY)

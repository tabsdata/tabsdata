#
# Copyright 2026 Tabsdata Inc.
#

"""The help printed where no terminal shows it: plain text. A machine run's
also leaves out the options that only shape a person's view of the output or
that it refuses, marks the options the command would ask a person for as
required, since a machine run never asks, and shows the run's own defaults."""

from __future__ import annotations

import sys

import rich_click as click
from click.core import UNSET

NOT_FOR_MACHINES = frozenset({"--browse", "--wrap", "--max-width", "--format", "-x"})


def help_shown(ctx: click.Context, rich) -> str:
    """The help of ``ctx.command``: ``rich``, the command's own rendering, at a
    terminal; :func:`plain_help` elsewhere, and in a machine run without the
    options :data:`NOT_FOR_MACHINES` names. A person's help hides the internal
    commands and options; with ``-x`` it shows them and ends saying what they
    are (:mod:`tabsdatak.cli._internal`)."""
    from tabsdatak.cli._internal import (
        INTERNAL_NOTE,
        concealing_internal,
        internal_shown,
    )
    from tabsdatak.cli._machine import machine
    from tabsdatak.cli.cli_utils import is_console

    run = machine()
    if run is not None:
        return plain_help(ctx, NOT_FOR_MACHINES, run)
    console = is_console(sys.stdout)
    with concealing_internal():
        text = rich(ctx) if console else plain_help(ctx)
    if not internal_shown():
        return text
    if console:
        return f"{text.rstrip()}\n\n {INTERNAL_NOTE}\n"
    return f"{text}\n\n{INTERNAL_NOTE}"


def option_help(param: click.Option, run=None) -> str:
    """``param``'s help, as the machine ``run`` sees it when it has its own."""
    shown = getattr(param, "machine_help", None)
    return (shown(run) if shown is not None and run is not None else param.help) or ""


def plain_help(
    ctx: click.Context,
    hidden: frozenset[str] = frozenset(),
    run=None,
) -> str:
    command = ctx.command
    lines = [f"Usage: {ctx.command_path} {_usage_tail(command)}".rstrip(), ""]
    text = (command.help or command.short_help or "").strip()
    if text:
        lines += [text, ""]
    arguments = [p for p in command.params if isinstance(p, click.Argument)]
    if arguments:
        lines.append("Arguments:")
        lines += [f"  {_argument(param)}" for param in arguments]
        lines.append("")
    options = [_option(param, run) for param in command.params if _shown(param, hidden)]
    if options:
        lines.append("Options:")
        lines += options
        lines.append("")
    if isinstance(command, click.Group):
        lines.append("Commands:")
        for name in sorted(command.list_commands(ctx)):
            sub = command.get_command(ctx, name)
            if sub is not None and not sub.hidden:
                summary = sub.get_short_help_str(limit=100)
                lines.append(f"  {name}  {summary}".rstrip())
        lines.append("")
    return "\n".join(lines).rstrip()


def _usage_tail(command: click.Command) -> str:
    words = ["[OPTIONS]"]
    words += [_argument(p) for p in command.params if isinstance(p, click.Argument)]
    if isinstance(command, click.Group):
        words.append("COMMAND [ARGS]...")
    return " ".join(words)


def _shown(param: click.Parameter, hidden: frozenset[str]) -> bool:
    return (
        isinstance(param, click.Option)
        and not param.hidden
        and not hidden.intersection(param.opts)
    )


def _argument(param: click.Argument) -> str:
    name = (param.metavar or param.name or "").upper()
    name = f"{name}..." if param.nargs == -1 else name
    return name if param.required else f"[{name}]"


def _option(param: click.Option, run=None) -> str:
    flags = ", ".join(param.opts)
    if param.secondary_opts:
        flags = f"{flags}/{', '.join(param.secondary_opts)}"
    value = "" if param.is_flag or param.count else f" {_metavar(param)}"
    notes = []
    asked = run is not None and param.prompt is not None and param.default is UNSET
    if param.required or asked:
        notes.append("required")
    if param.multiple:
        notes.append("repeatable")
    if param.show_default and not param.is_flag and param.default is not None:
        default = param.show_default
        notes.append(f"default: {param.default if default is True else default}")
    help_text = " ".join(option_help(param, run).split())
    tail = f" [{'; '.join(notes)}]" if notes else ""
    return f"  {flags}{value}  {help_text}{tail}".rstrip()


def _metavar(param: click.Option) -> str:
    if param.metavar:
        return param.metavar
    if isinstance(param.type, click.Choice):
        return "{" + "|".join(str(choice) for choice in param.type.choices) + "}"
    if isinstance(param.type, (click.types.IntParamType, click.types.FloatParamType)):
        return "N"
    return (getattr(param.type, "name", None) or "value").upper()

#
# Copyright 2026 Tabsdata Inc.
#

"""``--help --full``: the commands under the group its leading words name.
As the ``jsonb`` document, which a machine run always gives, they are a
listing tdk holds itself, paged like any other (``--max-result``,
``--cursor``, ``--fields``, a machine run's ``max_bytes``); as ``text`` or
``json`` they are that group's whole tree."""

from __future__ import annotations

import json

import rich_click as click

from tabsdatak.cli._errors import TdkException
from tabsdatak.cli._internal import concealing_internal
from tabsdatak.cli._machine import machine
from tabsdatak.cli._plain_help import option_help
from tabsdatak.cli._registry import cli_extensions
from tabsdatak.cli.cli_utils import (
    format_option,
    max_result_option,
    paged_options,
    render_list,
    take,
)

HELP_FLAGS = ("--help", "-h", "--full")
FULL_HELP_FORMATS = ("text", "json")
LESS_HINT = (
    "Ask for less with '--fields path,help', a one-line index, or a group's "
    "commands with 'tdk <group> --help --full'."
)


@click.command()
@click.argument("words", nargs=-1)
@max_result_option
@paged_options
@format_option
@click.pass_context
def full_help(ctx: click.Context, words: tuple[str, ...]):
    """The commands under a group, each with its options and capability."""
    group = _named(ctx.obj["root"], words)
    with concealing_internal():
        tree = _full_help_node(group)
    page, truncated = take(ctx, list(_leaves(tree)))

    def less():
        if truncated or ctx.obj.get("list_fitted") is not None:
            return LESS_HINT
        return None

    render_list(ctx, items=page, truncated=truncated, cut_hint=less)


@click.command(add_help_option=False)
@click.argument("words", nargs=-1)
@click.option(
    "-o",
    "--format",
    "output",
    type=click.Choice(FULL_HELP_FORMATS),
    default=FULL_HELP_FORMATS[0],
)
@click.pass_context
def full_help_tree(ctx: click.Context, words: tuple[str, ...], output: str):
    return build_full_help(ctx.obj["root"], output, words)


def full_help_page(root: click.Group, argv: list[str]) -> None:
    """Run :func:`full_help` for ``argv``, a ``--help --full`` invocation, on
    ``root``. Rendered as a machine run or a person's ``jsonb`` document
    renders a listing."""
    _run(full_help, root, argv)


def full_help_whole(root: click.Group, argv: list[str]) -> str:
    return _run(full_help_tree, root, argv)


def _run(command: click.Command, root: click.Group, argv: list[str]):
    return command.main(
        args=[arg for arg in argv if arg not in HELP_FLAGS],
        prog_name="tdk --help --full",
        standalone_mode=False,
        obj={"root": root},
    )


def _named(root: click.Group, words: tuple[str, ...]) -> click.Context:
    named = _root_context(root)
    for word in words:
        command = named.command
        sub = (
            command.get_command(named, word)
            if isinstance(command, click.Group)
            else None
        )
        if sub is None or sub.hidden:
            raise TdkException("usage", f"No command '{' '.join(words)}'.")
        named = click.Context(sub, info_name=word, parent=named)
    return named


def _leaves(node: dict):
    commands = node.get("commands")
    if commands is None:
        yield {key: value for key, value in node.items() if key != "name"}
        return
    for sub in commands:
        yield from _leaves(sub)


def _root_context(root: click.Group) -> click.Context:
    return click.Context(root, info_name="tdk", **root.context_settings)


def _full_help_options(command) -> list:
    options = []
    for param in getattr(command, "params", []):
        if isinstance(param, click.Option) and not param.hidden:
            options.append({
                "opts": list(param.opts) + list(param.secondary_opts),
                "help": option_help(param, machine()),
                "is_flag": bool(param.is_flag),
                "required": bool(param.required),
                "default": None if param.is_flag else param.to_info_dict()["default"],
                "type": getattr(param.type, "name", None),
            })
    return options


def _full_help_node(ctx: click.Context) -> dict:
    command = ctx.command
    node = {
        "name": ctx.info_name,
        "path": ctx.command_path,
        "help": (command.help or command.short_help or "").strip(),
        "options": _full_help_options(command),
        "arguments": [
            {"name": param.name, "required": bool(param.required)}
            for param in getattr(command, "params", [])
            if isinstance(param, click.Argument)
        ],
    }
    if isinstance(command, click.Group):
        subcommands = {
            sub: command.get_command(ctx, sub) for sub in command.list_commands(ctx)
        }
        node["commands"] = [
            _full_help_node(click.Context(subcommand, info_name=sub, parent=ctx))
            for sub, subcommand in sorted(subcommands.items())
            if not subcommand.hidden
        ]
    else:
        words = ctx.command_path.partition(" ")[2]
        for extension in cli_extensions():
            node.update(extension.describe(command, words))
    return node


def _full_help_text(node: dict, lines: list | None = None) -> str:
    lines = [] if lines is None else lines
    lines.append(node["path"])
    if node["help"]:
        for line in node["help"].splitlines():
            lines.append(f"    {line}".rstrip())
    if node["arguments"]:
        lines.append("    Arguments:")
        for arg in node["arguments"]:
            tag = "required" if arg["required"] else "optional"
            lines.append(f"      {arg['name']} ({tag})")
    if node["options"]:
        lines.append("    Options:")
        for opt in node["options"]:
            flags = ", ".join(opt["opts"])
            tag = "required" if opt["required"] else "optional"
            suffix = f"  {opt['help']}" if opt["help"] else ""
            lines.append(f"      {flags} ({tag}){suffix}".rstrip())
    lines.append("")
    for sub in node.get("commands", []):
        _full_help_text(sub, lines)
    return "\n".join(lines)


def build_full_help(root, output: str = "text", words: tuple[str, ...] = ()) -> str:
    """Render the help for the command ``words`` name under ``root``, ``root``
    itself by default, and every command/subcommand under it.

    Each option and argument is tagged ``(required)`` or ``(optional)`` in the
    text form, and carries a ``required`` boolean in the json form.

    ``output`` is ``"text"`` (a flat, indented listing) or ``"json"`` (a nested
    tree of ``{name, path, help, options, arguments, commands}`` objects, to
    which each CLI extension adds, what it says of the root only on the
    root)."""
    named = _named(root, words)
    with concealing_internal():
        tree = _full_help_node(named)
    if output == "json":
        if not words:
            commands = tree.pop("commands", [])
            for extension in cli_extensions():
                tree.update(extension.describe_root())
            tree = {**tree, "commands": commands}
        return json.dumps(tree, indent=2, default=str)
    return _full_help_text(tree)

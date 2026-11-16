#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk-reference.md`` quotes only commands and options the click tree has,
and every public command and option os ships. Deprecated aliases, internal
``x:`` commands and ``--x:`` options, and the commands an installed variant
adds are left out; a group counts as quoted through its commands.

What the reference quotes:

- A fenced block under ``## `tdk```: the root help screen. Its two-space
  indented entries name root commands (``set / unset`` names two); its option
  entries name root options.
- The ``## Common options`` block: each entry's options are one option of
  some command.
- Every other fenced block: a line starting at column 0 is a command line
  (``plan get --plan PLAN [...]``) and an indented line continues it; ``#``
  starts a comment.
- An inline span ``tdk ...``, which may wrap lines.

In a command line, the leading words are the command path (``cron|stream
list`` is two paths) and every ``-x``/``--xyz`` token after it is an option
of that command; other tokens are values.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from pathlib import Path

import click
import pytest

from tabsdatak.cli._internal import internal
from tabsdatak.cli.td.cli import cli

REFERENCE = next(
    parent / "tdk-reference.md"
    for parent in Path(__file__).resolve().parents
    if (parent / "tdk-reference.md").is_file()
)

_OPTION = re.compile(r"(?<![\w-])(-{1,2}[A-Za-z][\w-]*)")
_SPAN = re.compile(r"`(tdk\s[^`]*)`")
_FENCE = re.compile(r"^```[^\n]*\n(.*?)^```", re.DOTALL | re.MULTILINE)
_OPTION_ENTRY = re.compile(r"^ {2,6}-")
_COMMAND_ENTRY = re.compile(r"^  \S")
_PLACEHOLDER = re.compile(r"…|<[^<>\s]+>")


_ROOT = click.Context(cli, **cli.context_settings)


def _context(command: click.Command) -> click.Context:
    return _ROOT if command is cli else click.Context(command, parent=_ROOT)


def _params(command: click.Command) -> list[click.Parameter]:
    return command.get_params(_context(command))


def _names(command: click.Command) -> set[str]:
    return {
        name
        for param in _params(command)
        for name in (*param.opts, *param.secondary_opts)
    }


def _checkable(command: click.Command, words: list[str]) -> list[str]:
    """The words before a placeholder (``…``, ``<command>``) that is not an
    option's value: what follows it belongs to a command the reference does
    not name."""
    flags = {
        name
        for param in _params(command)
        if isinstance(param, click.Option) and (param.is_flag or param.count)
        for name in (*param.opts, *param.secondary_opts)
    }
    for index, word in enumerate(words):
        before = words[index - 1] if index else ""
        if _PLACEHOLDER.fullmatch(word) and (
            not before.startswith("-") or before in flags
        ):
            return words[:index]
    return words


def _resolve(words: list[str]) -> list[tuple[str, click.Command, list[str]]]:
    """Each command path the leading words name, with the words after it."""
    found = [("tdk", cli, words)]
    resolved = []
    while found:
        path, command, rest = found.pop()
        if (
            not isinstance(command, click.Group)
            or not rest
            or rest[0].startswith(("-", "["))
            or _PLACEHOLDER.fullmatch(rest[0])
        ):
            resolved.append((path, command, _checkable(command, rest)))
            continue
        for word in rest[0].split("|"):
            try:
                _, sub, left = command.resolve_command(
                    _context(command), [word, *rest[1:]]
                )
            except click.UsageError:
                resolved.append((f"{path} {word}", None, []))
                continue
            found.append((f"{path} {sub.name}", sub, left))
    return resolved


def _problems(quoted: str) -> list[str]:
    problems = []
    for path, command, rest in _resolve(quoted.split()[1:]):
        if command is None:
            problems.append(f"{quoted!r}: no command '{path}'")
            continue
        options = _OPTION.findall(" ".join(rest))
        known = _names(command)
        if "--full" in options and {"--help", "-h"} & set(options):
            # Any command's --help --full is read from argv, with its --format.
            known = known | {"--full", "--format"}
        problems += [
            f"{quoted!r}: '{path}' has no option {option}"
            for option in options
            if option not in known
        ]
    return problems


def _sections(text: str) -> list[tuple[str, str]]:
    """Each fenced block's body with the ``##`` heading it sits under."""
    headings = [(m.start(), m.group(1)) for m in re.finditer(r"^## (.*)$", text, re.M)]
    blocks = []
    for fence in _FENCE.finditer(text):
        heading = next(
            (title for start, title in reversed(headings) if start < fence.start()),
            "",
        )
        blocks.append((heading, fence.group(1)))
    return blocks


def _entry_head(line: str) -> str:
    return re.split(r"\s{2,}", line.strip())[0]


def _root_help_quotes(block: str) -> list[str]:
    quotes = []
    for line in block.splitlines():
        head = _entry_head(line)
        if _OPTION_ENTRY.match(line):
            quotes.append(f"tdk {' '.join(_OPTION.findall(head))}")
        elif _COMMAND_ENTRY.match(line):
            quotes += [f"tdk {name}" for name in head.split(" / ")]
    return quotes


def _every_command(command: click.Command = cli):
    yield command
    if isinstance(command, click.Group):
        context = _context(command)
        for name in command.list_commands(context):
            yield from _every_command(command.get_command(context, name))


def _common_options(block: str) -> list[set[str]]:
    return [
        set(_OPTION.findall(_entry_head(line)))
        for line in block.splitlines()
        if _OPTION_ENTRY.match(line)
    ]


def _common_option_problems(block: str) -> list[str]:
    problems = []
    for options in _common_options(block):
        if not any(
            options <= {*param.opts, *param.secondary_opts}
            for command in _every_command()
            for param in _params(command)
        ):
            problems.append(f"common option {sorted(options)} is no command's option")
    return problems


def _command_lines(block: str) -> list[str]:
    lines = []
    for line in block.splitlines():
        text = re.sub(r"(^|\s)#.*$", "", line).rstrip()
        if not text:
            continue
        if text[0].isspace() and lines:
            lines[-1] += " " + text.strip()
        elif not text[0].isspace():
            lines.append(text)
    return lines


def _quotes(text: str) -> list[str]:
    """Every command line the reference quotes, as ``tdk ...``."""
    quotes = []
    for heading, block in _sections(text):
        if heading == "`tdk`":
            quotes += _root_help_quotes(block)
        elif heading != "Common options":
            quotes += [f"tdk {line}" for line in _command_lines(block)]
    quotes += [" ".join(span.split()) for span in _SPAN.findall(text)]
    return quotes


def reference_problems(text: str) -> list[str]:
    problems = []
    for heading, block in _sections(text):
        if heading == "Common options":
            problems += _common_option_problems(block)
    for quoted in _quotes(text):
        problems += _problems(quoted)
    return problems


def _open_source(command: click.Command) -> bool:
    """Whether os ships a root command. An installed variant adds its own
    (see ``tabsdatak.cli._registry``), which its edition documents."""
    module = getattr(command.callback, "__module__", None) or ""
    return module.startswith("tabsdatak.cli.")


def documented_commands(
    path: str, command: click.Command, *, with_internal: bool = False
) -> Iterator[tuple[str, click.Command]]:
    """Each command under ``command`` a reference documents: no deprecated
    alias, no internal one unless ``with_internal`` and, at the root, only
    what os ships."""
    yield path, command
    if isinstance(command, click.Group):
        context = _context(command)
        for name in command.list_commands(context):
            sub = command.get_command(context, name)
            if sub.hidden or (internal(sub) and not with_internal):
                continue
            if command is cli and not _open_source(sub):
                continue
            yield from documented_commands(
                f"{path} {name}", sub, with_internal=with_internal
            )


def reference_gaps(
    text: str, commands: Iterable[tuple[str, click.Command]] | None = None
) -> list[str]:
    """What of ``commands`` (by default every public command os ships) the
    reference ``text`` does not quote. The common options of this reference
    hold for every command, extending ones included."""
    documented: dict[click.Command, set[str]] = {}
    for quoted in _quotes(text):
        for _, command, rest in _resolve(quoted.split()[1:]):
            if command is not None:
                documented.setdefault(command, set()).update(
                    _OPTION.findall(" ".join(rest))
                )
    common = set()
    for source in (REFERENCE.read_text(encoding="utf-8"), text):
        for heading, block in _sections(source):
            if heading == "Common options":
                common = common.union(*_common_options(block))
    gaps = []
    if commands is None:
        commands = documented_commands("tdk", cli)
    for path, command in commands:
        if command not in documented:
            if not isinstance(command, click.Group):
                gaps.append(f"'{path}' is not in the reference")
            continue
        quoted = documented[command] | common
        gaps += [
            f"'{path}' option {param.opts[0]} is not in the reference"
            for param in _params(command)
            if isinstance(param, click.Option)
            and not param.hidden
            and not internal(param)
            and not {*param.opts, *param.secondary_opts} & quoted
        ]
    return gaps


def test_the_reference_quotes_only_real_commands_and_options():
    problems = reference_problems(REFERENCE.read_text(encoding="utf-8"))
    assert not problems, "\n".join(problems)


def test_the_reference_quotes_every_public_command_and_option():
    gaps = reference_gaps(REFERENCE.read_text(encoding="utf-8"))
    assert not gaps, "\n".join(gaps)


@pytest.mark.parametrize(
    ("dropped", "gap"),
    [
        ("fn trigger  --name NAME", "'tdk fn trigger' is not in the reference"),
        ("[--wait [--timeout S] | --no-wait]", "'tdk fn trigger' option --wait"),
    ],
)
def test_the_check_catches_a_gap(dropped, gap):
    text = REFERENCE.read_text(encoding="utf-8")
    assert dropped in text
    gaps = reference_gaps(text.replace(dropped, ""))
    assert any(found.startswith(gap) for found in gaps), gaps


def test_the_reference_names_no_internal_command_or_option():
    text = REFERENCE.read_text(encoding="utf-8")
    assert re.findall(r"(?<![\w-])(?:--)?x:[a-z][\w-]*", text) == []


@pytest.mark.parametrize(
    ("quoted", "problem"),
    [
        ("```\nfunction list [-o]\n```\n", "no command 'tdk function'"),
        (
            "```\ntable versions --name N\n           [--details]\n```\n",
            "no option --details",
        ),
        ("`tdk plan|trx cancel -y`", "'tdk trx cancel' has no option -y"),
        ("`tdk\nexamples list --proj P`", "no option --proj"),
        ("`tdk plan get --plan <id> --bogus`", "'tdk plan get' has no option --bogus"),
    ],
)
def test_the_check_catches_drift(quoted, problem):
    problems = reference_problems(quoted)
    assert any(problem in found for found in problems), problems


def test_the_check_accepts_the_known_shapes():
    text = (
        "```\ndocs show --name SYMBOL [--category C] [-o]\n"
        "fn bundle --fn-version FVID | --name NAME\n"
        "          --file FILE | --extract DIR [--force]\n"
        "login [--token[=T]]\n```\n"
        "`tdk --help --full --format json` and `tdk cron|stream list`\n"
        "`tdk plan --help --full`, `tdk --no-prompt <command> -o json`, `tdk fn …`\n"
    )
    assert reference_problems(text) == []

#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk-reference.md`` quotes only commands and options the click tree has.

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
from pathlib import Path

import click
import pytest

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
        ):
            resolved.append((path, command, rest))
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
        if command is cli and "--full" in options:
            # --help --full reads its --format from argv itself.
            known = known | {"--format"}
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


def _root_help_problems(block: str) -> list[str]:
    problems = []
    for line in block.splitlines():
        head = _entry_head(line)
        if _OPTION_ENTRY.match(line):
            problems += _problems(f"tdk {' '.join(_OPTION.findall(head))}")
        elif _COMMAND_ENTRY.match(line):
            for name in head.split(" / "):
                problems += _problems(f"tdk {name}")
    return problems


def _every_command(command: click.Command = cli):
    yield command
    if isinstance(command, click.Group):
        context = _context(command)
        for name in command.list_commands(context):
            yield from _every_command(command.get_command(context, name))


def _common_option_problems(block: str) -> list[str]:
    problems = []
    for line in block.splitlines():
        if not _OPTION_ENTRY.match(line):
            continue
        options = set(_OPTION.findall(_entry_head(line)))
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


def _reference_problems(text: str) -> list[str]:
    problems = []
    for heading, block in _sections(text):
        if heading == "`tdk`":
            problems += _root_help_problems(block)
        elif heading == "Common options":
            problems += _common_option_problems(block)
        else:
            for line in _command_lines(block):
                problems += _problems(f"tdk {line}")
    for span in _SPAN.findall(text):
        problems += _problems(" ".join(span.split()))
    return problems


def test_the_reference_quotes_only_real_commands_and_options():
    problems = _reference_problems(REFERENCE.read_text(encoding="utf-8"))
    assert not problems, "\n".join(problems)


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
    ],
)
def test_the_check_catches_drift(quoted, problem):
    problems = _reference_problems(quoted)
    assert any(problem in found for found in problems), problems


def test_the_check_accepts_the_known_shapes():
    text = (
        "```\ndocs show --name SYMBOL [--category C] [-o]\n"
        "fn bundle --fn-version FVID | --name NAME\n"
        "          --file FILE | --extract DIR [--force]\n"
        "login [--token[=T]]\n```\n"
        "`tdk --help --full --format json` and `tdk cron|stream list`\n"
    )
    assert _reference_problems(text) == []

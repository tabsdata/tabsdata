#
# Copyright 2026 Tabsdata Inc.
#

"""An option means the same thing and takes the same short form on every
command that has it."""

from __future__ import annotations

import click
import pytest

from tabsdatak.cli.td.cli import cli

SHORTHANDS = {"--proj": "-P", "--coll": "-c", "--name": "-n", "--format": "-o"}


def _leaves(command: click.Command, path: tuple[str, ...] = ()):
    if isinstance(command, click.Group):
        for name, sub in sorted(command.commands.items()):
            yield from _leaves(sub, (*path, name))
    else:
        yield " ".join(path), command


LEAVES = list(_leaves(cli))


@pytest.mark.parametrize("option", sorted(SHORTHANDS))
def test_an_option_always_takes_its_short_form(option):
    missing = [
        path
        for path, command in LEAVES
        for param in command.params
        if isinstance(param, click.Option)
        and option in param.opts
        and SHORTHANDS[option] not in param.opts
    ]
    assert missing == []

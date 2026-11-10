#
# Copyright 2026 Tabsdata Inc.
#

"""tdk's command classes. A tdk command asks for nothing while click parses
its command line: the options it asks for (:mod:`tabsdatak.cli._prompted`)
are left missing, so the command line's own errors come first. When it runs,
it settles what it runs against, the login, unless the command is declared
with ``login=False``, then the project, for a command taking
``proj_option``'s ``--proj``, and only then asks for the missing values."""

from __future__ import annotations

import rich_click as click
from click.core import ParameterSource

from tabsdatak.cli._prompted import PromptedOption

INTERNAL_PREFIX = "--x:"
INTERNAL_NOTE = "Options starting with '--x:' are unsupported internal options."


class InternalOption(click.Option):
    """An ``--x:`` option, for a program running tdk: its help shows it, a
    person's never does."""

    @property
    def hidden(self) -> bool:
        from tabsdatak.cli._machine import machine

        return machine() is None

    @hidden.setter
    def hidden(self, value: bool) -> None:
        pass


def internal_option(*decls: str, **attrs):
    """``click.option`` for an :class:`InternalOption`; every name starts with
    :data:`INTERNAL_PREFIX`."""
    if not all(decl.startswith(INTERNAL_PREFIX) for decl in decls if decl[:1] == "-"):
        raise ValueError(f"Internal options are named '{INTERNAL_PREFIX}...': {decls}")
    return click.option(*decls, cls=InternalOption, **attrs)


class TdkCommand(click.RichCommand):
    """A tdk command; ``login=False`` for one that runs without a login, or
    settles the login and project itself on the paths that need them. Its
    help is plain text off a terminal (:mod:`tabsdatak.cli._plain_help`)."""

    def __init__(self, *args, login: bool = True, **kwargs):
        super().__init__(*args, **kwargs)
        self.login = login

    def invoke(self, ctx: click.Context):
        from tabsdatak.cli.cli_utils import verify_login_or_prompt

        if self.login:
            verify_login_or_prompt(ctx)
        for param in self.get_params(ctx):
            if (
                isinstance(param, PromptedOption)
                and ctx.get_parameter_source(param.name) is ParameterSource.DEFAULT
            ):
                param.fill(ctx)
        return super().invoke(ctx)

    def get_help(self, ctx: click.Context) -> str:
        from tabsdatak.cli._plain_help import help_shown

        return help_shown(ctx, super().get_help)


class TdkGroup(click.RichGroup):
    """A tdk group: its commands are :class:`TdkCommand` and its groups
    :class:`TdkGroup`; its help is plain text off a terminal too."""

    command_class = TdkCommand
    group_class = type

    def get_help(self, ctx: click.Context) -> str:
        from tabsdatak.cli._plain_help import help_shown

        return help_shown(ctx, super().get_help)

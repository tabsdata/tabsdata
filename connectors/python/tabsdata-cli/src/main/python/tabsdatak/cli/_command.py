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


class TdkCommand(click.RichCommand):
    """A tdk command; ``login=False`` for one that runs without a login, or
    settles the login and project itself on the paths that need them."""

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


class TdkGroup(click.RichGroup):
    """A tdk group: its commands are :class:`TdkCommand` and its groups
    :class:`TdkGroup`."""

    command_class = TdkCommand
    group_class = type

#
# Copyright 2026 Tabsdata Inc.
#

"""Options a command needs and asks a person for when they are missing:
click's ``prompt``, through tdk's own prompt, which fails naming the option in
a program-driven run or with prompting off. They are not ``required`` to
click, so the help stays as it was; agents learn which options a command
asks for from the AgentSpec, which lists them as required."""

from __future__ import annotations

from collections.abc import Callable

import rich_click as click
from click.core import UNSET, ParameterSource


class PromptedOption(click.Option):
    """An option click prompts for when it is missing; ``hint`` replaces the
    error's default "Pass <option>." when it cannot ask."""

    def __init__(self, *args, hint: str | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.missing_hint = hint

    def consume_value(self, ctx: click.Context, opts):
        """Only a value the command line gives is read while click parses
        it; a missing one is left for :meth:`fill`, which the command calls
        once it has settled the login and the project
        (:mod:`tabsdatak.cli._command`)."""
        if self.name in opts:
            return super().consume_value(ctx, opts)
        return UNSET, ParameterSource.DEFAULT

    def fill(self, ctx: click.Context):
        """Fill the missing value: click would ask even when a default is
        there, offering it; tdk takes the default, a pinned object for one,
        without asking."""
        from tabsdatak.cli.cli_utils import logical_prompt

        value = self.get_default(ctx)
        source = ParameterSource.DEFAULT
        if value is None or self.value_is_missing(value):
            flag = next(
                (opt for opt in self.opts if opt.startswith("--")), self.opts[0]
            )
            value = logical_prompt(
                ctx,
                self.prompt,
                hide_input=self.hide_input,
                option=flag,
                hint=self.missing_hint,
            )
            source = ParameterSource.PROMPT
        ctx.params[self.name] = self.process_value(ctx, value)
        ctx.set_parameter_source(self.name, source)


def prompted(*param_decls: str, prompt: str, **attrs):
    """``click.option`` for an option the command cannot run without, asked
    for with ``prompt`` when it is missing, unless a ``default``, a pinned
    object for one, fills it."""
    return click.option(*param_decls, prompt=prompt, cls=PromptedOption, **attrs)


def pinned(kind: str) -> Callable[[], str | None]:
    """A default for :func:`prompted`: the ``kind`` the profile pins, if any."""

    def default() -> str | None:
        from tabsdatak.cli.cli_utils import get_currently_pinned_object

        ctx = click.get_current_context()
        key = f"tdk.pinned.{kind}"
        if key not in ctx.meta:
            ctx.meta[key] = get_currently_pinned_object(ctx, kind)
        return ctx.meta[key]

    return default

#
# Copyright 2026 Tabsdata Inc.
#

"""What is not part of tdk's public interface, which its name says: a command
named ``x:...``, with every command under it, and an option named
``--x:...``. They are unsupported and may change in any release, so only what
ships with tdk (its skills, an agent's instructions) relies on them. A
person's help and completion leave them out unless ``-x`` asks for them; a
program running tdk always sees them. They run either way.

Their ``hidden`` is false, as for any command or option that is not a
deprecated alias. Only while a person's help or completion is built
(:func:`concealing_internal`) do they report themselves hidden, which is how
click and rich-click leave something out."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import rich_click as click

INTERNAL_COMMAND_PREFIX = "x:"
INTERNAL_PREFIX = "--x:"
SHOW_INTERNAL_FLAG = "-x"
INTERNAL_NOTE = (
    "'x:' commands and '--x:' options are internal and may change in any release."
)

_shown = False
_concealed = False


@contextmanager
def showing_internal(shown: bool) -> Iterator[None]:
    global _shown
    previous, _shown = _shown, shown
    try:
        yield
    finally:
        _shown = previous


def internal_asked() -> bool:
    """Whether ``-x`` was given."""
    return _shown


def internal_shown() -> bool:
    from tabsdatak.cli._machine import machine

    return _shown or machine() is not None


@contextmanager
def concealing_internal() -> Iterator[None]:
    global _concealed
    previous, _concealed = _concealed, not internal_shown()
    try:
        yield
    finally:
        _concealed = previous


def internal(item: click.Command | click.Parameter) -> bool:
    """Whether the name of a command or of an option says it is internal."""
    if isinstance(item, click.Command):
        return (item.name or "").startswith(INTERNAL_COMMAND_PREFIX)
    return any(opt.startswith(INTERNAL_PREFIX) for opt in getattr(item, "opts", ()))


class Internal:
    """A command or an option that, when its name says it is internal, is
    hidden while :func:`concealing_internal` holds, besides when it is declared
    hidden."""

    _hidden = False

    @property
    def hidden(self) -> bool:
        return self._hidden or (_concealed and internal(self))

    @hidden.setter
    def hidden(self, value: bool) -> None:
        self._hidden = value


class InternalOption(Internal, click.Option):
    """An ``--x:`` option."""


def internal_option(*decls: str, **attrs):
    """``click.option`` for an :class:`InternalOption`; every name starts with
    :data:`INTERNAL_PREFIX`."""
    if not all(decl.startswith(INTERNAL_PREFIX) for decl in decls if decl[:1] == "-"):
        raise ValueError(f"Internal options are named '{INTERNAL_PREFIX}...': {decls}")
    return click.option(*decls, cls=InternalOption, **attrs)


def check_internal_options(command: click.Command) -> None:
    """Refuse an ``--x:`` option of ``command`` that :func:`internal_option`
    did not declare, which a person's help would show."""
    for param in command.params:
        if internal(param) and not isinstance(param, InternalOption):
            raise TypeError(
                f"'{command.name}': declare {param.opts} with internal_option."
            )

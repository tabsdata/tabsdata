#
# Copyright 2026 Tabsdata Inc.
#

"""What a tdk command can do, declared on the command with
:func:`capability`: the area of the product it acts on, its server-side
effect, the local files it touches, the options that raise or lower either,
whether it needs a user session and whether it needs a person at the
terminal. ``tdk --help --full --format json`` carries it, and an extension
reads it with :func:`capability_of` to decide what an invocation may do."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from types import MappingProxyType

import rich_click as click

ANY = "any"
SESSION = "session"
CREDENTIALS = (ANY, SESSION)


class Domain(StrEnum):
    ANALYTICS = "analytics"
    OPERATIONS = "operations"
    DEVELOPMENT = "development"
    ADMIN = "admin"
    SESSION = "session"
    KNOWLEDGE = "knowledge"


class Level(IntEnum):
    @property
    def label(self) -> str:
        return self.name.lower()

    @classmethod
    def parse(cls, value: str | Level) -> Level:
        if isinstance(value, cls):
            return value
        try:
            return cls[str(value).upper()]
        except KeyError:
            raise ValueError(
                f"'{value}' is not one of {', '.join(m.label for m in cls)}."
            ) from None


class Effect(Level):
    READ = 0
    WRITE = 1
    DESTRUCTIVE = 2


class Fs(Level):
    NONE = 0
    READ = 1
    WRITE = 2


@dataclass(frozen=True)
class Escalation:
    effect: Effect | None = None
    fs: Fs | None = None
    credential: str | None = None

    def to_json(self) -> dict:
        document = {}
        if self.effect is not None:
            document["effect"] = self.effect.label
        if self.fs is not None:
            document["fs"] = self.fs.label
        if self.credential is not None:
            document["credential"] = self.credential
        return document


def _frozen(mapping: Mapping) -> Mapping:
    return MappingProxyType(dict(mapping))


@dataclass(frozen=True)
class Capability:
    domain: Domain
    effect: Effect
    fs: Fs = Fs.NONE
    paths: Mapping[str, Fs] = field(default_factory=lambda: _frozen({}))
    escalate: Mapping[str, Escalation] = field(default_factory=lambda: _frozen({}))
    lower: Mapping[str, Effect] = field(default_factory=lambda: _frozen({}))
    credential: str = ANY
    interactive: bool = False
    locate: Callable[[Mapping], Mapping[str, str | tuple[str, ...]]] | None = field(
        default=None, compare=False
    )

    def to_json(self) -> dict:
        return {
            "domain": self.domain.value,
            "effect": self.effect.label,
            "fs": self.fs.label,
            "paths": {option: level.label for option, level in self.paths.items()},
            "escalate": {
                option: escalation.to_json()
                for option, escalation in self.escalate.items()
            },
            "lower": {option: level.label for option, level in self.lower.items()},
            "credential": self.credential,
            "interactive": self.interactive,
        }


class CapabilityDeclarationError(Exception):
    """A ``@capability`` names a parameter its command does not have, or holds
    a value outside the model."""


_ATTRIBUTE = "_tdk_capability"


def option_strings(command: click.Command) -> dict[str, click.Option]:
    return {
        opt: param
        for param in command.params
        if isinstance(param, click.Option)
        for opt in (*param.opts, *param.secondary_opts)
    }


def primary_option(param: click.Parameter) -> str:
    return next((opt for opt in param.opts if opt.startswith("--")), param.opts[0])


def path_params(command: click.Command) -> dict[str, click.Parameter]:
    """The parameters a ``paths`` declaration can name: options by each of
    their option strings, arguments by their upper-cased name."""
    arguments = {
        param.name.upper(): param
        for param in command.params
        if isinstance(param, click.Argument)
    }
    return {**option_strings(command), **arguments}


def _escalation(option: str, spec: Mapping) -> Escalation:
    unknown = set(spec) - {"effect", "fs", "credential"}
    if unknown:
        raise CapabilityDeclarationError(
            f"Escalation of '{option}' has unknown keys: {', '.join(sorted(unknown))}."
        )
    credential = spec.get("credential")
    if credential not in (None, SESSION):
        raise CapabilityDeclarationError(
            f"Escalation of '{option}' can only require the 'session' credential."
        )
    return Escalation(
        effect=Effect.parse(spec["effect"]) if "effect" in spec else None,
        fs=Fs.parse(spec["fs"]) if "fs" in spec else None,
        credential=credential,
    )


def declare(
    domain: str,
    effect: str,
    fs: str = "none",
    *,
    paths: Mapping[str, str] | None = None,
    escalate: Mapping[str, Mapping[str, str]] | None = None,
    lower: Mapping[str, str] | None = None,
    credential: str = ANY,
    interactive: bool = False,
    locate: Callable[[Mapping], Mapping[str, str | tuple[str, ...]]] | None = None,
) -> Capability:
    """Declare what a command can do.

    ``paths`` names every option (by option string) and argument (by its
    upper-cased name) that takes a local path (a ``click.Path`` with no
    default; ``allow_dash=True`` when the command reads or writes stdio for
    ``-``) and what the command does with it; ``escalate`` names the options
    that raise the effect, the file access or the credential the command
    needs; ``lower`` the options that make it change less (a ``--dry-run``
    that only checks), and the effect it then has. ``locate``, for a path
    parameter whose value is not the path touched (a keyword, or a default
    location), maps the parsed parameters to the paths the invocation touches,
    keyed like ``paths``, given or not. ``interactive`` marks a command that
    needs a person at the terminal (a shell, a password prompt)."""
    if credential not in CREDENTIALS:
        raise CapabilityDeclarationError(
            f"credential must be one of {', '.join(CREDENTIALS)}."
        )
    try:
        lowered = {
            option: Effect.parse(level) for option, level in (lower or {}).items()
        }
        if any(level >= Effect.parse(effect) for level in lowered.values()):
            raise CapabilityDeclarationError(
                f"'lower' must name an effect below '{effect}'."
            )
        return Capability(
            domain=Domain(domain),
            effect=Effect.parse(effect),
            fs=Fs.parse(fs),
            paths=_frozen({
                option: Fs.parse(level) for option, level in (paths or {}).items()
            }),
            escalate=_frozen({
                option: _escalation(option, spec)
                for option, spec in (escalate or {}).items()
            }),
            lower=_frozen(lowered),
            credential=credential,
            interactive=interactive,
            locate=locate,
        )
    except ValueError as error:
        raise CapabilityDeclarationError(str(error)) from None


def bind(declared: Capability, command: click.Command) -> Capability:
    """``declared``, once its options and paths are checked against ``command``."""
    name = command.name
    options = option_strings(command)
    params = path_params(command)
    for option in declared.paths:
        if option not in params:
            raise CapabilityDeclarationError(
                f"'{name}' has no option or argument '{option}'."
            )
    for option in (*declared.escalate, *declared.lower):
        if option not in options:
            raise CapabilityDeclarationError(f"'{name}' has no option '{option}'.")
    for option, level in declared.paths.items():
        param = params[option]
        if level is Fs.NONE or not isinstance(param.type, click.Path):
            raise CapabilityDeclarationError(
                f"'{name}': '{option}' must be a click.Path read or written."
            )
        if param.to_info_dict()["default"] not in (None, ()):
            raise CapabilityDeclarationError(
                f"'{name}': path option '{option}' cannot have a default, "
                "since only a path given is checked."
            )
    return declared


def capability(*args, **kwargs):
    """:func:`declare` on a command. Apply it above the click decorator, so it
    receives the built command::

        @capability("analytics", "read")
        @table.command("list")
        ..."""
    declared = declare(*args, **kwargs)

    def decorate(command: click.Command) -> click.Command:
        setattr(command, _ATTRIBUTE, bind(declared, command))
        return command

    return decorate


def capability_of(command: click.Command) -> Capability | None:
    """What ``command`` declared with :func:`capability`, if anything."""
    return getattr(command, _ATTRIBUTE, None)

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import types
from dataclasses import MISSING, dataclass, field, fields
from pathlib import Path
from typing import Any, NoReturn, Union, get_args, get_origin, get_type_hints

from tabsdatak.server._tdrunner.function._commons.error.command import CommandErrorCode

OPTION = "option"
PATH_PLACEHOLDER = "PATH"


class Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:  # type: ignore[override]
        raise CommandErrorCode.CMD_1.exception(detail=message)


def make_print_action(text: str) -> type[argparse.Action]:
    class PrintAction(argparse.Action):
        def __init__(
            self,
            option_strings,
            dest,
            **options,
        ):
            options["nargs"] = 0
            options.setdefault("default", argparse.SUPPRESS)
            super().__init__(
                option_strings,
                dest,
                **options,
            )

        def __call__(
            self,
            parser,
            namespace,
            values,
            option_string=None,
        ):
            print(text)
            parser.exit(0)

    return PrintAction


def add_print_options(
    parser: argparse.ArgumentParser,
    *,
    version: str,
    information: str,
    manual: str,
) -> None:
    parser.add_argument(
        "--man",
        "-m",
        action=make_print_action(manual),
        help="Print the detailed user guide and exit.",
    )
    parser.add_argument(
        "--version",
        "-v",
        action=make_print_action(version),
        help="Print the command build version and exit.",
    )
    parser.add_argument(
        "--info",
        "-i",
        action=make_print_action(information),
        help="Print the command build info and exit.",
    )


@dataclass(frozen=True)
class Option:
    short: str | None
    help: str

    def flags(self, name: str) -> list[str]:
        long = f"--{name.replace('_', '-')}"
        return [long] if self.short is None else [long, self.short]


def option(
    *,
    short: str | None = None,
    help: str,
    default: Any = MISSING,
) -> Any:
    return field(
        default=default,
        metadata={OPTION: Option(short=short, help=help)},
    )


def value_type(annotation: Any) -> type:
    if get_origin(annotation) in (Union, types.UnionType):
        (value,) = [
            argument for argument in get_args(annotation) if argument is not type(None)
        ]
        return value
    return annotation


@dataclass(frozen=True, kw_only=True)
class Arguments:
    @classmethod
    def parser(
        cls,
        program: str,
        description: str,
        *,
        version: str = "",
        information: str = "",
        manual: str = "",
    ) -> ArgumentsParser:
        parser = ArgumentsParser(
            cls,
            prog=program,
            description=description,
        )
        add_print_options(
            parser,
            version=version,
            information=information,
            manual=manual,
        )
        hints = get_type_hints(cls)
        for item in fields(cls):
            declared: Option = item.metadata[OPTION]
            kind = value_type(hints[item.name])
            required = item.default is MISSING
            parser.add_argument(
                *declared.flags(item.name),
                dest=item.name,
                type=kind,
                required=required,
                default=None if required else item.default,
                metavar=PATH_PLACEHOLDER if kind is Path else None,
                help=declared.help,
            )
        return parser


@dataclass(frozen=True, kw_only=True)
class CommandArguments(Arguments):
    shared_folder: Path | None = option(
        short="-s",
        default=None,
        help="Shared working folder, owned by the pod.",
    )
    work_folder: Path | None = option(
        short="-w",
        default=None,
        help="Private working folder, owned by the container.",
    )
    logging_configuration: Path | None = option(
        short="-c",
        default=None,
        help="Path to a logging configuration YAML; the bundled one when omitted.",
    )
    liveness: Path = option(
        short="-l",
        help=(
            "File the command must touch at least once a minute to signal liveness "
            "to the framework."
        ),
    )
    responder_url: str = option(help="Responder endpoint URL.")
    responder_method: str = option(help="Responder HTTP method.")
    responder_authorization: str = option(help="Responder Authorization header value.")
    journal_file: Path = option(
        help=(
            "Path to the shared journal file every command in the chain reads and "
            "writes on the shared volume."
        ),
    )


class ArgumentsParser(Parser):
    def __init__(
        self,
        arguments: type[Arguments],
        **options: Any,
    ) -> None:
        super().__init__(**options)
        self.arguments = arguments

    def parse(self, values: list[str] | None = None) -> Arguments:
        namespace = self.parse_args(values)
        return self.arguments(**{
            item.name: getattr(namespace, item.name) for item in fields(self.arguments)
        })

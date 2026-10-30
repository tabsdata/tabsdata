#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import FrozenInstanceError, dataclass, fields
from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    OPTION,
    Arguments,
    CommandArguments,
    option,
)
from tabsdatak.server._tdrunner.function._lib.errors import InvalidOptionsError

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

COMMON = [
    "--liveness",
    "/tmp/live",
    "--responder-url",
    "http://localhost/responder",
    "--responder-method",
    "POST",
    "--responder-authorization",
    "Bearer token",
    "--journal-file",
    "/tmp/journal.yaml",
]


@dataclass(frozen=True, kw_only=True)
class EmptyArguments(CommandArguments):
    pass


@dataclass(frozen=True, kw_only=True)
class BareArguments(Arguments):
    pass


@dataclass(frozen=True, kw_only=True)
class SampleArguments(CommandArguments):
    request: Path = option(help="Path to the request file.")
    bundle_folder: Path = option(help="Folder of the bundle.")
    back_off_seconds: int = option(default=30, help="Seconds between scans.")


def parse(values: list[str]) -> SampleArguments:
    return SampleArguments.parser("sample", "Sample command.").parse(values)


def test_options_arrive_typed_under_their_field_names():
    arguments = parse([
        *COMMON,
        "--request",
        "/tmp/request.yaml",
        "--bundle-folder",
        "/tmp/bundle",
        "--back-off-seconds",
        "5",
    ])
    assert isinstance(arguments, SampleArguments)
    assert arguments.request == Path("/tmp/request.yaml")
    assert arguments.bundle_folder == Path("/tmp/bundle")
    assert arguments.back_off_seconds == 5
    assert arguments.liveness == Path("/tmp/live")
    assert arguments.journal_file == Path("/tmp/journal.yaml")
    assert arguments.responder_method == "POST"


def test_optional_options_take_their_defaults():
    arguments = parse([*COMMON, "--request", "r.yaml", "--bundle-folder", "b"])
    assert arguments.back_off_seconds == 30
    assert arguments.work_folder is None
    assert arguments.shared_folder is None
    assert arguments.logging_configuration is None


def test_common_options_parse_under_their_field_flags():
    arguments = parse([
        *COMMON,
        "--request",
        "r.yaml",
        "--bundle-folder",
        "b",
        "--work-folder",
        "/tmp/work",
        "--shared-folder",
        "/tmp/shared",
        "--logging-configuration",
        "/tmp/logging.yaml",
    ])
    assert arguments.work_folder == Path("/tmp/work")
    assert arguments.shared_folder == Path("/tmp/shared")
    assert arguments.logging_configuration == Path("/tmp/logging.yaml")


def test_short_flags_still_work():
    values = [*COMMON[2:], "-l", "/tmp/short", "--request", "r", "--bundle-folder", "b"]
    assert parse(values).liveness == Path("/tmp/short")


def test_a_missing_required_option_is_an_invalid_option():
    with pytest.raises(InvalidOptionsError, match="--bundle-folder"):
        parse([*COMMON, "--request", "r.yaml"])


def test_arguments_are_immutable():
    arguments = parse([*COMMON, "--request", "r.yaml", "--bundle-folder", "b"])
    with pytest.raises(FrozenInstanceError):
        arguments.request = Path("other")  # type: ignore[misc]


def test_every_flag_is_its_field_name():
    parser = SampleArguments.parser("sample", "Sample command.")
    for item in fields(SampleArguments):
        long = f"--{item.name.replace('_', '-')}"
        action = parser._option_string_actions[long]
        assert action.dest == item.name
        assert action.option_strings[0] == long
        short = item.metadata[OPTION].short
        assert action.option_strings[1:] == ([] if short is None else [short])


def test_option_takes_no_explicit_flag():
    with pytest.raises(TypeError):
        option("--bundle-folder", help="Folder of the bundle.")  # type: ignore[misc]


def test_an_empty_subclass_parses_only_the_common_options():
    arguments = EmptyArguments.parser("empty", "Empty command.").parse(COMMON)
    assert isinstance(arguments, EmptyArguments)
    assert {item.name for item in fields(EmptyArguments)} == {
        item.name for item in fields(CommandArguments)
    }
    assert arguments.liveness == Path("/tmp/live")
    assert arguments.journal_file == Path("/tmp/journal.yaml")
    with pytest.raises(InvalidOptionsError, match="--request"):
        EmptyArguments.parser("empty", "Empty command.").parse([
            *COMMON,
            "--request",
            "r.yaml",
        ])


def test_an_empty_subclass_still_requires_the_common_options():
    with pytest.raises(InvalidOptionsError, match="--liveness"):
        EmptyArguments.parser("empty", "Empty command.").parse([])


def test_a_bare_arguments_class_has_no_common_options():
    parser = BareArguments.parser("bare", "Bare command.")
    assert isinstance(parser.parse([]), BareArguments)
    with pytest.raises(InvalidOptionsError, match="--liveness"):
        parser.parse(["--liveness", "/tmp/live"])


def test_a_bare_arguments_class_prints_its_version(capsys):
    parser = BareArguments.parser("bare", "Bare command.", version="1.2.3")
    with pytest.raises(SystemExit) as exit_event:
        parser.parse(["--version"])
    assert exit_event.value.code == 0
    assert capsys.readouterr().out.strip() == "1.2.3"

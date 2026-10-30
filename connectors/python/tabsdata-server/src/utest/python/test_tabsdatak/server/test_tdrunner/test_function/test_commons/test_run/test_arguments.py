#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import FrozenInstanceError, dataclass
from pathlib import Path

import pytest

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
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
class SampleArguments(CommandArguments):
    request: Path = option(help="Path to the request file.")
    bundle_folder: Path = option("--bundle-dir", help="Folder of the bundle.")
    back_off_seconds: int = option(default=30, help="Seconds between scans.")


def parse(values: list[str]) -> SampleArguments:
    return SampleArguments.parser("sample", "Sample command.").parse(values)


def test_options_arrive_typed_under_their_field_names():
    arguments = parse([
        *COMMON,
        "--request",
        "/tmp/request.yaml",
        "--bundle-dir",
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
    arguments = parse([*COMMON, "--request", "r.yaml", "--bundle-dir", "b"])
    assert arguments.back_off_seconds == 30
    assert arguments.work_folder is None
    assert arguments.shared_folder is None
    assert arguments.logging_configuration is None


def test_common_options_keep_their_flags():
    arguments = parse([
        *COMMON,
        "--request",
        "r.yaml",
        "--bundle-dir",
        "b",
        "--work-dir",
        "/tmp/work",
        "--shared-dir",
        "/tmp/shared",
        "--log-config",
        "/tmp/logging.yaml",
    ])
    assert arguments.work_folder == Path("/tmp/work")
    assert arguments.shared_folder == Path("/tmp/shared")
    assert arguments.logging_configuration == Path("/tmp/logging.yaml")


def test_short_flags_still_work():
    values = [*COMMON[2:], "-l", "/tmp/short", "--request", "r", "--bundle-dir", "b"]
    assert parse(values).liveness == Path("/tmp/short")


def test_a_missing_required_option_is_an_invalid_option():
    with pytest.raises(InvalidOptionsError, match="--bundle-dir"):
        parse([*COMMON, "--request", "r.yaml"])


def test_arguments_are_immutable():
    arguments = parse([*COMMON, "--request", "r.yaml", "--bundle-dir", "b"])
    with pytest.raises(FrozenInstanceError):
        arguments.request = Path("other")  # type: ignore[misc]

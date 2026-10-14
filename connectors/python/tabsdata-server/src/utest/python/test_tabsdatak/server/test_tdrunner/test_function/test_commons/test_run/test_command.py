#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse

import pytest

from tabsdatak.common._enums import WorkerCallbackStatus
from tabsdatak.server._tdrunner.function._commons.run import command as command_module
from tabsdatak.server._tdrunner.function._commons.run.command import (
    build_parser,
    notify_status,
    notify_step_destination_write,
    run_command,
)
from tabsdatak.server._tdrunner.function._lib.errors import (
    CommandErrorCode,
    InvalidOptionsError,
)
from tabsdatak.server._tdrunner.function._lib.message import ResponseMessagePayload

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- build_parser ---


def test_build_parser_returns_argparse_parser():
    parser = build_parser("cmd", "desc")
    assert isinstance(parser, argparse.ArgumentParser)


def test_build_parser_required_liveness_long():
    parser = build_parser("cmd", "desc")
    args = parser.parse_args([
        "--liveness",
        "/tmp/x",
        "--responder-url",
        "http://localhost/responder",
        "--responder-method",
        "POST",
        "--responder-authorization",
        "Bearer tok",
        "--journal-file",
        "/tmp/em.yaml",
    ])
    assert args.liveness == "/tmp/x"


def test_build_parser_required_liveness_short():
    parser = build_parser("cmd", "desc")
    args = parser.parse_args([
        "-l",
        "/tmp/x",
        "--responder-url",
        "http://localhost/responder",
        "--responder-method",
        "POST",
        "--responder-authorization",
        "Bearer tok",
        "--journal-file",
        "/tmp/em.yaml",
    ])
    assert args.liveness == "/tmp/x"


def test_build_parser_missing_liveness_raises():
    parser = build_parser("cmd", "desc")
    with pytest.raises(InvalidOptionsError):
        parser.parse_args([])


def test_build_parser_unknown_flag_raises():
    parser = build_parser("cmd", "desc")
    with pytest.raises(InvalidOptionsError):
        parser.parse_args(["--no-such-flag"])


def test_build_parser_help_short_circuits(capsys):
    parser = build_parser("cmd", "the description here")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--help"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "the description here" in captured.out


def test_build_parser_help_short_form(capsys):
    parser = build_parser("cmd", "the description here")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["-h"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "the description here" in captured.out


def test_build_parser_man_short_circuits(capsys):
    parser = build_parser("cmd", "desc", manual="man text here")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--man"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "man text here" in captured.out


def test_build_parser_man_short_form(capsys):
    parser = build_parser("cmd", "desc", manual="MAN")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["-m"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "MAN" in captured.out


def test_build_parser_version_short_circuits(capsys):
    parser = build_parser("cmd", "desc", version="1.2.3")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--version"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "1.2.3" in captured.out


def test_build_parser_version_short_form(capsys):
    parser = build_parser("cmd", "desc", version="1.2.3")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["-v"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "1.2.3" in captured.out


def test_build_parser_info_short_circuits(capsys):
    parser = build_parser("cmd", "desc", info="build XYZ")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--info"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "build XYZ" in captured.out


def test_build_parser_info_short_form(capsys):
    parser = build_parser("cmd", "desc", info="INFO")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["-i"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "INFO" in captured.out


# --- run_command ---


def test_run_command_main_returns_none():
    assert run_command(lambda: None) == 0


def test_run_command_main_returns_zero():
    assert run_command(lambda: 0) == 0


def test_run_command_main_returns_int():
    assert run_command(lambda: 7) == 7


def test_run_command_invalid_options(capsys):
    def main():
        raise CommandErrorCode.CMD_1.exception(detail="bad args")

    assert run_command(main) == 1
    captured = capsys.readouterr()
    assert "bad args" in captured.err


def test_run_command_undefined_env(capsys):
    def main():
        raise CommandErrorCode.CMD_2.exception(var="FOO")

    assert run_command(main) == 2
    captured = capsys.readouterr()
    assert "FOO" in captured.err


# --- callback bodies ---


def _posted(monkeypatch) -> list[dict]:
    bodies: list[dict] = []
    monkeypatch.setattr(
        command_module,
        "notify",
        lambda _arguments, body, _what: bodies.append(body),
    )
    return bodies


def test_a_status_report_names_no_operation(monkeypatch):
    bodies = _posted(monkeypatch)
    notify_status(
        argparse.Namespace(),
        ResponseMessagePayload(status=WorkerCallbackStatus.RUNNING),
    )
    assert "op" not in bodies[0]


def test_a_destination_write_names_its_operation_only(monkeypatch):
    bodies = _posted(monkeypatch)
    notify_step_destination_write(argparse.Namespace())
    assert bodies == [{"op": "destination_write"}]

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from tabsdatak.common._enums import WorkerCallbackStatus
from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    Arguments,
    ArgumentsParser,
    CommandArguments,
)
from tabsdatak.server._tdrunner.function._commons.run.command import (
    Responder,
    command,
    run_command,
    show_dataflow_settings,
)
from tabsdatak.server._tdrunner.function._commons.run.dataflow import (
    POLARS_OOC_SPILL_DIR,
    POLARS_TEMP_DIR,
    POLARS_VARIABLES,
)
from tabsdatak.server._tdrunner.function._commons.error.command import (
    CommandErrorCode,
    InvalidOptionsError,
)
from tabsdatak.server._tdrunner.function._commons.document.message.callback import CallbackRequest

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- CommandArguments.parser ---


def test_parser_returns_arguments_parser():
    parser = CommandArguments.parser("cmd", "desc")
    assert isinstance(parser, ArgumentsParser)


def test_parser_required_liveness_long():
    parser = CommandArguments.parser("cmd", "desc")
    arguments = parser.parse([
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
    assert arguments.liveness == Path("/tmp/x")


def test_parser_required_liveness_short():
    parser = CommandArguments.parser("cmd", "desc")
    arguments = parser.parse([
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
    assert arguments.liveness == Path("/tmp/x")


def test_parser_missing_liveness_raises():
    parser = CommandArguments.parser("cmd", "desc")
    with pytest.raises(InvalidOptionsError):
        parser.parse([])


def test_parser_unknown_flag_raises():
    parser = CommandArguments.parser("cmd", "desc")
    with pytest.raises(InvalidOptionsError):
        parser.parse(["--no-such-flag"])


def test_parser_help_short_circuits(capsys):
    parser = CommandArguments.parser("cmd", "the description here")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse(["--help"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "the description here" in captured.out


def test_parser_help_short_form(capsys):
    parser = CommandArguments.parser("cmd", "the description here")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse(["-h"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "the description here" in captured.out


def test_parser_man_short_circuits(capsys):
    parser = CommandArguments.parser("cmd", "desc", manual="man text here")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse(["--man"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "man text here" in captured.out


def test_parser_man_short_form(capsys):
    parser = CommandArguments.parser("cmd", "desc", manual="MAN")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse(["-m"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "MAN" in captured.out


def test_parser_version_short_circuits(capsys):
    parser = CommandArguments.parser("cmd", "desc", version="1.2.3")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse(["--version"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "1.2.3" in captured.out


def test_parser_version_short_form(capsys):
    parser = CommandArguments.parser("cmd", "desc", version="1.2.3")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse(["-v"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "1.2.3" in captured.out


def test_parser_info_short_circuits(capsys):
    parser = CommandArguments.parser("cmd", "desc", information="build XYZ")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse(["--info"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "build XYZ" in captured.out


def test_parser_info_short_form(capsys):
    parser = CommandArguments.parser("cmd", "desc", information="INFO")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse(["-i"])
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


# --- standalone commands ---


def standalone(body):
    return command(
        name="td_tool",
        parser=lambda: Arguments.parser("tool", "A standalone tool."),
        reports=False,
    )(body)


def test_a_standalone_command_takes_only_its_own_options(monkeypatch):
    received: list[Arguments] = []
    monkeypatch.setattr(sys, "argv", ["tool"])
    main = standalone(lambda arguments: received.append(arguments) or 3)
    assert main() == 3
    assert isinstance(received[0], Arguments)
    assert main.tabsdata_command_name == "td_tool"


def test_a_failing_standalone_command_exits_with_its_code(monkeypatch, capsys):
    def failing(arguments):
        raise CommandErrorCode.CMD_1.exception(detail="bad option")

    monkeypatch.setattr(sys, "argv", ["tool"])
    assert standalone(failing)() == InvalidOptionsError.exit_code
    assert "bad option" in capsys.readouterr().err


# --- callback bodies ---


def _arguments() -> CommandArguments:
    return CommandArguments(
        liveness=Path("/l"),
        responder_url="http://localhost/responder",
        responder_method="POST",
        responder_authorization="Bearer tok",
        journal_file=Path("/j"),
    )


def _posted(monkeypatch) -> list[dict]:
    bodies: list[dict] = []
    monkeypatch.setattr(
        Responder,
        "notify",
        lambda _responder, body, _what: bodies.append(body),
    )
    return bodies


def test_a_status_report_names_no_operation(monkeypatch):
    bodies = _posted(monkeypatch)
    Responder(_arguments()).report(
        CallbackRequest(status=WorkerCallbackStatus.RUNNING),
    )
    assert "op" not in bodies[0]


def test_a_destination_write_names_its_operation_only(monkeypatch):
    bodies = _posted(monkeypatch)
    Responder(_arguments()).destination_write()
    assert bodies == [{"op": "destination_write"}]


def test_a_command_logs_the_query_engine_settings_in_effect(monkeypatch, caplog):
    for name in POLARS_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv(POLARS_TEMP_DIR, "/dataflow/temporary")
    monkeypatch.setenv(POLARS_OOC_SPILL_DIR, "/dataflow/spill")

    show_dataflow_settings()

    assert caplog.messages == [
        f"Query engine setting {POLARS_TEMP_DIR} is '/dataflow/temporary'",
        f"Query engine setting {POLARS_OOC_SPILL_DIR} is '/dataflow/spill'",
    ]


def test_a_command_says_when_the_query_engine_keeps_its_defaults(monkeypatch, caplog):
    for name in POLARS_VARIABLES:
        monkeypatch.delenv(name, raising=False)

    show_dataflow_settings()

    assert caplog.messages == [
        "The query engine runs on its own memory and spill defaults"
    ]

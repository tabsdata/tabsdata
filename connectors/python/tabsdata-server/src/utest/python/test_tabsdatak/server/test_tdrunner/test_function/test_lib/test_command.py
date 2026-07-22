#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse

import pytest

from tabsdatak.server._tdrunner.function._lib.command import (
    build_parser,
    resolve_flag,
    resolve_value,
    run_command,
)
from tabsdatak.server._tdrunner.function._lib.errors import (
    CommandErrorCode,
    InvalidOptionsError,
    UndefinedEnvError,
)

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


# --- resolve_value ---


def test_resolve_value_literal():
    assert resolve_value("hello") == "hello"


def test_resolve_value_empty_string():
    assert resolve_value("") == ""


def test_resolve_value_val_prefix_strips():
    assert resolve_value("val:hello") == "hello"


def test_resolve_value_val_prefix_strips_only_first():
    # `val:` forces the rest to be a literal even if it looks like `env:`
    assert resolve_value("val:env:foo") == "env:foo"


def test_resolve_value_val_prefix_empty_payload():
    assert resolve_value("val:") == ""


def test_resolve_value_env_resolves(monkeypatch):
    monkeypatch.setenv("TD_TEST_RESOLVE", "the-value")
    assert resolve_value("env:TD_TEST_RESOLVE") == "the-value"


def test_resolve_value_env_unset_raises(monkeypatch):
    monkeypatch.delenv("TD_TEST_RESOLVE_NOPE", raising=False)
    with pytest.raises(UndefinedEnvError) as exc_info:
        resolve_value("env:TD_TEST_RESOLVE_NOPE")
    assert "TD_TEST_RESOLVE_NOPE" in str(exc_info.value)
    assert exc_info.value.exit_code == 2


# --- resolve_flag ---


@pytest.mark.parametrize("arg", ["true", "TRUE", "True", "tRuE"])
def test_resolve_flag_true_case_insensitive(arg):
    assert resolve_flag(arg) is True


@pytest.mark.parametrize("arg", ["false", "FALSE", "False", "fAlSe"])
def test_resolve_flag_false_case_insensitive(arg):
    assert resolve_flag(arg) is False


def test_resolve_flag_via_val_prefix_true():
    assert resolve_flag("val:true") is True


def test_resolve_flag_via_val_prefix_false():
    assert resolve_flag("val:false") is False


def test_resolve_flag_via_env(monkeypatch):
    monkeypatch.setenv("TD_TEST_FLAG", "TRUE")
    assert resolve_flag("env:TD_TEST_FLAG") is True


def test_resolve_flag_via_env_unset(monkeypatch):
    monkeypatch.delenv("TD_TEST_NOFLAG", raising=False)
    with pytest.raises(UndefinedEnvError):
        resolve_flag("env:TD_TEST_NOFLAG")


@pytest.mark.parametrize("arg", ["yes", "no", "1", "0", "", "y", "n", "trueish"])
def test_resolve_flag_invalid_value(arg):
    with pytest.raises(InvalidOptionsError) as exc_info:
        resolve_flag(arg)
    assert exc_info.value.exit_code == 1


def test_resolve_flag_val_prefix_invalid_payload():
    # `val:env:true` -> literal "env:true" -> not a valid flag value
    with pytest.raises(InvalidOptionsError):
        resolve_flag("val:env:true")


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
    parser = build_parser("cmd", "desc", man="man text here")
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--man"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "man text here" in captured.out


def test_build_parser_man_short_form(capsys):
    parser = build_parser("cmd", "desc", man="MAN")
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



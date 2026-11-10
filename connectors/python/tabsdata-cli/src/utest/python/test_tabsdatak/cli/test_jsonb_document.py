#
# Copyright 2026 Tabsdata Inc.
#

"""A person's ``-o jsonb`` speaks the same document as a machine run: the
result, or the error document, on stdout."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from tabsdatak.cli._error_document import error_document
from tabsdatak.cli._errors import ExitCode, TdkException
from tabsdatak.cli.td.cli import cli


@pytest.mark.parametrize(
    "argv, kind, code",
    [
        (["docs", "show", "--name", "NoSuchSymbol"], "not_found", ExitCode.NOT_FOUND),
        (["user", "list", "--cursor", "not-a-cursor"], "usage", ExitCode.USAGE),
        (["docs", "list", "--max-result", "-1"], "usage", ExitCode.USAGE),
    ],
)
def test_a_failure_is_the_error_document_on_stdout(argv, kind, code):
    result = CliRunner().invoke(cli, [*argv, "-o", "jsonb"])
    assert result.exit_code == code, result.output
    error = json.loads(result.stdout)["error"]
    assert error["type"] == kind
    assert error["command"] == ["tdk", *argv, "-o", "jsonb"]
    assert "╭" not in result.output


def test_the_error_state_speaks_the_document_times():
    state = {"triggered_on": 1794242398995, "runs": [{"ended_on": None}]}
    error = TdkException("wait_timeout", "still running", state=state)
    document = error_document(error.error, ["tdk", "plan", "get"])
    assert document["error"]["state"] == {
        "triggered_on": "2026-11-09T16:39:58.995Z",
        "runs": [{"ended_on": None}],
    }


def test_secrets_in_the_command_are_redacted():
    argv = ["login", "--server", "http://127.0.0.1:1", "--password", "s3cret"]
    result = CliRunner().invoke(cli, [*argv, "--max-result", "1", "-o", "jsonb"])
    document = json.loads(result.stdout)
    assert "s3cret" not in result.stdout
    assert "***" in document["error"]["command"]


def test_other_formats_keep_the_error_box():
    result = CliRunner().invoke(cli, ["docs", "show", "--name", "NoSuchSymbol", "-o", "json"])
    assert result.exit_code == ExitCode.NOT_FOUND
    assert result.stdout == ""


def test_a_prompt_without_a_terminal_stays_off_stdout():
    result = CliRunner().invoke(cli, ["docs", "search", "-o", "jsonb"], input="sum\n")
    assert result.exit_code == 0, result.output
    assert "Words to search for" not in result.stdout
    assert "items" in json.loads(result.stdout)

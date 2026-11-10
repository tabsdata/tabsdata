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


def test_server_times_become_iso_and_table_rows_stay_as_they_are():
    from tabsdatak.cli.cli_utils import Verbatim, jsonb_document

    millis = 1750074554472
    document = jsonb_document({
        "created_at": millis,
        "retry_at": millis,
        "started_on": millis,
        "count_at": "x",
        "rows": Verbatim([{"created_at": millis, "seen_on": millis}]),
    })
    iso = "2025-06-16T11:49:14.472Z"
    assert document["created_at"] == document["retry_at"] == iso
    assert document["started_on"] == iso and document["count_at"] == "x"
    assert document["rows"] == [{"created_at": millis, "seen_on": millis}]


def test_a_listing_states_the_order_the_server_sorted_it_in():
    from unittest.mock import MagicMock

    import rich_click as click

    from tabsdatak.cli.cli_utils import list_order, take

    ctx = click.Context(click.Command("x"), obj={})
    rows = MagicMock()
    rows.__iter__.return_value = iter([])
    rows.sorted_by = "name-"
    take(ctx, rows)
    assert list_order(ctx) == "name desc"
    ctx.obj["list_sorted_by"] = None
    ctx.obj["list_requested_order"] = "triggered_on"
    assert list_order(ctx) == "triggered_on asc"
    ctx.obj["list_requested_order"] = None
    assert list_order(ctx) is None


def test_record_fields_on_a_listing_keep_the_fields_of_its_items():
    import rich_click as click

    from tabsdatak.cli.cli_utils import record_fields_option, render_list

    @click.command()
    @record_fields_option
    @click.pass_context
    def listed(ctx):
        ctx.obj["output_format"] = "jsonb"
        render_list(ctx, items=[{"id": "fv1", "name": "f"}], truncated=False)

    result = CliRunner().invoke(listed, ["--fields", "id"], obj={})
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["items"] == [{"id": "fv1"}]


def test_a_status_count_by_code_is_left_out_beside_the_one_by_name():
    from tabsdatak.cli.cli_utils import jsonb_document

    document = jsonb_document({
        "function_run_status_count": {"C": 1, "F": 0},
        "function_runs_by_status": {"Committed": 1},
    })
    assert document == {"function_runs_by_status": {"Committed": 1}}
    assert jsonb_document({"function_run_status_count": {"C": 1}}) == {
        "function_run_status_count": {"C": 1}
    }


def test_a_program_listing_rows_gets_them_without_internal_ids(machine_settings):
    import rich_click as click

    from tabsdatak.cli._machine import Machine, running
    from tabsdatak.cli.cli_utils import list_options, render_list

    row = {"id": "ep1", "status_name": "Finished", "topology_version_id": "t", "physical_status": "D"}
    emitted = []

    @click.command()
    @list_options
    @click.pass_context
    def listed(ctx):
        ctx.obj["output_format"] = "jsonb"
        render_list(ctx, items=[row], truncated=False)

    def run(*argv):
        with running(Machine(emit=emitted.append)):
            result = CliRunner().invoke(listed, list(argv), obj={})
        assert result.exit_code == 0, result.output
        return emitted.pop()

    slim = run()
    assert slim["items"] == [{"id": "ep1", "status_name": "Finished"}]
    assert slim["fields_omitted"] == ["topology_version_id", "physical_status"]
    full = run("--fields", "all")
    assert full["items"] == [row] and "fields_omitted" not in full


def test_a_server_failure_names_the_request_id_the_server_logged():
    from tabsdatak.cli._errors import classify
    from tabsdatak.sdk._apiserver import APIServerError

    error = classify(
        APIServerError(
            {"code": "X::5000", "error_description": "An internal error occurred"},
            status=500,
            request_id="rid1",
        )
    )
    assert error.extra["request_id"] == "rid1"
    assert "request id rid1" in error.hint and "X::5000" in error.hint

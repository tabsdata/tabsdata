#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from unittest.mock import MagicMock, patch

import requests
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer


def test_a_monitor_error_after_the_trigger_is_not_reported_as_a_failed_trigger(
    tmp_path,
):
    """`tdk fn trigger` commits the trigger, and the apiserver then drops the
    connection while the plan is being monitored. The plan exists and runs,
    so the command must not report "Failed to trigger function", which
    invites a second trigger and a duplicate plan."""
    execution = MagicMock(id="P1", status="R", function_runs=[])
    execution.refresh.side_effect = requests.ConnectionError("connection reset")
    client = _client()
    client.trigger_function.return_value = execution

    result = _trigger(client, tmp_path)

    client.trigger_function.assert_called_once()
    assert "Failed to trigger function" not in result.output, result.output


def test_a_trigger_whose_answer_is_lost_says_it_may_have_run(tmp_path):
    client = _client()
    client.trigger_function.side_effect = requests.ReadTimeout("read timed out")

    result = _trigger(client, tmp_path)

    assert "Failed to trigger function" not in result.output, result.output
    assert "may have been triggered" in result.output, result.output


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    return client


def _trigger(client: MagicMock, tmp_path):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
        patch("tabsdatak.cli.td.fn_group.sleep", lambda seconds: None),
    ):
        return CliRunner().invoke(cli, ["fn", "trigger", "-n", "f1", "-c", "c1"])

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk._apiserver import NotFound
from tabsdatak.sdk.tabsdata_server import TabsdataServer

_DOCUMENT = {
    "kind": "plan",
    "plan": {
        "id": "ep20260930-1",
        "name": "daily",
        "status": "L",
        "status_name": "Stalled",
        "health": "warning",
        "final": True,
    },
    "summary": {
        "runs": 3,
        "by_health": {"ok": 1, "running": 0, "warning": 1, "error": 1},
        "held": 1,
    },
    "root_cause": {
        "function_run_id": "fr20260930-1",
        "collection": "sales",
        "function": "clean_orders",
        "category": "schema_mismatch",
        "code": "key_error",
        "message": "KeyError: 'country'",
        "user_frame": {"module": "clean.py", "line": 42, "function": "clean_orders"},
        "hint": "Compare the input schema.",
    },
    "failed_runs": [
        {
            "function_run_id": "fr20260930-1",
            "collection": "sales",
            "function": "clean_orders",
            "status_name": "Failed",
            "attempts": 2,
            "worker_id": "W1",
            "signature": {
                "category": "schema_mismatch",
                "message": "KeyError: 'country'",
            },
            "log_excerpt": "Traceback (most recent call last):\nKeyError: 'country'",
            "argv": {"replay": ["fnrun", "replay", "--fnrun", "fr20260930-1", "--wait"]},
        }
    ],
    "held_runs": [
        {
            "function_run_id": "R2",
            "collection": "bi",
            "function": "report",
            "blocked_by": ["fr20260930-1"],
            "blocked_by_outside_plan": False,
        }
    ],
    "truncated": {"failed_runs_shown": 1, "failed_runs_total": 2},
}


def _invoke(argv, client, tmp_path):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv, env={"COLUMNS": "250"})


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    client.connection.project = "p"
    diagnosis = SimpleNamespace(to_dict=lambda: _DOCUMENT)
    client.diagnose_execution.return_value = diagnosis
    client.diagnose_function_run.return_value = diagnosis
    return client


def test_plan_diagnose_json_is_the_diagnosis(tmp_path):
    client = _client()
    result = _invoke(
        ["plan", "diagnose", "--plan", "ep20260930-1", "--max-runs", "3", "--no-logs"]
        + ["-o", "json"],
        client,
        tmp_path,
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == _DOCUMENT
    client.diagnose_execution.assert_called_once_with(
        "ep20260930-1", max_runs=3, log_lines=200, logs=False
    )
    assert client.connection.project is None


def test_plan_diagnose_table(tmp_path):
    result = _invoke(["plan", "diagnose", "--plan", "ep20260930-1"], _client(), tmp_path)
    assert result.exit_code == 0, result.output
    assert "Plan ep20260930-1 daily: Stalled (warning)" in result.output
    assert "Root cause [schema_mismatch]: KeyError: 'country'" in result.output
    assert "at clean.py:42 in clean_orders" in result.output
    assert "replay: tdk fnrun replay --fnrun fr20260930-1 --wait" in result.output
    assert "Showing 1 of 2 failed runs" in result.output
    assert "R2" in result.output


def test_unknown_plan_exits_not_found(tmp_path):
    client = _client()
    client.diagnose_execution.side_effect = NotFound("Execution with ID X not found.")
    result = _invoke(["plan", "diagnose", "--plan", "ep20260930-9", "--machine"], client, tmp_path)
    assert result.exit_code == 5, result.output
    assert json.loads(result.stdout)["error"]["type"] == "not_found"


def test_fnrun_diagnose(tmp_path):
    client = _client()
    result = _invoke(
        ["fnrun", "diagnose", "--fnrun", "fr20260930-1", "--log-lines", "10", "-o", "json"],
        client,
        tmp_path,
    )
    assert result.exit_code == 0, result.output
    client.diagnose_function_run.assert_called_once_with("fr20260930-1", log_lines=10, logs=True)

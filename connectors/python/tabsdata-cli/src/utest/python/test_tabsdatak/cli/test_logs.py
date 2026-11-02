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


def _entry(worker_id: str, **overrides) -> dict:
    entry = {
        "worker_id": worker_id,
        "function_run_id": "fr20260930-1",
        "collection": "sales",
        "function": "clean_orders",
        "status": "F",
        "status_name": "Failed",
        "health": "error",
        "attempt": 1,
        "lines_total": 2,
        "log_truncated": False,
        "log_unavailable": None,
        "signature": {
            "category": "schema_mismatch",
            "code": "key_error",
            "message": "KeyError: 'country'",
            "user_frame": None,
        },
        "log": f"log of {worker_id}",
    }
    entry.update(overrides)
    return entry


def _worker(id: str, status: str, run: str = "fr20260930-1"):
    return SimpleNamespace(
        id=id,
        project="p",
        status=status,
        started_on_str=None,
        ended_on_str=None,
        collection=SimpleNamespace(name="sales"),
        function=SimpleNamespace(name="clean_orders"),
        kwargs={"status": status, "logical_function_run_id": run},
    )


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    client.connection.project = "p"
    return client


def _invoke(argv, client, tmp_path, env=None):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv, env={"COLUMNS": "250", **(env or {})})


def test_plan_logs_all_is_the_list_envelope(tmp_path):
    client = _client()
    client.scope_logs.return_value = ([_entry("W2"), _entry("W1")], True)
    result = _invoke(
        ["plan", "logs", "--plan", "ep20260930-1", "--all", "--tail", "50", "--failed-only"]
        + ["-o", "json"],
        client,
        tmp_path,
    )
    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert [e["worker_id"] for e in document["items"]] == ["W2", "W1"]
    assert document["truncated"] is True
    assert document["next"] is None
    client.scope_logs.assert_called_once_with(
        plan="ep20260930-1", tail=50, failed_only=True, max_workers=20
    )


def test_max_result_bounds_the_workers(tmp_path):
    client = _client()
    client.scope_logs.return_value = ([], False)
    for argv, cap in (
        (["trx", "logs", "--trx", "tx20260930-1", "--all", "--max-result", "3"], 3),
        (["fnrun", "logs", "--fnrun", "fr20260930-1", "--all", "--max-result", "0"], 0),
    ):
        result = _invoke(argv + ["-o", "json"], client, tmp_path)
        assert result.exit_code == 0, result.output
        assert client.scope_logs.call_args.kwargs["max_workers"] == cap


def test_all_table_view_shows_each_log_and_its_signature(tmp_path):
    client = _client()
    client.scope_logs.return_value = (
        [
            _entry("W2"),
            _entry("W1", log=None, log_unavailable="forbidden", signature=None),
        ],
        False,
    )
    result = _invoke(["plan", "logs", "--plan", "ep20260930-1", "--all"], client, tmp_path)
    assert result.exit_code == 0, result.output
    assert "Worker W2  sales/clean_orders  Failed  attempt 1" in result.output
    assert "log of W2" in result.output
    assert "Error [schema_mismatch]: KeyError: 'country'" in result.output
    assert "(log unavailable: forbidden)" in result.output


def test_all_writes_every_log_to_the_file(tmp_path):
    client = _client()
    client.scope_logs.return_value = ([_entry("W2"), _entry("W1")], False)
    target = tmp_path / "logs.txt"
    result = _invoke(
        ["trx", "logs", "--trx", "tx20260930-1", "--all", "--file", str(target)],
        client,
        tmp_path,
    )
    assert result.exit_code == 0, result.output
    text = target.read_text()
    assert "log of W2" in text and "log of W1" in text


def test_failed_only_narrows_the_picker(tmp_path):
    client = _client()
    client.list_workers.return_value = [
        _worker("A" * 25 + "1", "D"),
        _worker("A" * 25 + "2", "F"),
    ]
    client.worker_log_entry.side_effect = lambda worker, **kw: _entry(
        worker.id, attempt=kw["attempt"]
    )
    result = _invoke(
        ["plan", "logs", "--plan", "ep20260930-1", "--failed-only", "-o", "json"],
        client,
        tmp_path,
    )
    assert result.exit_code == 0, result.output
    (entry,) = json.loads(result.stdout)["items"]
    assert entry["worker_id"] == "A" * 25 + "2"
    assert entry["attempt"] == 2


def test_failed_only_without_failures_says_so(tmp_path):
    client = _client()
    client.list_workers.return_value = [_worker("A" * 26, "D")]
    result = _invoke(
        ["plan", "logs", "--plan", "ep20260930-1", "--failed-only"], client, tmp_path
    )
    assert result.exit_code == 1
    assert "no workers whose health is warning or error" in result.output


def test_several_workers_without_all_point_at_all(tmp_path):
    client = _client()
    client.list_workers.return_value = [_worker("A" * 26, "F"), _worker("B" * 26, "D")]
    result = _invoke(
        ["--no-prompt", "fnrun", "logs", "--fnrun", "fr20260930-1"], client, tmp_path
    )
    assert result.exit_code == 2
    assert "Pass '--all'" in result.output
    client.worker_log_entry.assert_not_called()


def test_worker_logs_tail_and_not_found(tmp_path):
    client = _client()
    client.worker_log_entry.return_value = _entry("W1")
    result = _invoke(
        ["worker", "logs", "--worker", "wkr20260301-1", "--tail", "5", "-o", "json"],
        client,
        tmp_path,
    )
    assert result.exit_code == 0, result.output
    client.worker_log_entry.assert_called_once_with("wkr20260301-1", tail=5)
    assert json.loads(result.stdout)["items"] == [_entry("W1")]

    client.worker_log_entry.side_effect = NotFound("Worker with ID W9 not found.")
    result = _invoke(["worker", "logs", "--worker", "wkr20260301-9"], client, tmp_path)
    assert result.exit_code == 5, result.output


def test_a_typed_worker_outside_the_listing_is_not_found(tmp_path):
    client = _client()
    client.list_workers.return_value = [_worker("A" * 26, "F"), _worker("B" * 26, "D")]

    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        result = CliRunner().invoke(
            cli,
            ["fnrun", "logs", "--fnrun", "fr20260930-1"],
            input="C" * 26 + "\n",
            env={"COLUMNS": "250"},
        )
    assert result.exit_code == 5, result.output
    client.worker_log_entry.assert_not_called()


def test_machine_logs_have_a_budget_over_the_whole_document(
    tmp_path, machine_settings
):
    machine_settings.update(log_cap_bytes=256 * 1024, logs_budget_bytes=2 * 1024 * 1024)
    client = _client()
    entries = [_entry(f"W{i}", log="x" * (300 * 1024)) for i in range(10)]
    client.scope_logs.return_value = (entries, False)
    result = _invoke(["plan", "logs", "--plan", "ep20260930-1", "--all", "--machine"], client, tmp_path)
    assert result.exit_code == 0, result.output
    items = json.loads(result.stdout)["items"]
    sizes = [len(item["log"].encode()) for item in items]
    assert sum(sizes) == 2 * 1024 * 1024
    assert max(sizes) == 256 * 1024 and sizes[-1] == 0
    assert all(item["log_truncated"] for item in items)

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import base64
import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli._errors import ExitCode
from tabsdatak.cli.td import _wait
from tabsdatak.cli.td.cli import cli

# noinspection PyProtectedMember
from tabsdatak.sdk import tabsdata_server as ts


class _Clock:
    def __init__(self):
        self.now = 1_000_000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch) -> _Clock:
    fake = _Clock()
    monkeypatch.setattr(ts, "time", fake)
    monkeypatch.setattr(_wait, "time", fake)
    return fake


def _listing(rows: list[dict]) -> MagicMock:
    response = MagicMock()
    response.json.return_value = {"data": {"data": rows}}
    return response


def _run_row(run_id: str, status: str) -> dict:
    return {
        "id": run_id,
        "status": status,
        "collection": "c1",
        "name": "f1",
        "transaction_id": "tx20260930-1",
        "execution_id": "ep20260930-1",
    }


def _connection(plan_statuses: list[str], runs: list[dict]) -> MagicMock:
    connection = MagicMock()
    connection.timeout = (10, 60)
    connection.project = "p1"
    statuses = iter(plan_statuses)
    last = [plan_statuses[0]]

    def _plans(*_, **__):
        last[0] = next(statuses, last[0])
        return _listing([{"id": "ep20260930-1", "status": last[0]}])

    connection.execution_list.side_effect = _plans
    connection.function_list_runs.return_value = _listing(runs)
    return connection


def _client(connection: MagicMock) -> MagicMock:
    client = MagicMock(spec=ts.TabsdataServer)
    client.connection = connection
    execution = ts.Execution(connection, "ep20260930-1")
    client.trigger_function.return_value = execution
    client.get_execution.return_value = execution
    client.list_function_runs.return_value = []
    client.list_workers.return_value = []
    return client


def _invoke(argv, client, tmp_path, env=None):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv, env=env)


def _document(result) -> dict:
    lines = result.stdout.splitlines()
    assert len(lines) == 1, result.output
    return json.loads(lines[0])


_TRIGGER = ["fn", "trigger", "-n", "f1", "-c", "c1"]


def test_trigger_returns_the_plan_without_waiting(clock, tmp_path):
    client = _client(_connection(["S"], []))
    result = _invoke([*_TRIGGER, "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert _document(result)["status_name"] == "Scheduled"
    assert clock.sleeps == []


def _trigger_wait(tmp_path, stored: str) -> None:
    (tmp_path / "tdk-settings.json").write_text(json.dumps({"trigger_wait": stored}))


def test_no_wait_overrides_the_default_trigger_wait(clock, tmp_path):
    client = _client(_connection(["S"], []))
    result = _invoke([*_TRIGGER, "--no-wait", "-o", "jsonb"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert json.loads(result.stdout)["status_name"] == "Scheduled"
    assert "deprecated" not in result.stderr
    assert clock.sleeps == []


@pytest.mark.parametrize("detach", ["--detach", "-d"])
def test_detach_is_a_deprecated_no_wait(clock, tmp_path, detach):
    client = _client(_connection(["S"], []))
    result = _invoke([*_TRIGGER, detach, "-o", "jsonb"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert json.loads(result.stdout)["status_name"] == "Scheduled"
    assert "Warning: '--detach' is deprecated" in result.stderr
    assert "'--no-wait'" in result.stderr
    assert clock.sleeps == []


@pytest.mark.parametrize("detach", ["--detach", "-d"])
def test_wait_and_detach_together_are_a_usage_error(clock, tmp_path, detach):
    client = _client(_connection(["S"], []))
    result = _invoke([*_TRIGGER, "--wait", detach], client, tmp_path)
    assert result.exit_code == ExitCode.USAGE
    assert client.trigger_function.call_count == 0


def test_trigger_help_shows_wait_no_wait_and_hides_detach(tmp_path):
    result = _invoke(["fn", "trigger", "-h"], _client(_connection(["S"], [])), tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert "--wait/--no-wait" in result.output
    assert "--detach" not in result.output


def test_trigger_waits_by_default(clock, tmp_path):
    client = _client(_connection(["S", "R", "F"], [_run_row("fr20260930-1", "C")]))
    result = _invoke([*_TRIGGER, "-o", "jsonb"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert json.loads(result.stdout)["status_name"] == "Finished"
    assert clock.sleeps == [1, 2]


def test_the_trigger_wait_setting_false_returns_at_once(clock, tmp_path):
    _trigger_wait(tmp_path, "disabled")
    client = _client(_connection(["S"], []))
    result = _invoke([*_TRIGGER, "-o", "jsonb"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert json.loads(result.stdout)["status_name"] == "Scheduled"
    assert clock.sleeps == []


def test_wait_overrides_the_trigger_wait_setting(clock, tmp_path):
    _trigger_wait(tmp_path, "disabled")
    client = _client(_connection(["S", "R", "F"], [_run_row("fr20260930-1", "C")]))
    result = _invoke([*_TRIGGER, "--wait", "-o", "jsonb"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert json.loads(result.stdout)["status_name"] == "Finished"


def test_a_machine_run_ignores_the_trigger_wait_setting(clock, tmp_path):
    _trigger_wait(tmp_path, "enabled")
    client = _client(_connection(["S"], []))
    result = _invoke([*_TRIGGER, "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert _document(result)["status_name"] == "Scheduled"
    assert clock.sleeps == []


def test_timeout_needs_wait(clock, tmp_path):
    client = _client(_connection(["S"], []))
    result = _invoke([*_TRIGGER, "--timeout", "5", "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.USAGE
    assert client.trigger_function.call_count == 0


def test_trigger_wait_returns_the_finished_plan_with_its_runs(clock, tmp_path):
    runs = [_run_row("fr20260930-1", "C")]
    client = _client(_connection(["S", "R", "F"], runs))
    result = _invoke([*_TRIGGER, "--wait", "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert document["status_name"] == "Finished"
    assert document["final"] is True
    assert [run["id"] for run in document["runs"]] == ["fr20260930-1"]
    assert document["waited_ms"] == 3000
    assert clock.sleeps == [1, 2]


def test_failed_runs_exit_8_with_the_plan_as_state(clock, tmp_path):
    runs = [_run_row("fr20260930-1", "C"), _run_row("FR2", "F"), _run_row("FR3", "H")]
    client = _client(_connection(["R", "F"], runs))
    result = _invoke([*_TRIGGER, "--wait", "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.RUN_FAILED
    error = _document(result)["error"]
    assert error["type"] == "run_failed"
    assert "FR2, FR3" in error["message"]
    assert "tdk fnrun diagnose --fnrun FR2" in error["hint"]
    assert error["state"] == {
        "plan": "ep20260930-1",
        "status_name": "Finished",
        "health": "ok",
        "function_runs_by_status": {"Committed": 1, "Failed": 1, "On Hold": 1},
        "failed_runs": ["FR2", "FR3"],
    }


def test_wait_seconds_is_the_default_budget_then_exits_7(
    clock, tmp_path, machine_settings
):
    machine_settings.update(wait_seconds=60.0)
    client = _client(_connection(["R"], []))
    result = _invoke(["plan", "monitor", "--plan", "ep20260930-1", "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.WAIT_TIMEOUT
    error = _document(result)["error"]
    assert error["type"] == "wait_timeout"
    assert error["resume"] == ["tdk", "plan", "get", "--plan", "ep20260930-1", "--wait"]
    assert error["state"]["status_name"] == "Running"
    assert sum(clock.sleeps) == 60


def test_timeout_sets_the_budget(clock, tmp_path):
    client = _client(_connection(["R"], []))
    argv = [*_TRIGGER, "--wait", "--timeout", "5", "--machine"]
    result = _invoke(argv, client, tmp_path)
    assert result.exit_code == ExitCode.WAIT_TIMEOUT
    assert sum(clock.sleeps) == 5
    assert client.trigger_function.call_count == 1


def test_max_wait_caps_the_budget_and_names_it_in_the_hint(clock, tmp_path, machine_settings):
    machine_settings.update(max_wait=(3.0, "a 3 s cap"))
    client = _client(_connection(["R"], []))
    argv = [*_TRIGGER, "--wait", "--timeout", "100", "--machine"]
    result = _invoke(argv, client, tmp_path)
    assert result.exit_code == ExitCode.WAIT_TIMEOUT
    assert sum(clock.sleeps) == 3
    assert "limited by a 3 s cap" in _document(result)["error"]["hint"]
    assert result.stderr == ""


def _jwt(exp: float) -> str:
    def _part(document: dict) -> str:
        raw = json.dumps(document).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    return f"{_part({'alg': 'HS256'})}.{_part({'exp': exp, 'jti': 'j'})}.sig"


def test_session_expiry_caps_the_budget(clock, tmp_path):
    client = _client(_connection(["R"], []))
    env = {"TDK_BEARER": _jwt(clock.now + 20), "TDK_SERVER": "http://h:2457"}
    argv = ["plan", "monitor", "--plan", "ep20260930-1", "--timeout", "100", "--machine"]
    result = _invoke(argv, client, tmp_path, env=env)
    assert result.exit_code == ExitCode.WAIT_TIMEOUT
    assert sum(clock.sleeps) == 15
    assert "TDK_BEARER" in _document(result)["error"]["hint"]


def test_humans_wait_without_limit_and_get_exit_8(clock, tmp_path):
    runs = [_run_row("fr20260930-1", "C"), _run_row("FR2", "F")]
    client = _client(_connection(["R"] * 30 + ["L"], runs))
    result = _invoke(["plan", "monitor", "--plan", "ep20260930-1"], client, tmp_path)
    assert result.exit_code == ExitCode.RUN_FAILED, result.output
    assert "did not succeed:" in result.output
    assert "--fnrun FR2" in result.output
    assert len(clock.sleeps) == 30


def test_json_monitor_keeps_progress_off_stdout(clock, tmp_path):
    client = _client(_connection(["R", "F"], [_run_row("fr20260930-1", "C")]))
    argv = ["plan", "monitor", "--plan", "ep20260930-1", "-o", "json"]
    result = _invoke(argv, client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert json.loads(result.stdout)["status_name"] == "Finished"
    assert "Waiting for the plan 'ep20260930-1' to finish" in result.stderr


def test_get_wait_adds_the_waited_time(clock, tmp_path):
    client = _client(_connection(["R", "F"], []))
    argv = ["plan", "get", "--plan", "ep20260301-1", "--wait", "--machine"]
    result = _invoke(argv, client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert document["plan"]["status_name"] == "Finished"
    assert document["waited_ms"] == 1000


def test_recover_wait_waits_for_the_plan(clock, tmp_path):
    client = _client(_connection(["R", "F"], [_run_row("fr20260930-1", "C")]))
    argv = ["plan", "recover", "--plan", "ep20260930-1", "--wait", "--machine"]
    result = _invoke(argv, client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert client.recover_execution.call_count == 1
    assert _document(result)["status_name"] == "Finished"


def _function_run(rows: list[dict], monkeypatch) -> ts.FunctionRun:
    connection = MagicMock()
    connection.timeout = (10, 60)
    polled = iter(rows)
    last = [rows[0]]

    def _list(self, filter=None, **_):
        last[0] = next(polled, last[0])
        return [ts.FunctionRun(self.connection, "fr20260930-1", **last[0])]

    monkeypatch.setattr(ts.TabsdataServer, "list_function_runs", _list)
    return ts.FunctionRun(connection, "fr20260930-1", status="F")


def _attempt(physical_id: str, physical_status: str) -> dict:
    return {
        "status": "F",
        "physical_function_run_id": physical_id,
        "physical_status": physical_status,
    }


def test_replay_wait_follows_the_new_attempt(clock, tmp_path, monkeypatch):
    run = _function_run(
        [_attempt("A2", "R"), _attempt("A2", "R"), _attempt("A2", "D")], monkeypatch
    )
    client = _client(_connection(["R"], []))
    client.replay_logical_function_run.return_value = {"new_function_run_id": "A2"}
    client.get_function_run.return_value = run
    argv = ["fnrun", "replay", "--fnrun", "fr20260930-1", "--wait", "--machine"]
    result = _invoke(argv, client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert document["physical_status"] == "D"
    assert document["replay"] == {"new_function_run_id": "A2"}


def test_replay_wait_exits_8_when_the_attempt_fails(clock, tmp_path, monkeypatch):
    run = _function_run([_attempt("A2", "E")], monkeypatch)
    client = _client(_connection(["R"], []))
    client.replay_logical_function_run.return_value = {"new_function_run_id": "A2"}
    client.get_function_run.return_value = run
    argv = ["fnrun", "replay", "--fnrun", "fr20260930-1", "--wait", "--machine"]
    result = _invoke(argv, client, tmp_path)
    assert result.exit_code == ExitCode.RUN_FAILED
    error = _document(result)["error"]
    assert error["state"]["physical_status"] == "E"
    assert error["state"]["attempt"] == "A2"
    assert error["state"]["failed_runs"] == [run.id]
    assert f"tdk fnrun diagnose --fnrun {run.id}" in error["hint"]


def test_replay_timeout_resumes_on_the_attempt(clock, tmp_path, monkeypatch):
    run = _function_run([_attempt("A1", "F")], monkeypatch)
    client = _client(_connection(["R"], []))
    client.replay_logical_function_run.return_value = {"new_function_run_id": "A2"}
    client.get_function_run.return_value = run
    argv = ["fnrun", "replay", "--fnrun", "fr20260930-1", "--wait", "--timeout", "4"]
    result = _invoke([*argv, "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.WAIT_TIMEOUT
    assert _document(result)["error"]["resume"] == [
        "tdk",
        "fnrun",
        "get",
        "--fnrun",
        "fr20260930-1",
        "--wait",
        "--attempt",
        "A2",
    ]


def _listed(connection, status: str) -> ts.Execution:
    execution = ts.Execution(connection, "ep20260930-1", status=status)
    execution._data = {"id": "ep20260930-1", "status": status}
    return execution


def test_list_monitor_is_bounded_and_resumes_the_command(clock, tmp_path):
    client = _client(_connection(["R"], []))
    client.list_executions_generator.side_effect = lambda **_: iter([
        _listed(client.connection, "R")
    ])
    argv = ["plan", "list", "--monitor", "--timeout", "3"]
    result = _invoke([*argv, "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.WAIT_TIMEOUT
    error = _document(result)["error"]
    assert error["resume"] == ["tdk", *argv]
    assert [item["id"] for item in error["state"]["items"]] == ["ep20260930-1"]
    assert sum(clock.sleeps) == 3


def test_a_persons_jsonb_list_monitor_timeout_is_an_error_document(clock, tmp_path):
    client = _client(_connection(["R"], []))
    client.list_executions_generator.side_effect = lambda **_: iter([
        _listed(client.connection, "R")
    ])
    argv = ["plan", "list", "--monitor", "--timeout", "3", "-o", "jsonb"]
    result = _invoke(argv, client, tmp_path)
    assert result.exit_code == ExitCode.WAIT_TIMEOUT
    error = json.loads(result.stdout)["error"]
    assert error["type"] == "wait_timeout"
    assert error["state"]["items"][0]["id"] == "ep20260930-1"


def test_list_monitor_emits_the_envelope_once_final(clock, tmp_path):
    statuses = iter(["R", "F"])
    client = _client(_connection(["R"], []))
    client.list_executions_generator.side_effect = lambda **_: iter([
        _listed(client.connection, next(statuses))
    ])
    argv = ["plan", "list", "--monitor", "--machine"]
    result = _invoke(argv, client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert _document(result)["items"][0]["status_name"] == "Finished"


def test_a_request_outlasting_the_budget_is_a_wait_timeout(clock, tmp_path):
    import requests

    connection = _connection(["R"], [])
    answers = iter([_listing([{"id": "ep20260930-1", "status": "R"}])])
    read_timeouts = []

    def _slow(*_, **__):
        read_timeouts.append(connection.timeout)
        answer = next(answers, None)
        if answer is None:
            raise requests.exceptions.ReadTimeout()
        return answer

    connection.execution_list.side_effect = _slow
    client = _client(connection)
    argv = ["plan", "monitor", "--plan", "ep20260930-1", "--timeout", "5", "--machine"]
    result = _invoke(argv, client, tmp_path)
    assert result.exit_code == ExitCode.WAIT_TIMEOUT, result.output
    state = _document(result)["error"]["state"]
    assert (state["id"], state["status_name"]) == ("ep20260930-1", "Running")
    assert read_timeouts[-1] == (5.0, 5.0)
    assert connection.timeout == (10, 60)


@pytest.mark.parametrize(
    "argv, recovered",
    [
        (["trx", "monitor", "--trx", "tx20260930-1"], None),
        (["trx", "recover", "--trx", "tx20260930-1", "--wait"], "recover_transaction"),
        (["fnrun", "recover", "--fnrun", "fr20260930-1", "--wait"], "recover_transaction"),
        (["worker", "recover", "--worker", "wkr20260930-1", "--wait"], "recover_transaction"),
    ],
)
def test_transaction_waits_render_the_waited_transaction(
    argv, recovered, clock, tmp_path, monkeypatch
):
    statuses = iter(["R", "C"])

    def _list(self, filter=None, **_):
        return [ts.Transaction(self.connection, "tx20260930-1", status=next(statuses, "C"))]

    monkeypatch.setattr(ts.TabsdataServer, "list_transactions", _list)
    connection = _connection(["R"], [_run_row("fr20260930-1", "C")])
    client = _client(connection)
    client.get_transaction.return_value = ts.Transaction(connection, "tx20260930-1")
    client.get_function_run.return_value.transaction.id = "tx20260930-1"
    client.get_worker.return_value.transaction.id = "tx20260930-1"
    result = _invoke([*argv, "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert document["status_name"] == "Committed"
    assert [run["id"] for run in document["runs"]] == ["fr20260930-1"]
    assert "waited_ms" in document
    assert client.connection.project is None
    if recovered:
        assert getattr(client, recovered).call_count == 1


_ATTEMPT_2 = "pfr20260930-2"
_ATTEMPT_3 = "pfr20260930-0000000003"


def test_a_superseded_attempt_ends_the_wait(clock, tmp_path, monkeypatch):
    run = _function_run(
        [_attempt(_ATTEMPT_2, "R"), _attempt(_ATTEMPT_3, "R")], monkeypatch
    )
    client = _client(_connection(["R"], []))
    client.get_function_run.return_value = run
    argv = ["fnrun", "get", "--fnrun", "fr20260930-1", "--wait", "--attempt", _ATTEMPT_2]
    result = _invoke([*argv, "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.CONFLICT, result.output
    error = _document(result)["error"]
    assert _ATTEMPT_3 in error["message"]
    assert f"--attempt {_ATTEMPT_3}" in error["hint"]
    assert sum(clock.sleeps) == 1


def test_a_mistyped_attempt_is_a_usage_error(clock, tmp_path):
    client = _client(_connection(["R"], []))
    argv = ["fnrun", "get", "--fnrun", "fr20260930-1", "--wait", "--attempt", "A2x"]
    result = _invoke([*argv, "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.USAGE, result.output
    assert "new_function_run_id" in _document(result)["error"]["hint"]
    assert client.get_function_run.call_count == 0


@pytest.mark.parametrize(
    ("argv", "hint"),
    [
        (["plan", "cancel", "--plan", "daily", "--confirm", "cancel"], "tdk plan list"),
        (["plan", "recover", "--plan", "daily"], "tdk plan list"),
        (["plan", "monitor", "--plan", "daily"], "tdk plan list"),
        (["plan", "logs", "--plan", "daily"], "tdk plan list"),
        (["plan", "diagnose", "--plan", "daily"], "tdk plan list"),
        (["trx", "list", "--plan", "daily"], "tdk plan list"),
        (["trx", "cancel", "--trx", "T1", "--confirm", "cancel"], "tdk trx list"),
        (["trx", "recover", "--trx", "T1"], "tdk trx list"),
        (["trx", "monitor", "--trx", "T1"], "tdk trx list"),
        (["trx", "logs", "--trx", "T1"], "tdk trx list"),
        (["fnrun", "list", "--trx", "T1"], "tdk trx list"),
        (["fnrun", "logs", "--fnrun", "R1"], "tdk fnrun list"),
        (["fnrun", "replay", "--fnrun", "R1"], "tdk fnrun list"),
        (["fnrun", "recover", "--fnrun", "R1"], "tdk fnrun list"),
        (["fnrun", "diagnose", "--fnrun", "R1"], "tdk fnrun list"),
        (["worker", "list", "--fnrun", "R1"], "tdk fnrun list"),
        (["worker", "recover", "--worker", "3"], "tdk worker list"),
    ],
)
def test_a_non_id_is_a_usage_error_before_any_request(clock, tmp_path, argv, hint):
    connection = _connection(["R"], [])
    client = _client(connection)
    result = _invoke([*argv, "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.USAGE, result.output
    error = _document(result)["error"]
    assert error["message"] == f"'{argv[2]} {argv[3]}' is not an id."
    assert hint in error["hint"]
    assert [c for c in client.mock_calls if not c[0].startswith("connection")] == []
    assert connection.mock_calls == []


@pytest.mark.parametrize("attempt", ["pfr20260930-2", "pfr20260930-0000000002"])
def test_a_logical_attempt_id_is_accepted(clock, tmp_path, monkeypatch, attempt):
    run = _function_run([_attempt(attempt, "D")], monkeypatch)
    client = _client(_connection(["R"], []))
    client.get_function_run.return_value = run
    argv = ["fnrun", "get", "--fnrun", "fr20260930-1", "--wait", "--attempt", attempt]
    result = _invoke([*argv, "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output


def test_resume_keeps_the_profile(clock, tmp_path):
    client = _client(_connection(["R"], []))
    argv = ["plan", "monitor", "--plan", "ep20260930-1", "--timeout", "2", "--profile", "dev"]
    result = _invoke([*argv, "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.WAIT_TIMEOUT, result.output
    assert _document(result)["error"]["resume"] == [
        "tdk",
        "plan",
        "get",
        "--plan",
        "ep20260930-1",
        "--wait",
        "--profile",
        "dev",
    ]


def test_resume_keeps_the_default_profile_over_a_set_one(clock, tmp_path):
    (tmp_path / "profile.json").write_text(json.dumps({"profile": "dev"}))
    client = _client(_connection(["R"], []))
    argv = ["plan", "monitor", "--plan", "ep20260930-1", "--timeout", "2", "--profile"]
    result = _invoke([*argv, "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.WAIT_TIMEOUT, result.output
    assert _document(result)["error"]["resume"][-2:] == ["--wait", "--profile"]


def test_resume_needs_no_profile_without_a_set_one(clock, tmp_path):
    client = _client(_connection(["R"], []))
    argv = ["plan", "monitor", "--plan", "ep20260930-1", "--timeout", "2"]
    result = _invoke([*argv, "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.WAIT_TIMEOUT, result.output
    assert _document(result)["error"]["resume"][-1] == "--wait"


def test_an_unknown_run_status_is_not_a_failure(clock, tmp_path):
    runs = [_run_row("fr20260930-1", "C"), _run_row("FR2", "Q")]
    client = _client(_connection(["R", "F"], runs))
    result = _invoke([*_TRIGGER, "--wait", "--machine"], client, tmp_path)
    assert result.exit_code == ExitCode.OK, result.output
    assert [run["health"] for run in _document(result)["runs"]] == ["ok", None]


def test_trigger_summary_is_the_plan_its_runs_by_status_and_the_failed(
    clock, tmp_path
):
    runs = [_run_row("fr20260930-1", "C")]
    client = _client(_connection(["S", "R", "F"], runs))
    argv = [*_TRIGGER, "--wait", "--summary", "--machine"]
    document = _document(_invoke(argv, client, tmp_path))
    waited = document.pop("waited_ms")
    assert isinstance(waited, int)
    assert document == {
        "plan": "ep20260930-1",
        "status_name": "Finished",
        "health": "ok",
        "function_runs_by_status": {"Committed": 1},
        "failed_runs": [],
    }


def test_trigger_count_runs_one_after_another(clock, tmp_path):
    client = _client(_connection(["S", "R", "F"], [_run_row("fr20260930-1", "C")]))
    argv = [*_TRIGGER, "--wait", "--count", "2", "--machine"]
    document = _document(_invoke(argv, client, tmp_path))
    assert document["count"] == 2
    assert [run["status_name"] for run in document["triggered"]] == ["Finished"] * 2
    assert client.trigger_function.call_count == 2


def test_trigger_count_stops_at_the_first_failure(clock, tmp_path):
    runs = [_run_row("FR2", "F")]
    client = _client(_connection(["R", "F"], runs))
    argv = [*_TRIGGER, "--wait", "--count", "3", "--machine"]
    result = _invoke(argv, client, tmp_path)
    assert result.exit_code == ExitCode.RUN_FAILED
    error = _document(result)["error"]
    assert error["message"].startswith("Run 1 of 3: ")
    assert error["state"]["triggered"] == []
    assert error["state"]["current"]["failed_runs"] == ["FR2"]
    assert client.trigger_function.call_count == 1


def test_trigger_count_needs_the_wait(clock, tmp_path):
    argv = [*_TRIGGER, "--no-wait", "--count", "2", "--machine"]
    result = _invoke(argv, _client(_connection(["S"], [])), tmp_path)
    assert _document(result)["error"]["type"] == "usage"


def test_jsonb_drops_the_status_code_and_json_keeps_it(clock, tmp_path):
    client = _client(_connection(["S"], []))
    document = json.loads(
        _invoke([*_TRIGGER, "--no-wait", "-o", "jsonb"], client, tmp_path).stdout
    )
    assert "status" not in document and document["status_name"] == "Scheduled"
    compat = json.loads(
        _invoke([*_TRIGGER, "--no-wait", "-o", "json"], client, tmp_path).stdout
    )
    assert compat["status"] == "S"

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tabsdatak.sdk import tabsdata_server as ts
from tabsdatak.sdk._apiserver import APIServerError, NotFound
from test_tabsdatak.sdk.bootest import TESTING_RESOURCES_PATH

KEY_ERROR_LOG = (
    Path(TESTING_RESOURCES_PATH) / "worker_logs" / "pandas_key_error.txt"
).read_text()


def _run(id: str, status: str, fv: str, ended_on: int | None = None, **extra) -> dict:
    return {
        "id": id,
        "status": status,
        "name": f"fn_{fv}",
        "collection": "sales",
        "collection_deleted": None,
        "execution_id": "P1",
        "transaction_id": "T1",
        "function_version_id": fv,
        "function_version_patch_id": None,
        "physical_function_run_id": f"PFR_{id}",
        "ended_on": ended_on,
        **extra,
    }


def _worker(id: str, run: str):
    return SimpleNamespace(id=id, kwargs={"logical_function_run_id": run})


def _graph(*relations) -> dict:
    tables = {}
    edges = []
    for producer, table, consumer in relations:
        tables[table] = {"function_version_id": producer}
        edges.append({
            "function_version_id": producer,
            "table_version_id": table,
            "edge": {"Output": {"versions": [], "output": {}}},
        })
        edges.append({
            "function_version_id": consumer,
            "table_version_id": table,
            "edge": {"Dependency": {"versions": [], "dependency": {}}},
        })
    return {"relations": edges, "all_tables": tables}


def _server(runs, plan_status="L", workers=None, logs=None, graph=None):
    server = ts.TabsdataServer.__new__(ts.TabsdataServer)
    server.connection = MagicMock()
    server._spanning = nullcontext
    server.get_execution = MagicMock(
        return_value=SimpleNamespace(
            id="P1", kwargs={"name": "daily", "project": "sales", "status": plan_status}
        )
    )
    server.list_function_runs_generator = MagicMock(
        side_effect=lambda filter: iter(SimpleNamespace(kwargs=r) for r in runs)
    )
    by_run = workers or {}
    server.list_workers = MagicMock(
        side_effect=lambda filter: by_run.get(filter[0].rsplit(":", 1)[1], [])
    )

    def get_worker_log(worker_id):
        log = (logs or {}).get(worker_id, "")
        if isinstance(log, Exception):
            raise log
        return log

    server.get_worker_log = get_worker_log
    server.get_execution_graph = MagicMock(return_value=graph or {})
    return server


def test_plan_diagnosis_names_the_earliest_failure():
    runs = [
        _run("R3", "F", "fv3", ended_on=30),
        _run("R1", "F", "fv1", ended_on=10),
        _run("R2", "C", "fv2", ended_on=5),
        _run("R4", "E", "fv4", ended_on=40),
    ]
    server = _server(
        runs,
        workers={"R1": [_worker("w1a", "R1"), _worker("w1b", "R1")]},
        logs={"w1b": KEY_ERROR_LOG},
    )
    document = server.diagnose_execution("P1").to_dict()
    assert document["kind"] == "plan"
    assert document["plan"] == {
        "id": "P1",
        "name": "daily",
        "project": "sales",
        "status": "L",
        "status_name": "Stalled",
        "health": "error",
        "final": True,
    }
    assert document["summary"] == {
        "runs": 4,
        "by_health": {"ok": 1, "running": 0, "warning": 1, "error": 2},
        "held": 0,
    }
    assert [r["function_run_id"] for r in document["failed_runs"]] == ["R1", "R3", "R4"]
    first = document["failed_runs"][0]
    assert first["worker_id"] == "w1b"
    assert first["attempts"] == 2
    assert first["signature"]["category"] == "schema_mismatch"
    assert first["can_replay"] is True and first["can_recover"] is True
    assert first["argv"]["replay"] == ["fnrun", "replay", "--fnrun", "R1", "--wait"]
    assert first["argv"]["recover"] == ["plan", "recover", "--plan", "P1"]
    assert first["argv"]["patch"][-4:] == ["--fn-version", "fv1", "--source", "<file>"]
    retrying = document["failed_runs"][2]
    assert retrying["health"] == "warning"
    assert retrying["argv"] is None and retrying["can_replay"] is False
    root = document["root_cause"]
    assert root["function_run_id"] == "R1"
    assert root["category"] == "schema_mismatch"
    assert root["code"] == "key_error"
    assert root["user_frame"] == {
        "module": "pipeline/transform.py",
        "line": 17,
        "function": "enrich",
    }
    assert document["truncated"] == {"failed_runs_shown": 3, "failed_runs_total": 3}


def test_max_runs_caps_the_failed_runs():
    runs = [_run(f"R{i}", "F", f"fv{i}", ended_on=i) for i in range(4)]
    document = _server(runs).diagnose_execution("P1", max_runs=2).to_dict()
    assert [r["function_run_id"] for r in document["failed_runs"]] == ["R0", "R1"]
    assert document["truncated"] == {"failed_runs_shown": 2, "failed_runs_total": 4}


def test_forbidden_logs_degrade_to_statuses():
    runs = [_run("R1", "F", "fv1", ended_on=1)]
    server = _server(
        runs,
        workers={"R1": [_worker("w1", "R1")]},
        logs={"w1": APIServerError({"error": "forbidden"}, status=403)},
    )
    document = server.diagnose_execution("P1").to_dict()
    (run,) = document["failed_runs"]
    assert run["log_unavailable"] == "forbidden (DevelopProject required)"
    assert run["signature"] is None
    assert document["root_cause"]["category"] == "unknown"


def test_no_logs_reads_no_workers():
    runs = [_run("R1", "F", "fv1", ended_on=1)]
    server = _server(runs)
    document = server.diagnose_execution("P1", logs=False).to_dict()
    server.list_workers.assert_not_called()
    assert document["failed_runs"][0]["attempts"] is None


def test_deleted_collection_cannot_be_replayed():
    runs = [_run("R1", "F", "fv1", collection_deleted=123)]
    document = _server(runs).diagnose_execution("P1").to_dict()
    assert document["failed_runs"][0]["can_replay"] is False


def test_held_runs_name_their_blockers_inside_the_plan():
    runs = [
        _run("R1", "F", "fv1", ended_on=1),
        _run("R2", "H", "fv2"),
        _run("R3", "H", "fv3"),
    ]
    graph = _graph(("fv1", "t1", "fv2"), ("fv2", "t2", "fv3"))
    document = _server(runs, graph=graph).diagnose_execution("P1").to_dict()
    assert document["held_runs"] == [
        {
            "function_run_id": "R2",
            "collection": "sales",
            "function": "fn_fv2",
            "blocked_by": ["R1"],
            "blocked_by_outside_plan": False,
        },
        {
            "function_run_id": "R3",
            "collection": "sales",
            "function": "fn_fv3",
            "blocked_by": ["R1"],
            "blocked_by_outside_plan": False,
        },
    ]
    assert document["summary"]["held"] == 2


def test_held_runs_blocked_outside_the_plan():
    runs = [_run("R2", "H", "fv2")]
    graph = _graph(("fv_elsewhere", "t1", "fv2"))
    document = (
        _server(runs, plan_status="L", graph=graph).diagnose_execution("P1").to_dict()
    )
    (held,) = document["held_runs"]
    assert held["blocked_by"] == [] and held["blocked_by_outside_plan"] is True
    assert document["root_cause"]["category"] == "on_hold"
    assert document["failed_runs"] == []


def test_a_held_run_with_nothing_in_its_plan_names_its_functions_earlier_failure():
    held = _run("R2", "H", "fv2", function_id="f2", triggered_on=20)
    earlier = _run("R1", "F", "fv2", function_id="f2", triggered_on=10, ended_on=1)
    server = _server([held], graph=_graph())
    server.list_function_runs_generator = MagicMock(
        side_effect=lambda filter: iter(
            SimpleNamespace(kwargs=r)
            for r in (
                [held, earlier] if filter[0].startswith("function_id") else [held]
            )
        )
    )
    (entry,) = server.diagnose_execution("P1").to_dict()["held_runs"]
    assert entry["blocked_by"] == ["R1"] and entry["blocked_by_outside_plan"] is False


def test_a_failed_producer_in_the_plan_is_not_blocked_outside_it():
    runs = [_run("R1", "F", "fv1", ended_on=1), _run("R2", "H", "fv2")]
    graph = _graph(("fv1", "t1", "fv2"), ("fv_elsewhere", "t9", "fv2"))
    document = _server(runs, graph=graph).diagnose_execution("P1").to_dict()
    (held,) = document["held_runs"]
    assert held["blocked_by"] == ["R1"]
    assert held["blocked_by_outside_plan"] is False


def test_the_root_cause_is_the_earliest_failure_even_when_not_shown():
    runs = [
        _run("R1", "E", "fv1", ended_on=1),
        _run("R2", "E", "fv2", ended_on=2),
        _run("R3", "F", "fv3", ended_on=3),
    ]
    server = _server(
        runs,
        workers={"R3": [_worker("w3", "R3")]},
        logs={"w3": KEY_ERROR_LOG},
    )
    document = server.diagnose_execution("P1", max_runs=1).to_dict()
    assert [r["function_run_id"] for r in document["failed_runs"]] == ["R1"]
    assert document["root_cause"]["function_run_id"] == "R3"
    assert document["root_cause"]["code"] == "key_error"


def test_a_healthy_plan_has_no_root_cause():
    runs = [_run("R1", "C", "fv1")]
    document = _server(runs, plan_status="F").diagnose_execution("P1").to_dict()
    assert document["root_cause"] is None
    assert document["plan"]["health"] == "ok"


@pytest.mark.parametrize(
    "getter, lister",
    [
        ("get_execution", "list_executions"),
        ("get_function_run", "list_function_runs"),
        ("get_transaction", "list_transactions"),
        ("get_worker", "list_workers"),
    ],
)
def test_getters_raise_not_found(getter, lister):
    server = ts.TabsdataServer.__new__(ts.TabsdataServer)
    server.connection = MagicMock()
    server._spanning = nullcontext
    setattr(server, lister, MagicMock(return_value=[]))
    with pytest.raises(NotFound, match="X"):
        getattr(server, getter)("X")


def test_unknown_plan_raises():
    server = _server([])
    server.get_execution = MagicMock(side_effect=NotFound("Execution X not found."))
    with pytest.raises(NotFound):
        server.diagnose_execution("X")


def test_function_run_diagnosis():
    run = _run("R1", "F", "fv1", ended_on=1)
    server = _server(
        [run], workers={"R1": [_worker("w1", "R1")]}, logs={"w1": KEY_ERROR_LOG}
    )
    server.get_function_run = MagicMock(return_value=SimpleNamespace(kwargs=run))
    document = server.diagnose_function_run("R1", log_lines=3).to_dict()
    assert document["kind"] == "function_run"
    assert document["function_run"]["plan_id"] == "P1"
    assert document["function_run"]["health"] == "error"
    (failed,) = document["failed_runs"]
    assert failed["argv"]["recover"] == ["fnrun", "recover", "--fnrun", "R1"]
    assert failed["log_truncated"] is True
    assert failed["log_excerpt"].startswith("Traceback (most recent call last):")
    server.list_function_runs_generator.assert_not_called()


def test_held_function_run_walks_its_plan():
    runs = [_run("R1", "F", "fv1", ended_on=1), _run("R2", "H", "fv2")]
    server = _server(runs, graph=_graph(("fv1", "t1", "fv2")))
    server.get_function_run = MagicMock(return_value=SimpleNamespace(kwargs=runs[1]))
    document = server.diagnose_function_run("R2").to_dict()
    assert document["held_runs"][0]["blocked_by"] == ["R1"]
    assert document["root_cause"]["blocked_by"] == ["R1"]

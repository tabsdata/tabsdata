#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

import pytest
from click.testing import CliRunner

from tabsdatax.pytesting.plugins.harvester.client import main as monitor
from tabsdatax.pytesting.plugins.harvester.internal.constants import (
    TDX_HARVESTER_FOLDER_ENV,
    TDX_HARVESTER_SESSION_ENV,
)
from tabsdatax.pytesting.plugins.harvester.internal.database import DB_FILENAME
from tabsdatax.pytesting.plugins.harvester.internal.sessions import list_sessions
from tabsdatax.pytesting.plugins.harvester.plugin import TD_ON_GA_ENV

pytestmark = [pytest.mark.unit]

EXAMPLE = "tabsdatax/pytesting/plugins/harvester/sample.py"
META_FILENAME = "_meta_state.json"
HEADER = re.compile(r"^harvester session: (\S+) \(tdpytest \1\)$", re.MULTILINE)
# Above any pid Linux, macOS or Windows hands out in practice.
DEAD_PID = 4194305


@pytest.fixture
def base(pytester: pytest.Pytester, monkeypatch) -> Path:
    # The session running these tests marks itself in the environment; the
    # sessions started here must harvest on their own.
    monkeypatch.delenv(TDX_HARVESTER_SESSION_ENV, raising=False)
    monkeypatch.delenv(TDX_HARVESTER_FOLDER_ENV, raising=False)
    monkeypatch.delenv(TD_ON_GA_ENV, raising=False)
    return pytester.path / "harvested"


def harvest(pytester: pytest.Pytester, base: Path, *arguments: str):
    copied = pytester.copy_example(EXAMPLE)
    return pytester.runpytest_subprocess(
        "-p",
        "no:cacheprovider",
        f"--tdx-harvester-folder={base}",
        *arguments,
        copied.name,
    )


def session_of(result: pytest.RunResult) -> str:
    match = HEADER.search(result.stdout.str())
    assert match, result.stdout.str()
    return match.group(1)


def query(folder: Path, sql: str) -> list[tuple]:
    connection = sqlite3.connect(folder / DB_FILENAME)
    try:
        return connection.execute(sql).fetchall()
    finally:
        connection.close()


@pytest.mark.parametrize("workers", ["0", "2"])
def test_a_session_records_its_tests_in_a_folder_of_its_own(pytester, base, workers):
    result = harvest(pytester, base, "-n", workers)
    result.assert_outcomes(passed=3, failed=1, skipped=1)
    session = session_of(result)
    assert list_sessions(base) == [session]
    statuses = dict(query(base / session, "select nodeid, status from test_records"))
    assert statuses == {
        "sample.py::test_passes[0]": "Passed",
        "sample.py::test_passes[1]": "Passed",
        "sample.py::test_passes[2]": "Passed",
        "sample.py::test_fails": "Failed (Execution)",
        "sample.py::test_skips": "Skipped",
    }
    state = json.loads((base / session / META_FILENAME).read_text(encoding="utf-8"))
    assert state["collected"] == 5
    assert state["fixtures"]["shared"]["total"] == 4
    assert state["fixtures"]["shared"]["scope"] == "session"
    assert state["markers"]["parametrize"]["total"] == 3
    assert sum(entry["total"] for entry in state["components"].values()) == 5


@pytest.mark.parametrize("workers", ["0", "2"])
def test_deselected_tests_are_not_counted(pytester, base, workers):
    session = session_of(harvest(pytester, base, "-k", "passes", "-n", workers))
    state = json.loads((base / session / META_FILENAME).read_text(encoding="utf-8"))
    assert state["collected"] == 3
    assert state["fixtures"]["shared"]["total"] == 3


def test_a_second_session_leaves_the_first_one_alone(pytester, base):
    first = session_of(harvest(pytester, base, "-n", "0"))
    records = query(base / first, "select count(*) from test_records")
    second = session_of(harvest(pytester, base, "-n", "0"))
    assert first != second
    assert list_sessions(base) == sorted([first, second])
    assert query(base / first, "select count(*) from test_records") == records


@pytest.mark.parametrize("workers", ["0", "2"])
def test_wider_scoped_fixtures_are_torn_down(pytester, base, workers):
    session = session_of(harvest(pytester, base, "-n", workers))
    torn_down = {
        fixture
        for (fixture,) in query(
            base / session,
            "select fixture from fixture_events where event = 'teardown_end'",
        )
    }
    assert {"shared", "per_module"} <= torn_down


@pytest.mark.parametrize(
    "variable, value",
    [(TDX_HARVESTER_SESSION_ENV, "20260101T000000Z-1"), (TD_ON_GA_ENV, "true")],
)
def test_a_nested_or_github_actions_pytest_records_nothing(
    pytester, base, monkeypatch, variable, value
):
    monkeypatch.setenv(variable, value)
    result = harvest(pytester, base, "-n", "0")
    result.assert_outcomes(passed=3, failed=1, skipped=1)
    assert not HEADER.search(result.stdout.str())
    assert list_sessions(base) == []


def test_old_finished_sessions_are_pruned(pytester, base):
    finished = [f"2026010{day}T000000Z-{DEAD_PID}" for day in range(1, 4)]
    for name in finished:
        (base / name).mkdir(parents=True)
    session = session_of(harvest(pytester, base, "--tdx-harvester-keep=1", "-n", "0"))
    assert list_sessions(base) == [finished[-1], session]


def test_the_monitor_needs_a_session_it_can_find(base):
    (base / "20260101T000000Z-1").mkdir(parents=True)
    runner = CliRunner()
    for arguments in ([], ["20990101T000000Z-1"]):
        result = runner.invoke(monitor, [*arguments, "--folder", str(base)])
        assert result.exit_code == 2
        assert "20260101T000000Z-1" in result.output

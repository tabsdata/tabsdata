#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatax.itest.tdksystems2.systems.db2cdc import backend
from tabsdatax.itest.tdksystems2.systems.db2cdc.backend import Db2Cdc


@pytest.fixture
def cdc(monkeypatch) -> Db2Cdc:
    monkeypatch.setattr(backend.time, "sleep", lambda seconds: None)
    system = Db2Cdc()
    system.calls = []
    system.scalars = []
    system.lines = {}

    def run(container, body, **kwargs):
        system.calls.append(body)
        return ""

    def scalar(container, sql):
        system.calls.append(sql)
        return system.scalars.pop(0) if system.scalars else ""

    def lines(container, sql):
        system.calls.append(sql)
        return next(
            (rows for fragment, rows in system.lines.items() if fragment in sql), []
        )

    system._run = run
    system._clp_scalar = scalar
    system._clp_lines = lines
    return system


def test_each_table_is_registered_in_one_container_call(cdc):
    cdc.scalars = ["0 3"]
    cdc._register_tables("C", "S", ["A", "B", "D"])
    registrations = [call for call in cdc.calls if "asnclp" in call]
    assert len(registrations) == 3
    for call in registrations:
        assert "data capture changes" in call
        assert "IBMSNAP_LOGMARKER" in call
        assert "ibmsnap_pruncntl" in call
        assert "'CAPSTART'" in call


def test_every_registration_is_waited_for_together(cdc):
    cdc.scalars = ["1 2", "0 2", "0 3"]
    cdc._register_tables("C", "S", ["A", "B", "D"])
    polls = [call for call in cdc.calls if "sysdummy1" in call]
    assert len(polls) == 3
    assert "'A', 'B', 'D'" in polls[0]


def test_a_failing_registration_step_ends_its_call(cdc):
    cdc.scalars = ["0 1"]
    cdc._register_tables("C", "S", ["A"])
    [call] = [call for call in cdc.calls if "asnclp" in call]
    steps = [line for line in call.splitlines() if line.startswith("db2w ")]
    assert steps
    assert all(line.endswith(backend._OR_EXIT) for line in steps)


def test_every_table_is_stopped_in_one_call_and_waited_for_once(cdc):
    cdc.lines = {"cd_table from": ["A TD_X__A", "B TD_X__B"]}
    cdc.scalars = ["0"]
    cdc._remove_registrations("C", "S", ["A", "B"])
    stops = [call for call in cdc.calls if "'CAPSTOP'" in call and "insert" in call]
    assert len(stops) == 1
    assert "'S.A'" in stops[0] and "'S.B'" in stops[0]
    drops = next(call for call in cdc.calls if "drop table ASN.TD_X__A" in call)
    assert "drop table ASN.TD_X__B" in drops


def test_only_the_latest_stop_of_each_table_is_waited_for(cdc):
    cdc.scalars = ["0"]
    assert cdc._capstops_consumed("C", ["S.A"]) is True
    assert "row_number() over" in cdc.calls[-1]


def test_a_cd_table_that_survives_its_removal_is_an_error(cdc):
    cdc.lines = {"cd_table from": ["A TD_X__A"], "tabname in": ["TD_X__A"]}
    cdc.scalars = ["0"]
    with pytest.raises(backend.TdxError, match="could not be dropped"):
        cdc._remove_registrations("C", "S", ["A"])


def test_the_capture_program_is_tuned_to_its_minimum_intervals(cdc, monkeypatch):
    commands = []
    monkeypatch.setattr(
        cdc, "_exec_inst", lambda container, script, **k: commands.append(script) or ""
    )
    cdc._tune_capture("C")
    assert any("commit_interval = 1" in call for call in cdc.calls)
    assert any("chgparms commit_interval=1 sleep_interval=1" in c for c in commands)

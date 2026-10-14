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


def test_a_failing_registration_step_ends_its_call(cdc):
    cdc.scalars = ["0 1"]
    cdc._register_tables("C", "S", ["A"])
    [call] = [call for call in cdc.calls if "asnclp" in call]
    steps = [line for line in call.splitlines() if line.startswith("db2w ")]
    assert steps
    assert all(line.endswith(backend._OR_EXIT) for line in steps)


def test_every_table_is_removed_in_one_container_call(cdc):
    """Past reading the recorded CD table names, one docker exec and one CLP
    connect for the stops, the wait, the removal and the check."""
    cdc.lines = {"cd_table from": ["A TD_X__A", "B TD_X__B"]}
    cdc._remove_registrations("C", "S", ["A", "B"])
    [call] = [call for call in cdc.calls if "'CAPSTOP'" in call]
    assert call.count("connect to") == 1
    assert "'S.A', 'P'" in call and "'S.B', 'P'" in call
    assert "while [ $SECONDS -lt 300 ]" in call and "$(db2" not in call
    for statement in (
        "delete from ASN.ibmsnap_register",
        "delete from ASN.ibmsnap_pruncntl",
        "drop table ASN.TD_X__A",
        "drop table ASN.TD_X__B",
    ):
        assert statement in call
    assert call.index("'CAPSTOP'") < call.index("while") < call.index("drop table")


def test_only_the_latest_stop_of_each_table_is_waited_for(cdc):
    cdc._remove_registrations("C", "S", ["A"])
    [call] = [call for call in cdc.calls if "'CAPSTOP'" in call]
    assert "row_number() over" in call and "latest = 1" in call


def test_a_deregistration_clears_data_capture_and_drops_its_sources(cdc):
    cdc._deregister_tables("C", "S", ["A"], drop=True)
    [call] = [call for call in cdc.calls if "'CAPSTOP'" in call]
    assert "alter table S.A data capture none" in call
    assert call.index("drop table ASN.") < call.index("drop table S.A")


def test_a_cd_table_that_survives_its_removal_is_an_error(cdc):
    cdc.lines = {"cd_table from": ["A TD_X__A"]}
    cdc._run = lambda container, body, **kwargs: (
        f"DB20000I done\n{backend._SECTION}left\nTD_X__A\n"
    )
    with pytest.raises(backend.TdxError, match="could not be dropped"):
        cdc._remove_registrations("C", "S", ["A"])


def test_the_capture_program_is_tuned_to_its_minimum_intervals(cdc, monkeypatch):
    commands = []
    monkeypatch.setattr(
        cdc, "_exec_inst", lambda container, script, **k: commands.append(script) or ""
    )
    cdc._tune_capture("C")
    assert any("commit_interval = 1" in call for call in cdc.calls)
    assert any("memory_limit = 1024" in call for call in cdc.calls)
    assert any(
        "chgparms commit_interval=1 sleep_interval=1 memory_limit=1024" in c
        for c in commands
    )


def test_every_table_is_registered_in_one_container_call(cdc):
    """One docker exec, one CLP connect and one asnclp run for the whole
    model: per table they cost ~3.5s before any DDL, most of an install."""
    cdc.scalars = ["0 3"]
    cdc._register_tables("C", "S", ["A", "B", "D"])
    [call] = [call for call in cdc.calls if "asnclp" in call]
    assert call.count("| asnclp") == 1 and call.count("connect to") == 1
    for table in ("A", "B", "D"):
        assert f"alter table S.{table} data capture changes" in call
        assert f"create registration (S.{table})" in call
        assert f"'{table}', 0, 'TDQUAL'" in call
    assert "IBMSNAP_LOGMARKER" not in call and "create trigger" not in call
    assert call.count("'CAPSTART'") == 3


def test_every_registration_is_waited_for_together_in_the_container(cdc):
    """One wait for all the tables, looped inside the container over one
    connection: a docker exec and a connect per look cost ~3s each."""
    cdc._register_tables("C", "S", ["A", "B", "D"])
    [wait] = [call for call in cdc.calls if "sysdummy1" in call]
    assert "'A', 'B', 'D'" in wait and wait.count("connect to") == 1
    assert 'if [ "$got" = "0 3" ]' in wait and "while [ $SECONDS -lt 300 ]" in wait
    # the query in the connecting shell: `$(db2 ...)` would run it in a
    # subshell, which the CLP's connection does not reach (SQL1024N)
    assert '> "$rows" 2>&1' in wait and "$(db2" not in wait


def test_a_wait_that_never_ends_says_what_it_last_read(cdc):
    def run(container, body, **kwargs):
        raise backend.TdxError("db2 instance command failed: last read: 1 2")

    cdc._run = run
    with pytest.raises(
        backend.TdxError, match=r"CDC: timed out waiting for x .*last read: 1 2"
    ):
        cdc._wait_until("C", "select 1 from sysibm.sysdummy1", "0 3", what="x")


def test_what_an_install_needs_is_read_in_one_call(cdc):
    """The schema's tables, this domain's CD tables -- held and existing, by
    its own prefix exactly -- the capture intervals and asncap's status, from
    one container call over one connection."""
    p = cdc.staging_prefix("S")
    other = "TD_X" + p[4:]  # `_` in LIKE matches it; the exact filter does not
    out = "\n".join([
        f"{backend._SECTION}tables",
        "EMPLOYEES",
        "SALARIES",
        f"{backend._SECTION}owners",
        f"{p}EMPLOYEES S_OLD",
        f"{other}EMPLOYEES S_X",
        f"{backend._SECTION}existing",
        f"{p}EMPLOYEES",
        "SQL0100W  No row was found",
        f"{backend._SECTION}intervals",
        "          1           1        1024",
        f"{backend._SECTION}asncap",
        "ASN0520I  AsnCcmd : The STATUS command response: ...",
        "rc=0",
    ])
    cdc._run = lambda container, body, **kwargs: cdc.calls.append(body) or out
    probe = cdc._install_probe("C", "S")
    [call] = cdc.calls
    assert call.count("connect to") == 1 and "asnccmd" in call
    assert probe.tables == ["EMPLOYEES", "SALARIES"]
    assert probe.owners == {f"{p}EMPLOYEES": "S_OLD"}
    assert probe.existing == {f"{p}EMPLOYEES"}
    assert probe.asncap_up and probe.tuned

    down = out.replace("ASN0520I", backend._ASNCAP_DOWN)
    cdc._run = lambda container, body, **kwargs: down.replace("1           1", "5 30")
    probe = cdc._install_probe("C", "S")
    assert not probe.asncap_up and not probe.tuned


def test_asncap_is_tuned_only_when_it_needs_it(cdc, monkeypatch):
    done: list[str] = []
    monkeypatch.setattr(cdc, "_container_and_schema", lambda meta: ("C", "S"))
    monkeypatch.setattr(cdc, "_claim_cd_tables", lambda *a: None)
    monkeypatch.setattr(cdc, "_register_tables", lambda *a: None)
    monkeypatch.setattr(cdc, "_tune_capture", lambda c: done.append("tune"))
    monkeypatch.setattr(cdc, "_setup_asncap", lambda c: done.append("setup"))
    for up, tuned, want in (
        (True, True, []),
        (True, False, ["tune"]),
        (False, False, ["setup"]),
    ):
        done.clear()
        probe = backend._Probe(["A"], {}, set(), asncap_up=up, tuned=tuned)
        monkeypatch.setattr(cdc, "_install_probe", lambda c, s, probe=probe: probe)
        cdc.after_install_schema({}, "m")
        assert done == want, (up, tuned)

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest

from tabsdatak.server._tdserver.render.assess import (
    Check,
    Help,
    Report,
    State,
    grouped,
    titled,
)

pytestmark = pytest.mark.unit


def check(
    name: str,
    state: State = State.SKIPPED,
    title: str = "The check could not run",
    facts: list[str] | None = None,
    help: list[Help] | None = None,
) -> Check:
    return Check(
        concern="Probe",
        check=name,
        state=state,
        title=title,
        facts=facts or [],
        help=help or [],
    )


def test_checks_failing_the_same_way_are_told_once():
    found = grouped([check("one"), check("two"), check("three")])
    assert len(found) == 1
    leading, also = found[0]
    assert leading.check == "one"
    assert [entry.check for entry in also] == ["two", "three"]


def test_checks_failing_differently_are_told_apart():
    found = grouped([check("one", title="One reason"), check("two", title="Another")])
    assert len(found) == 2


def test_the_same_words_in_different_states_are_told_apart():
    found = grouped([
        check("one", state=State.FAILED),
        check("two", state=State.WARNING),
    ])
    assert len(found) == 2


def test_the_same_words_with_different_facts_are_told_apart():
    found = grouped([
        check("one", facts=["port 2457"]),
        check("two", facts=["port 2458"]),
    ])
    assert len(found) == 2


def test_the_same_words_with_different_help_are_told_apart():
    found = grouped([
        check("one", help=[Help(text="Do this")]),
        check("two", help=[Help(text="Do that")]),
    ])
    assert len(found) == 2


def test_help_carrying_a_different_command_is_told_apart():
    found = grouped([
        check("one", help=[Help(text="Run", command="tdkserver init")]),
        check("two", help=[Help(text="Run", command="tdkserver delete")]),
    ])
    assert len(found) == 2


def test_the_order_checks_ran_in_is_kept():
    found = grouped([
        check("first", title="One reason"),
        check("second", title="Another"),
        check("third", title="One reason"),
    ])
    assert [leading.check for leading, _ in found] == ["first", "second"]
    assert [entry.check for entry in found[0][1]] == ["third"]


def test_nothing_failing_yields_nothing_to_tell():
    assert grouped([]) == []


def test_only_the_checks_worth_reading_reach_the_report():
    report = Report(
        checks=[
            check("passed", state=State.PASSED),
            check("waived", state=State.WAIVED),
            check("failed", state=State.FAILED),
            check("warned", state=State.WARNING),
            check("skipped", state=State.SKIPPED),
        ]
    )
    assert [entry.check for entry in report.failing] == ["failed", "warned", "skipped"]


def test_a_report_groups_its_checks_by_concern():
    report = Report(checks=[check("one"), check("two")])
    assert list(report.concerns) == ["Probe"]
    assert len(report.concerns["Probe"]) == 2


def test_a_title_keeps_the_small_words_small():
    assert titled("room for the pods") == "Room for the Pods"
    assert titled("cluster access") == "Cluster Access"


def test_a_title_capitalises_the_first_word_whatever_it_is():
    assert titled("the instance") == "The Instance"

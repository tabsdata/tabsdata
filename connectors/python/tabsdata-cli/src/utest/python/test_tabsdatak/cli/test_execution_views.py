#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from types import SimpleNamespace

from tabsdatak.cli.td import execution_utils
from tabsdatak.common._tabular import Column, Kind


def _row(**kwargs):
    defaults = dict(
        id="ID1",
        name="a_plan_with_a_rather_long_name",
        project=None,
        collection=SimpleNamespace(name="input"),
        function=SimpleNamespace(name="pub"),
        triggered_by="admin",
        started_on_str="2026-10-22 10:00:00",
        ended_on_str="None",
        status="Failed",
        type="ST",
        kwargs={"retry_at": None},
        _data={},
    )
    return SimpleNamespace(**{**defaults, **kwargs})


def test_plans_view_keeps_missing_values_for_the_placeholder():
    view = execution_utils.plans_view([_row()])
    assert view.count == "plans"
    assert view.rows == [
        [
            "ID1",
            "a_plan_with_a_rather_long_name",
            None,
            "input / pub",
            "admin",
            "2026-10-22 10:00:00",
            None,
            "Failed",
        ]
    ]
    assert [c.kind for c in view.columns][-1] is Kind.STATUS
    assert view.columns[1].width == 20


def test_indexed_workers_view_numbers_its_rows():
    view = execution_utils.workers_view([_row(), _row(id="ID2")], indexed=True)
    assert view.columns[0] == Column("Index", Kind.NUMBER)
    assert [row[:2] for row in view.rows] == [[1, "ID1"], [2, "ID2"]]
    assert view.rows[0][-1] == "Failed (needs action)"

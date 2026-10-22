#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import json
import re

import pytest

from tabsdatak.common._tabular import Kind
from tabsdatak.server._tdserver.render import table

pytestmark = pytest.mark.unit

ANSI = re.compile(r"\x1b\[[0-9;]*m")

INSTANCES = {
    "title": "Instances",
    "columns": [
        {"name": "Name", "color_scheme": None},
        {"name": "Status", "color_scheme": "status"},
        {"name": "Folder", "color_scheme": None},
    ],
    "rows": [["tabsdata", "Running", "/home/user/.tabsdata/instances/" + "x" * 80]],
}


def test_a_status_scheme_makes_a_status_column():
    view = table.view(INSTANCES)

    assert view.title == "Instances"
    assert [column.kind for column in view.columns] == [
        Kind.TEXT,
        Kind.STATUS,
        Kind.TEXT,
    ]
    assert view.rows == INSTANCES["rows"]


def test_every_table_of_the_payload_is_shown_whole(monkeypatch):
    stdout = io.StringIO()
    pods = {**INSTANCES, "title": "Pods", "rows": []}
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps([INSTANCES, pods])))
    monkeypatch.setattr("sys.stdout", stdout)

    assert table.main() == 0

    output = ANSI.sub("", stdout.getvalue())
    assert "Instances" in output
    assert "x" * 80 in output
    assert "Pods" in output
    assert "No rows found." in output

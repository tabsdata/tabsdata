#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import os
import re
from types import SimpleNamespace

import polars as pl

import tabsdatak.common._tabular as tabular
from tabsdatak.cli.td.table_group import (
    TABLE_DATA_MAX_WIDTH,
    TABLE_DATA_PLACEHOLDER,
    table_data_view,
)

# noinspection PyProtectedMember
from tabsdatak.common._tabular import Display, _printer, show
from tabsdatak.common._tabular._browser import TableBrowser

ANSI = re.compile(r"\x1b\[[0-9;]*m")
LONG_TITLE = "a title long enough to be cut by any table that shrinks its columns " * 3
BODY = "### Description\n\nCurrently, pyiceberg metadata is not cached."

FRAME = pl.DataFrame(
    {
        "number": [29799, 29768],
        "title": [LONG_TITLE, "[bold]not markup[/bold]"],
        "body": [BODY, None],
        "labels": [["bug", "P-medium"], []],
    }
    | {f"column_{index}": [index, index] for index in range(20)}
)


class Terminal(io.StringIO):
    def isatty(self) -> bool:
        return True


def terminal(monkeypatch, browsers: list, columns: int = 80) -> None:
    stdout = SimpleNamespace(isatty=lambda: True)
    monkeypatch.setattr(tabular, "sys", SimpleNamespace(stdout=stdout))
    monkeypatch.setattr(
        _printer.shutil,
        "get_terminal_size",
        lambda: os.terminal_size((columns, 24)),
    )
    monkeypatch.setattr(TableBrowser, "run", lambda self: browsers.append(self))


def test_the_view_names_the_project_collection_and_table():
    view = table_data_view("issue_triage", FRAME, "github_tfr", "jev")

    assert view.title == "Data"
    assert view.context == [
        ("Project", "jev"),
        ("Collection", "github_tfr"),
        ("Table", "issue_triage"),
        ("Size", "2 rows × 24 columns"),
    ]


def test_the_view_has_a_column_per_field_with_its_type():
    view = table_data_view("issue_triage", FRAME)

    assert [(column.name, column.detail) for column in view.columns[:4]] == [
        ("number", "i64"),
        ("title", "str"),
        ("body", "str"),
        ("labels", "list[str]"),
    ]


def test_the_view_keeps_rules_nulls_and_its_wider_limit():
    view = table_data_view("issue_triage", FRAME)

    assert view.rules
    assert view.placeholder == TABLE_DATA_PLACEHOLDER == "null"
    assert view.max_width == TABLE_DATA_MAX_WIDTH == 128
    assert view.rows[1][2] is None


def test_not_a_terminal_prints_every_value_whole(capsys):
    show(table_data_view("issue_triage", FRAME), Display(browse=True))

    output = capsys.readouterr().out
    assert "Data | Table: issue_triage - Size: 2 rows × 24 columns" in output
    assert LONG_TITLE.strip() in output
    assert "### Description↵↵Currently" in output
    assert "column_19" in output
    assert '["bug", "P-medium"]' in output
    assert "[bold]not markup[/bold]" in output
    assert "null" in output


def test_on_a_terminal_the_table_opens_the_browser(monkeypatch, capsys):
    browsers = []
    terminal(monkeypatch, browsers)

    show(table_data_view("issue_triage", FRAME), Display(browse=True))

    assert len(browsers) == 1
    assert capsys.readouterr().out == ""


def test_print_cuts_each_line_at_the_terminal_width(monkeypatch):
    browsers = []
    terminal(monkeypatch, browsers, columns=60)
    stream = Terminal()

    _printer.print_view(table_data_view("issue_triage", FRAME), Display(), file=stream)

    lines = [line for line in ANSI.sub("", stream.getvalue()).splitlines() if line]
    assert browsers == []
    assert lines[0] == "Data | Table: issue_triage - Size: 2 rows × 24 columns"
    table = [line for line in lines if line[:1] in "┏┃┡│├└"]
    assert table
    assert all(len(line) <= 60 for line in table)
    assert all(line.endswith("…") for line in table)


def test_print_wide_wraps_to_the_terminal_and_keeps_every_value(monkeypatch, capsys):
    browsers = []
    terminal(monkeypatch, browsers, columns=60)

    show(table_data_view("issue_triage", FRAME), Display(wrap=True))

    output = capsys.readouterr().out
    assert browsers == []
    assert "column_19" in output
    assert "pyiceberg" in output

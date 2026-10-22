#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import io
import os
import re
import sys

import pytest
from rich.console import Console

# noinspection PyProtectedMember
from tabsdatak.common._tabular import _printer
from tabsdatak.common._tabular import (
    Column,
    Display,
    RecordView,
    Kind,
    TableView,
    renderable,
    show,
)

ANSI = re.compile(r"\x1b\[[0-9;]*m")
LONG = "a value long enough to be cut by any table that keeps its columns short " * 2
BODY = "### Description\n\nCurrently, pyiceberg metadata is not cached."


class Terminal(io.StringIO):
    def isatty(self) -> bool:
        return True


def plans(**changes) -> TableView:
    view = TableView(
        title="Plans",
        context=["project 'sales'"],
        columns=[
            Column("ID", Kind.ID),
            Column("Name"),
            Column("Status", Kind.STATUS),
            Column("Rows", Kind.NUMBER),
        ],
        rows=[["01HV9ZQ8", LONG, "Failed", 1200], ["01HV9ZQ9", BODY, "Done", 3]],
        count="plans",
        notes=["More results available."],
    )
    for name, value in changes.items():
        setattr(view, name, value)
    return view


def printed(view, display=Display(), terminal=False, columns=80, monkeypatch=None):
    if monkeypatch is not None:
        monkeypatch.setattr(
            _printer.shutil,
            "get_terminal_size",
            lambda: os.terminal_size((columns, 24)),
        )
    stream = Terminal() if terminal else io.StringIO()
    _printer.print_view(view, display, file=stream)
    return ANSI.sub("", stream.getvalue())


@pytest.mark.common
def test_a_pipe_gets_every_value_whole():
    output = printed(plans())

    assert "Plans  ·  project 'sales'" in output
    assert LONG.strip() in output
    assert "### Description↵↵Currently" in output
    assert "Number of plans: 2" in output
    assert "More results available." in output


@pytest.mark.common
def test_the_block_starts_and_ends_with_a_blank_line():
    lines = printed(plans()).split("\n")

    assert lines[0] == ""
    assert lines[-2:] == ["", ""]


@pytest.mark.common
def test_a_terminal_cuts_the_cells_and_each_line_at_its_width(monkeypatch):
    output = printed(plans(), terminal=True, columns=60, monkeypatch=monkeypatch)

    table = [line for line in output.splitlines() if line[:1] in "┏┃┡│└"]
    assert table
    assert all(len(line) <= 60 for line in table)
    assert LONG.strip() not in output


@pytest.mark.common
def test_a_terminal_leaves_lines_that_fit_untouched(monkeypatch):
    output = printed(
        plans(rows=[["01HV9ZQ8", "short", "Done", 1]]),
        terminal=True,
        columns=200,
        monkeypatch=monkeypatch,
    )

    assert "…" not in output


@pytest.mark.common
def test_cells_stop_at_the_maximum_width(monkeypatch):
    output = printed(plans(), terminal=True, columns=500, monkeypatch=monkeypatch)

    assert LONG[:48] + "…" in output
    assert LONG[:49] + "…" not in output


@pytest.mark.common
def test_the_display_width_overrides_the_view_and_column_widths(monkeypatch):
    view = plans(columns=[Column("ID", Kind.ID), Column("Name", width=10)])
    view.rows = [["01HV9ZQ8", LONG]]

    by_column = printed(view, terminal=True, columns=500, monkeypatch=monkeypatch)
    by_display = printed(
        view,
        Display(max_width=5),
        terminal=True,
        columns=500,
        monkeypatch=monkeypatch,
    )

    assert LONG[:10] + "…" in by_column
    assert LONG[:5] + "…" in by_display
    assert "01HV9…" in by_display


@pytest.mark.common
def test_wrap_fits_the_terminal_and_keeps_every_value(monkeypatch):
    output = printed(
        plans(),
        Display(wrap=True),
        terminal=True,
        columns=60,
        monkeypatch=monkeypatch,
    )

    lines = output.splitlines()
    assert all(len(line) <= 60 for line in lines)
    assert "…" not in output
    words = " ".join(line.strip("│ ") for line in lines)
    assert "pyiceberg" in words
    assert "Currently," in output


@pytest.mark.common
def test_the_header_frame_is_heavier_than_the_body():
    output = printed(plans())

    assert "┏" in output
    assert "┡" in output
    assert "└" in output


@pytest.mark.common
def test_rows_have_rules_only_when_the_view_asks():
    assert "├" not in printed(plans())
    assert "├" in printed(plans(rules=True))


@pytest.mark.common
def test_numbers_are_aligned_to_the_right():
    output = printed(plans())

    assert "│    3 │" in output


@pytest.mark.common
def test_a_column_detail_is_a_second_header_line():
    view = plans(columns=[Column("Size", Kind.NUMBER, detail="MB")])
    view.rows = [[1]]

    lines = printed(view).splitlines()
    header = [line for line in lines if line.startswith("┃")]
    assert "Size" in header[0]
    assert "MB" in header[1]


@pytest.mark.common
def test_an_empty_table_says_so_instead_of_drawing_a_frame():
    output = printed(plans(rows=[]))

    assert "No plans found." in output
    assert "More results available." in output
    assert "┏" not in output
    assert "Number of plans" not in output


@pytest.mark.common
def test_a_custom_empty_message_is_used():
    assert "(no inflight runs)" in printed(plans(rows=[], empty="(no inflight runs)"))


@pytest.mark.common
def test_a_title_wider_than_the_table_is_printed_whole():
    view = TableView(
        title="A title much wider than the narrow one-column table beneath it",
        columns=[Column("N")],
        rows=[[1]],
    )

    assert view.title in printed(view)


@pytest.mark.common
def test_a_record_shows_its_fields_without_a_header():
    record = RecordView(
        title="User",
        fields=[("Name", "admin"), ("Email", None), ("Status", "Running")],
        kinds={"Status": Kind.STATUS},
    )

    output = printed(record)

    assert output.splitlines()[1] == "User"
    assert "┌" in output
    assert "┏" not in output
    assert "│ Email  │ -       │" in output


@pytest.mark.common
def test_a_record_keeps_every_line_and_cuts_only_the_long_ones(monkeypatch):
    record = RecordView(
        title="Collection",
        fields=[("Connection info", "id: 1\ntype: " + "x" * 80 + "\nkind: def")],
    )

    output = printed(record, terminal=True, columns=500, monkeypatch=monkeypatch)

    assert "id: 1" in output
    assert "kind: def" in output
    assert "type: " + "x" * 42 + "…" in output


@pytest.mark.common
def test_a_field_width_of_zero_never_cuts(monkeypatch):
    token = "t" * 90
    record = RecordView(
        title="Service token created",
        fields=[("Token", token), ("Comment", "c" * 90)],
        widths={"Token": 0},
    )

    output = printed(record, terminal=True, columns=500, monkeypatch=monkeypatch)

    assert token in output
    assert "c" * 48 + "…" in output


def rendered(renderable) -> str:
    stream = io.StringIO()
    Console(file=stream, width=200).print(renderable)
    return ANSI.sub("", stream.getvalue())


@pytest.mark.common
def test_a_renderable_holds_the_title_the_cut_table_and_the_footer():
    output = rendered(renderable(plans()))

    assert output.startswith("Plans  ·  project 'sales'")
    assert LONG[:48] + "…" in output
    assert "Number of plans: 2" in output


@pytest.mark.common
def test_an_empty_renderable_says_so():
    output = rendered(renderable(plans(rows=[])))

    assert "No plans found." in output
    assert "┏" not in output


@pytest.mark.common
def test_several_views_share_one_block(monkeypatch):
    stream = io.StringIO()
    monkeypatch.setattr(sys, "stdout", stream)

    show([plans(), plans(title="Workers")])

    lines = ANSI.sub("", stream.getvalue()).split("\n")
    assert lines[0] == ""
    assert lines.count("") == 4
    assert any(line.startswith("Workers") for line in lines)

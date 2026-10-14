#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace

import polars as pl
import pytest

from tabsdatak.cli import _table_viewer
from tabsdatak.cli._table_viewer import (
    CELL_LIMIT,
    TableViewer,
    ValueScreen,
    build_table,
    clickable_cell,
    short_cell,
    show_table,
    table_title,
    whole_cell,
)

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


def terminal(monkeypatch, viewers: list, columns: int = 80) -> None:
    monkeypatch.setattr(
        _table_viewer,
        "sys",
        SimpleNamespace(stdout=SimpleNamespace(isatty=lambda: True)),
    )
    monkeypatch.setattr(
        _table_viewer.shutil,
        "get_terminal_size",
        lambda: os.terminal_size((columns, 24)),
    )
    monkeypatch.setattr(TableViewer, "run", lambda self: viewers.append(self))


def viewer(frame: pl.DataFrame = FRAME) -> TableViewer:
    return TableViewer("issue_triage", table_title("issue_triage", frame), frame)


def test_a_short_cell_shows_its_first_line_and_a_mark():
    cell = short_cell(BODY, 0, 2)

    assert cell.plain == "### Description…"


def test_a_short_cell_cuts_long_values_at_the_limit():
    cell = short_cell("x" * (CELL_LIMIT + 10), 0, 1)

    assert cell.plain == "x" * CELL_LIMIT + "…"


def test_a_value_that_fits_gets_no_mark():
    assert short_cell("bug", 0, 0).plain == "bug"
    assert short_cell(None, 0, 0).plain == "null"


def test_only_the_mark_of_a_clickable_cell_opens_the_value():
    cell = clickable_cell(BODY, 3, 2)

    clickable = [span for span in cell.spans if span.style.meta]
    assert len(clickable) == 1
    assert cell.plain[clickable[0].start : clickable[0].end] == "…"
    assert clickable[0].style.meta == {"@click": "app.show_value(3, 2)"}


def test_a_whole_cell_keeps_every_line_on_one():
    cell = whole_cell(BODY, 0, 2)

    assert cell.plain == "### Description↵↵Currently, pyiceberg metadata is not cached."


def test_not_a_terminal_prints_every_value_whole(capsys):
    show_table("issue_triage", FRAME)

    output = capsys.readouterr().out
    assert "Data of table 'issue_triage'  ·  2 rows × 24 columns" in output
    assert LONG_TITLE.strip() in output
    assert "### Description↵↵Currently" in output
    assert "column_19" in output
    assert '["bug", "P-medium"]' in output
    assert "[bold]not markup[/bold]" in output


@pytest.mark.parametrize("frame", [FRAME, FRAME.head(1).select("number")])
def test_on_a_terminal_every_table_opens_the_viewer(monkeypatch, capsys, frame):
    viewers = []
    terminal(monkeypatch, viewers)

    show_table("issue_triage", frame)

    assert len(viewers) == 1
    assert capsys.readouterr().out == ""


def test_print_cuts_the_cells_and_each_line_at_the_terminal_width(monkeypatch, capsys):
    viewers = []
    terminal(monkeypatch, viewers, columns=60)

    show_table("issue_triage", FRAME, viewer=False)

    lines = capsys.readouterr().out.splitlines()
    title, table = lines[0], lines[1:]
    assert viewers == []
    assert title == "Data of table 'issue_triage'  ·  2 rows × 24 columns"
    assert all(len(line) <= 60 for line in table)
    assert all(line.endswith("…") for line in table)


def test_print_wide_keeps_every_value_whole(monkeypatch, capsys):
    viewers = []
    terminal(monkeypatch, viewers, columns=60)

    show_table("issue_triage", FRAME, viewer=False, wide=True)

    output = capsys.readouterr().out
    assert viewers == []
    assert LONG_TITLE.strip() in output
    assert "### Description↵↵Currently" in output
    assert "column_19" in output


def test_print_leaves_lines_that_fit_untouched(monkeypatch, capsys):
    viewers = []
    terminal(monkeypatch, viewers, columns=200)

    show_table("issue_triage", FRAME.select("number", "labels"), viewer=False)

    assert "…" not in capsys.readouterr().out


def test_a_title_wider_than_the_table_is_printed_whole(capsys):
    show_table("issue_triage", FRAME.head(1).select("number"))

    output = capsys.readouterr().out
    assert "Data of table 'issue_triage'  ·  1 rows × 1 columns" in output


def test_the_table_has_a_column_per_field_with_its_type():
    table = build_table(FRAME, short_cell)

    headers = [column.header.plain for column in table.columns]
    assert headers[:4] == [
        "number\nInt64",
        "title\nString",
        "body\nString",
        "labels\nList(String)",
    ]
    assert all(column.no_wrap for column in table.columns)


def test_the_viewer_keeps_every_column_at_its_width_and_quits_with_q():
    async def run():
        app = viewer()
        async with app.run_test(size=(80, 20)) as pilot:
            table = app.query_one("#table")
            shown = table.virtual_size.width
            await pilot.press("q")
        return shown, app.return_code

    shown, return_code = asyncio.run(run())
    assert shown > CELL_LIMIT + 80
    assert return_code == 0


def test_clicking_a_mark_opens_the_whole_value():
    frame = pl.DataFrame({"body": [BODY]})

    async def run():
        app = viewer(frame)
        async with app.run_test(size=(80, 20)) as pilot:
            mark = 1 + 1 + 1 + len("### Description")
            await pilot.click("#table", offset=(mark, 4))
            await pilot.pause()
            screen = app.screen
            title = str(screen.query_one("#box-title").render())
            value = str(screen.query_one("#box-value").render())
            return isinstance(screen, ValueScreen), title, value

    opened, title, value = asyncio.run(run())
    assert opened
    assert title == "body  ·  row 1"
    assert value == BODY


@pytest.mark.parametrize("close", ["escape", "q", "click outside"])
def test_the_value_window_closes_with_esc_q_or_a_click_outside(close):
    async def run():
        app = viewer()
        async with app.run_test(size=(80, 20)) as pilot:
            app.action_show_value(0, FRAME.columns.index("body"))
            await pilot.pause()
            if close == "click outside":
                await pilot.click(offset=(0, 0))
            else:
                await pilot.press(close)
            await pilot.pause()
            return isinstance(app.screen, ValueScreen)

    assert asyncio.run(run()) is False


def test_a_click_inside_the_value_window_keeps_it_open():
    async def run():
        app = viewer()
        async with app.run_test(size=(80, 20)) as pilot:
            app.action_show_value(0, FRAME.columns.index("body"))
            await pilot.pause()
            await pilot.click("#box-value", offset=(2, 0))
            await pilot.pause()
            return isinstance(app.screen, ValueScreen)

    assert asyncio.run(run()) is True


@pytest.mark.parametrize(
    "platform, term_program, key",
    [
        ("linux", None, "Shift"),
        ("win32", None, "Shift"),
        ("darwin", "Apple_Terminal", "fn"),
        ("darwin", "iTerm.app", "⌥"),
        ("darwin", None, "⌥"),
    ],
)
def test_the_select_key_depends_on_the_terminal(
    monkeypatch, platform, term_program, key
):
    monkeypatch.setattr(_table_viewer, "sys", SimpleNamespace(platform=platform))
    if term_program is None:
        monkeypatch.delenv("TERM_PROGRAM", raising=False)
    else:
        monkeypatch.setenv("TERM_PROGRAM", term_program)

    assert _table_viewer.select_key() == key


def test_the_footers_explain_the_keys():
    key = _table_viewer.select_key()

    assert _table_viewer.footer_text().plain == (
        f"q/esc Quit  ·  wheel Scroll  ·  … Whole value  ·  {key}+drag Select"
    )
    assert _table_viewer.value_footer_text().plain == (
        f"esc Close  ·  click outside Close  ·  {key}+drag Select"
    )


@pytest.mark.parametrize("key", ["q", "escape"])
def test_q_or_esc_quits_the_viewer(key):
    async def run():
        app = viewer()
        async with app.run_test(size=(80, 20)) as pilot:
            await pilot.press(key)
            await pilot.pause()
            return app.is_running

    assert asyncio.run(run()) is False


def test_esc_in_the_value_window_closes_only_the_window():
    async def run():
        app = viewer()
        async with app.run_test(size=(80, 20)) as pilot:
            app.action_show_value(0, FRAME.columns.index("body"))
            await pilot.pause()
            await pilot.press("escape")
            await pilot.pause()
            return app.is_running, isinstance(app.screen, ValueScreen)

    assert asyncio.run(run()) == (True, False)


def test_the_title_names_the_table_collection_and_project():
    title = table_title("issue_triage", FRAME, "github_tfr", "jev")

    assert title.plain == (
        "Data of table 'issue_triage'  ·  collection 'github_tfr'  ·  "
        "project 'jev'  ·  2 rows × 24 columns"
    )

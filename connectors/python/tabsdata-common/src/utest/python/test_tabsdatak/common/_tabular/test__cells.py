#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import pytest
from rich.text import Text

# noinspection PyProtectedMember
from tabsdatak.common._tabular._cells import (
    cut_cell,
    value_text,
    whole_cell,
    wrapped_cell,
)

# noinspection PyProtectedMember
from tabsdatak.common._tabular._theme import CELL, RED, status_style

# noinspection PyProtectedMember
from tabsdatak.common._tabular._view import Column, Kind

BODY = "### Description\n\nCurrently, pyiceberg metadata is not cached."
TEXT = Column("body")


@pytest.mark.common
def test_a_cut_cell_shows_its_first_line_and_a_mark():
    assert cut_cell(BODY, TEXT, "-", 128).plain == "### Description…"


@pytest.mark.common
def test_a_cut_cell_stops_at_the_limit():
    assert cut_cell("x" * 30, TEXT, "-", 20).plain == "x" * 20 + "…"


@pytest.mark.common
def test_a_limit_of_zero_keeps_the_whole_first_line():
    assert cut_cell("x" * 300, TEXT, "-", 0).plain == "x" * 300


@pytest.mark.common
def test_a_value_that_fits_gets_no_mark():
    assert cut_cell("bug", TEXT, "-", 128).plain == "bug"


@pytest.mark.common
def test_a_missing_value_shows_the_placeholder():
    assert cut_cell(None, TEXT, "null", 128).plain == "null"
    assert whole_cell(None, TEXT, "-").plain == "-"
    assert wrapped_cell(None, TEXT, "-").plain == "-"


@pytest.mark.common
def test_only_the_mark_of_a_clickable_cell_runs_the_action():
    cell = cut_cell(BODY, TEXT, "-", 128, "app.show_value(3, 2)")

    clickable = [span for span in cell.spans if span.style.meta]
    assert len(clickable) == 1
    assert cell.plain[clickable[0].start : clickable[0].end] == "…"
    assert clickable[0].style.meta == {"@click": "app.show_value(3, 2)"}


@pytest.mark.common
def test_a_whole_cell_keeps_every_line_on_one():
    assert whole_cell(BODY, TEXT, "-").plain == (
        "### Description↵↵Currently, pyiceberg metadata is not cached."
    )


@pytest.mark.common
def test_a_wrapped_cell_keeps_its_line_breaks():
    assert wrapped_cell(BODY, TEXT, "-").plain == BODY


@pytest.mark.common
def test_lists_and_dicts_show_as_json():
    assert value_text(["bug", "P-medium"]) == '["bug", "P-medium"]'
    assert value_text({"a": 1}) == '{"a": 1}'


@pytest.mark.common
def test_markup_in_values_is_shown_as_written():
    assert cut_cell("[bold]x[/bold]", TEXT, "-", 128).plain == "[bold]x[/bold]"


@pytest.mark.common
def test_rich_text_values_keep_their_style():
    value = Text("ok", style="bold")

    assert str(cut_cell(value, TEXT, "-", 128).style) == "bold"


@pytest.mark.common
def test_status_cells_take_the_status_colour():
    cell = cut_cell("Failed (needs action)", Column("Status", Kind.STATUS), "-", 128)

    assert str(cell.style) == RED


@pytest.mark.common
@pytest.mark.parametrize(
    "status, style",
    [
        ("Done", "#6ab35c"),
        ("committed", "#6ab35c"),
        ("Running", "#39aaf1"),
        ("Run requested", "#39aaf1"),
        ("On hold", "#ccb233"),
        ("on_hold", "#ccb233"),
        ("Failed (retrying at 12:00, failure 2 in a row)", "#ef8282"),
        ("Something new", CELL),
    ],
)
def test_statuses_follow_the_ui_status_colours(status, style):
    assert status_style(status) == style

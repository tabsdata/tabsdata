#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import os
import shutil
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING

from rich import box
from rich.console import Console
from rich.segment import Segment, Segments
from rich.style import Style
from rich.table import Table
from rich.text import Text
from textual import events
from textual.app import App, ComposeResult
from textual.containers import ScrollableContainer, Vertical
from textual.screen import ModalScreen
from textual.widgets import Static

if TYPE_CHECKING:
    import polars as pl

# The colors of tabsdata-ui (new-ui/src/styles/globals.css), dark theme.
GREEN = "#6ab35c"
BLUE = "#39aaf1"
GRAY_100 = "#0a0a0a"
GRAY_200 = "#1f1f1f"
GRAY_300 = "#3d3d3d"
GRAY_500 = "#8f8f8f"
GRAY_900 = "#d6d6d6"

RENDER_LIMIT = 1_000_000
CELL_LIMIT = 128
CUT_MARK = "…"
LINE_BREAK_MARK = "↵"

CellRenderer = Callable[[object, int, int], Text]


def show_table(
    name: str,
    frame: pl.DataFrame,
    collection: str | None = None,
    project: str | None = None,
    viewer: bool = True,
    wide: bool = False,
) -> None:
    """Show `frame` with every column at full width and one line per row.

    On a terminal the table opens in a viewer that scrolls both ways. Each
    cell shows the first line of its value, up to `CELL_LIMIT` characters,
    and a mark that opens the whole value. With `viewer` False the table is
    printed instead: with the same cells, each line cut at the terminal
    width, or with every value whole when `wide` is True and the terminal
    wraps the lines. Output that is not a terminal gets every value whole.
    """
    title = table_title(name, frame, collection, project)
    terminal = sys.stdout.isatty()
    if viewer and terminal:
        TableViewer(name, title, frame).run()
        return
    whole = wide or not terminal
    table = build_table(frame, whole_cell if whole else short_cell)
    console = Console(width=max(natural_width(table), title.cell_len))
    console.print(title)
    if whole:
        console.print(table)
        return
    columns = shutil.get_terminal_size().columns
    lines = console.render_lines(table, console.options, pad=False)
    console.print(
        Segments(
            segment
            for line in lines
            for segment in [*cut_line(line, columns), Segment.line()]
        )
    )


def cut_line(line: list[Segment], columns: int) -> list[Segment]:
    if Segment.get_line_length(line) <= columns:
        return line
    kept = Segment.adjust_line_length(line, columns - 1)
    return [*kept, Segment(CUT_MARK, Style(color=GRAY_500))]


def natural_width(table: Table) -> int:
    return Console(width=RENDER_LIMIT).measure(table).maximum


def table_title(
    name: str,
    frame: pl.DataFrame,
    collection: str | None = None,
    project: str | None = None,
) -> Text:
    parts = [f"Data of table '{name}'"]
    if collection:
        parts.append(f"collection '{collection}'")
    if project:
        parts.append(f"project '{project}'")
    parts.append(f"{frame.height} rows × {frame.width} columns")
    return Text("  ·  ".join(parts), style=BLUE)


def build_table(frame: pl.DataFrame, render_cell: CellRenderer) -> Table:
    table = Table(box=box.SQUARE, border_style=GREEN, show_lines=True, header_style="")
    for column, dtype in frame.schema.items():
        header = Text.assemble((column, f"bold {GREEN}"), (f"\n{dtype}", GRAY_500))
        table.add_column(header, no_wrap=True)
    for row, values in enumerate(frame.iter_rows()):
        table.add_row(
            *(render_cell(value, row, column) for column, value in enumerate(values))
        )
    return table


def value_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value, default=str)
    return str(value)


def null_text() -> Text:
    return Text("null", style=f"italic {GRAY_500}")


def whole_cell(value: object, row: int, column: int) -> Text:
    text = value_text(value)
    if text is None:
        return null_text()
    cell = Text(style=GRAY_900)
    for index, line in enumerate(text.split("\n")):
        if index:
            cell.append(LINE_BREAK_MARK, style=GRAY_500)
        cell.append(line)
    return cell


def short_cell(value: object, row: int, column: int) -> Text:
    return cut_cell(value, None)


def clickable_cell(value: object, row: int, column: int) -> Text:
    return cut_cell(value, f"app.show_value({row}, {column})")


def cut_cell(value: object, action: str | None) -> Text:
    """The first line of `value`, up to `CELL_LIMIT` characters, then a mark
    when anything is left out; clicking the mark runs `action`."""
    text = value_text(value)
    if text is None:
        return null_text()
    shown = text.split("\n", 1)[0][:CELL_LIMIT]
    cell = Text(shown, style=GRAY_900)
    if shown != text:
        meta = {"@click": action} if action else {}
        cell.append(CUT_MARK, style=Style(color=GRAY_500, meta=meta))
    return cell


class TableViewer(App):
    CSS = f"""
    Screen {{ background: {GRAY_100}; }}
    #title {{ padding: 0 1; height: 1; }}
    #viewport {{
        background: {GRAY_100};
        scrollbar-color: {GRAY_300};
        scrollbar-color-hover: {GREEN};
        scrollbar-color-active: {GREEN};
        scrollbar-background: {GRAY_200};
        height: 1fr;
    }}
    #table {{
        padding: 0 1;
        link-color: {GRAY_500};
        link-background: {GRAY_100};
        link-style: none;
        link-color-hover: {GREEN};
        link-background-hover: {GRAY_100};
        link-style-hover: none;
    }}
    #footer {{ dock: bottom; height: 1; padding: 0 1; background: {GRAY_200}; }}
    """
    ENABLE_COMMAND_PALETTE = False
    BINDINGS = [("q", "quit", "Quit"), ("escape", "quit", "Quit")]

    def __init__(self, name: str, title: Text, frame: pl.DataFrame):
        super().__init__()
        self.table_name = name
        self.table_title = title
        self.frame = frame

    def compose(self) -> ComposeResult:
        table = build_table(self.frame, clickable_cell)
        yield Static(self.table_title, id="title")
        with ScrollableContainer(id="viewport"):
            content = Static(table, id="table")
            # Measured here: measuring against the screen would shrink columns.
            content.styles.width = natural_width(table) + 2
            yield content
        yield Static(footer_text(), id="footer")

    def action_show_value(self, row: int, column: int) -> None:
        name = self.frame.columns[column]
        value = value_text(self.frame.row(row)[column]) or ""
        self.push_screen(ValueScreen(f"{name}  ·  row {row + 1}", value))


class ValueScreen(ModalScreen):
    """A floating window with the whole value of one cell."""

    CSS = f"""
    ValueScreen {{ align: center middle; background: {GRAY_100} 60%; }}
    #box {{
        width: 80%;
        height: 80%;
        border: solid {GREEN};
        background: {GRAY_100};
    }}
    #box-title {{ height: 1; padding: 0 1; color: {BLUE}; }}
    #box-scroll {{
        height: 1fr;
        scrollbar-color: {GRAY_300};
        scrollbar-color-hover: {GREEN};
        scrollbar-color-active: {GREEN};
        scrollbar-background: {GRAY_200};
    }}
    #box-value {{ padding: 0 1; color: {GRAY_900}; }}
    #box-footer {{ height: 1; padding: 0 1; background: {GRAY_200}; }}
    """
    BINDINGS = [("escape", "close", "Close"), ("q", "close", "Close")]

    def __init__(self, title: str, value: str):
        super().__init__()
        self.value_title = title
        self.value = value

    def compose(self) -> ComposeResult:
        with Vertical(id="box"):
            yield Static(Text(self.value_title, style=BLUE), id="box-title")
            with ScrollableContainer(id="box-scroll"):
                yield Static(Text(self.value, style=GRAY_900), id="box-value")
            yield Static(value_footer_text(), id="box-footer")

    def action_close(self) -> None:
        self.dismiss()

    def on_click(self, event: events.Click) -> None:
        if event.widget is self:
            self.dismiss()


def footer_text() -> Text:
    return hints(
        ("q/esc", "Quit"),
        ("wheel", "Scroll"),
        (CUT_MARK, "Whole value"),
        (f"{select_key()}+drag", "Select"),
    )


def value_footer_text() -> Text:
    return hints(
        ("esc", "Close"),
        ("click outside", "Close"),
        (f"{select_key()}+drag", "Select"),
    )


def hints(*pairs: tuple[str, str]) -> Text:
    text = Text()
    for index, (key, action) in enumerate(pairs):
        if index:
            text.append("  ·  ", style=GRAY_300)
        text.append(key, style=BLUE)
        text.append(f" {action}", style=GRAY_500)
    return text


def select_key() -> str:
    """The key that lets the mouse select text while the viewer captures it.

    Each terminal picks its own: Shift on Windows and Linux, Option in most
    macOS terminals, and fn in macOS Terminal.
    """
    if sys.platform != "darwin":
        return "Shift"
    if os.environ.get("TERM_PROGRAM") == "Apple_Terminal":
        return "fn"
    return "⌥"

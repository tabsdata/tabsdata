#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import sys

from rich.console import Group
from rich.text import Text
from textual import events
from textual.app import App, ComposeResult
from textual.containers import ScrollableContainer, Vertical
from textual.screen import ModalScreen
from textual.widgets import Static

from tabsdatak.common._tabular._cells import CUT_MARK, value_text
from tabsdatak.common._tabular._printer import (
    build_table,
    cut_renderer,
    footer_lines,
    natural_width,
    title_line,
    view_columns,
    view_rows,
    wrapped_renderer,
)
from tabsdatak.common._tabular._theme import (
    BACKGROUND,
    BORDER,
    CELL,
    DELIMITER as DELIMITER_STYLE,
    KEY,
    MUTED,
    PANEL,
    SEPARATOR,
)
from tabsdatak.common._tabular._view import (
    DETAIL_DELIMITER,
    TOPIC_DELIMITER,
    Display,
    RecordView,
    TableView,
)

SCROLLBARS = f"""
        scrollbar-color: {SEPARATOR};
        scrollbar-color-hover: {BORDER};
        scrollbar-color-active: {BORDER};
        scrollbar-background: {PANEL};
"""


class TableBrowser(App):
    CSS = f"""
    Screen {{ background: {BACKGROUND}; }}
    #title {{ padding: 0 1; height: 1; }}
    #viewport {{
        background: {BACKGROUND};
        {SCROLLBARS}
        height: 1fr;
    }}
    #table {{
        padding: 0 1;
        link-color: {MUTED};
        link-background: {BACKGROUND};
        link-style: none;
        link-color-hover: {BORDER};
        link-background-hover: {BACKGROUND};
        link-style-hover: none;
    }}
    #footer {{ dock: bottom; height: 1; padding: 0 1; background: {PANEL}; }}
    """
    ENABLE_COMMAND_PALETTE = False
    BINDINGS = [("q", "quit", "Quit"), ("escape", "quit", "Quit")]

    def __init__(self, view: TableView | RecordView, display: Display = Display()):
        super().__init__()
        self.view = view
        self.presentation = display

    def compose(self) -> ComposeResult:
        if self.presentation.wrap:
            table = build_table(self.view, wrapped_renderer(self.view))
        else:
            table = build_table(
                self.view,
                cut_renderer(
                    self.view,
                    self.presentation,
                    lambda row, column: f"app.show_value({row}, {column})",
                ),
            )
        content = Group(table, *footer_lines(self.view))
        yield Static(title_line(self.view), id="title")
        with ScrollableContainer(id="viewport"):
            body = Static(content, id="table")
            body.styles.width = natural_width(content) + 2
            yield body
        yield Static(footer_text(), id="footer")

    def action_show_value(self, row: int, column: int) -> None:
        name = view_columns(self.view)[column].name
        if isinstance(self.view, RecordView):
            name = self.view.fields[row][0]
        value = value_text(view_rows(self.view)[row][column]) or ""
        position = (
            ""
            if isinstance(self.view, RecordView)
            else f"{TOPIC_DELIMITER}Row {row + 1}"
        )
        self.push_screen(ValueScreen(f"{name}{position}", value))


class ValueScreen(ModalScreen):
    CSS = f"""
    ValueScreen {{ align: center middle; background: {BACKGROUND} 60%; }}
    #box {{
        width: 80%;
        height: 80%;
        border: solid {BORDER};
        background: {BACKGROUND};
    }}
    #box-title {{ height: 1; padding: 0 1; color: {KEY}; }}
    #box-scroll {{
        height: 1fr;
        {SCROLLBARS}
    }}
    #box-value {{ padding: 0 1; color: {CELL}; }}
    #box-footer {{ height: 1; padding: 0 1; background: {PANEL}; }}
    """
    BINDINGS = [("escape", "close", "Close"), ("q", "close", "Close")]

    def __init__(self, title: str, value: str):
        super().__init__()
        self.value_title = title
        self.value = value

    def compose(self) -> ComposeResult:
        with Vertical(id="box"):
            yield Static(Text(self.value_title, style=KEY), id="box-title")
            with ScrollableContainer(id="box-scroll"):
                yield Static(Text(self.value, style=CELL), id="box-value")
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
            text.append(DETAIL_DELIMITER, style=DELIMITER_STYLE)
        text.append(key, style=KEY)
        text.append(f" {action}", style=MUTED)
    return text


def select_key() -> str:
    if sys.platform != "darwin":
        return "Shift"
    if os.environ.get("TERM_PROGRAM") == "Apple_Terminal":
        return "fn"
    return "⌥"

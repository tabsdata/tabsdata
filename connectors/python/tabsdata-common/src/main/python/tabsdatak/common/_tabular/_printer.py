#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import shutil
import sys
from collections.abc import Callable
from typing import IO

from rich import box
from rich.console import Console, RenderableType
from rich.segment import Segment, Segments
from rich.style import Style
from rich.table import Table
from rich.text import Text

from tabsdatak.common._tabular._cells import (
    CUT_MARK,
    cut_cell,
    cut_lines,
    whole_cell,
    wrapped_cell,
)
from tabsdatak.common._tabular._theme import (
    BORDER,
    CONTEXT,
    DETAIL,
    HEADING,
    MUTED,
    SEPARATOR,
    TITLE,
)
from tabsdatak.common._tabular._view import Column, Display, Kind, RecordView, TableView

RENDER_LIMIT = 1_000_000
CONTEXT_SEPARATOR = "  ·  "

CellRenderer = Callable[[object, int, int], Text]


def title_line(view: TableView | RecordView) -> Text:
    line = Text()
    line.append(view.title, style=TITLE)
    for part in view.context:
        line.append(CONTEXT_SEPARATOR, style=SEPARATOR)
        line.append(part, style=CONTEXT)
    return line


def footer_lines(view: TableView | RecordView) -> list[Text]:
    return [Text(line, style=MUTED) for line in view.footer()]


def view_columns(view: TableView | RecordView) -> list[Column]:
    if isinstance(view, TableView):
        return view.columns
    return [Column("Field"), Column("Value")]


def view_rows(view: TableView | RecordView) -> list[list[object]]:
    if isinstance(view, TableView):
        return view.rows
    return [[name, value] for name, value in view.fields]


def cell_column(view: TableView | RecordView, row: int, column: int) -> Column:
    if isinstance(view, TableView):
        return view.columns[column]
    name = view.fields[row][0]
    return Column(name, view.kinds.get(name, Kind.TEXT), width=view.widths.get(name))


def cut_renderer(
    view: TableView | RecordView,
    display: Display,
    action: Callable[[int, int], str] | None = None,
) -> CellRenderer:
    cut = cut_lines if isinstance(view, RecordView) else cut_cell

    def render(value: object, row: int, column: int) -> Text:
        target = cell_column(view, row, column)
        limit = display.limit(view, target)
        command = action(row, column) if action else None
        return cut(value, target, view.placeholder, limit, command)

    return render


def whole_renderer(view: TableView | RecordView) -> CellRenderer:
    whole = wrapped_cell if isinstance(view, RecordView) else whole_cell

    def render(value: object, row: int, column: int) -> Text:
        return whole(value, cell_column(view, row, column), view.placeholder)

    return render


def wrapped_renderer(view: TableView | RecordView) -> CellRenderer:
    def render(value: object, row: int, column: int) -> Text:
        return wrapped_cell(value, cell_column(view, row, column), view.placeholder)

    return render


def build_table(
    view: TableView | RecordView,
    render_cell: CellRenderer,
    wrap: bool = False,
) -> Table:
    if isinstance(view, RecordView):
        return build_record_table(view, render_cell, wrap)
    table = Table(
        box=box.HEAVY_HEAD,
        border_style=BORDER,
        header_style="",
        show_lines=view.rules,
    )
    for column in view.columns:
        header = Text()
        header.append(column.name, style=HEADING)
        if column.detail:
            header.append(f"\n{column.detail}", style=DETAIL)
        table.add_column(
            header,
            justify="right" if column.kind is Kind.NUMBER else "left",
            no_wrap=not (wrap and column.wraps),
            overflow="fold" if wrap else "ellipsis",
        )
    for row, values in enumerate(view.rows):
        table.add_row(
            *(render_cell(value, row, column) for column, value in enumerate(values))
        )
    return table


def build_record_table(
    view: RecordView, render_cell: CellRenderer, wrap: bool
) -> Table:
    table = Table(box=box.SQUARE, border_style=BORDER, show_header=False)
    table.add_column(no_wrap=True)
    table.add_column(no_wrap=not wrap)
    for row, (name, value) in enumerate(view.fields):
        table.add_row(Text(name, style=HEADING), render_cell(value, row, 1))
    return table


def natural_width(renderable: RenderableType) -> int:
    return Console(width=RENDER_LIMIT).measure(renderable).maximum


def cut_line(line: list[Segment], columns: int) -> list[Segment]:
    if Segment.get_line_length(line) <= columns:
        return line
    kept = Segment.adjust_line_length(line, columns - 1)
    return [*kept, Segment(CUT_MARK, Style.parse(MUTED))]


def is_empty(view: TableView | RecordView) -> bool:
    return isinstance(view, TableView) and not view.rows


def print_view(
    view: TableView | RecordView,
    display: Display = Display(),
    file: IO[str] | None = None,
) -> None:
    output = file or sys.stdout
    terminal = output.isatty()
    columns = shutil.get_terminal_size().columns
    title = title_line(view)
    footer = footer_lines(view)
    if is_empty(view):
        lines = [
            Text(line, style=MUTED) for line in [view.empty_message(), *view.notes]
        ]
        width = max(title.cell_len, *(line.cell_len for line in lines))
        console = Console(file=output, width=width)
        console.print()
        console.print(title)
        for line in lines:
            console.print(line)
        console.print()
        return
    if display.wrap:
        table = build_table(view, wrapped_renderer(view), wrap=True)
        width = columns if terminal else natural_width(table)
    elif terminal:
        table = build_table(view, cut_renderer(view, display))
        width = natural_width(table)
    else:
        table = build_table(view, whole_renderer(view))
        width = natural_width(table)
    if not (display.wrap and terminal):
        width = max(width, title.cell_len, *(line.cell_len for line in footer))
    console = Console(file=output, width=width)
    console.print()
    console.print(title)
    if terminal and not display.wrap:
        lines = console.render_lines(table, console.options, pad=False)
        console.print(
            Segments(
                segment
                for line in lines
                for segment in [*cut_line(line, columns), Segment.line()]
            )
        )
    else:
        console.print(table)
    for line in footer:
        console.print(line)
    console.print()

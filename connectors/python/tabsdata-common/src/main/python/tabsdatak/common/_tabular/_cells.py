#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json

from rich.style import Style
from rich.text import Text

from tabsdatak.common._tabular._theme import CELL, MUTED, PLACEHOLDER, status_style
from tabsdatak.common._tabular._view import Column, Kind

CUT_MARK = "…"
LINE_BREAK_MARK = "↵"


def value_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, Text):
        return value.plain
    if isinstance(value, (list, dict)):
        return json.dumps(value, default=str)
    return str(value)


def styled(value: object, column: Column) -> Text:
    if isinstance(value, Text):
        return value.copy()
    text = value_text(value) or ""
    style = status_style(text) if column.kind is Kind.STATUS else CELL
    return Text(text, style=style)


def placeholder(text: str) -> Text:
    return Text(text, style=PLACEHOLDER)


def whole_cell(value: object, column: Column, empty: str) -> Text:
    if value is None:
        return placeholder(empty)
    lines = styled(value, column).split("\n")
    cell = Text()
    for index, line in enumerate(lines):
        if index:
            cell.append(LINE_BREAK_MARK, style=MUTED)
        cell.append_text(line)
    return cell


def wrapped_cell(value: object, column: Column, empty: str) -> Text:
    if value is None:
        return placeholder(empty)
    return styled(value, column)


def cut_cell(
    value: object,
    column: Column,
    empty: str,
    limit: int,
    action: str | None = None,
) -> Text:
    if value is None:
        return placeholder(empty)
    full = styled(value, column)
    first = full.split("\n")[0] if full.plain else full
    shown = first[:limit] if limit else first
    if shown.plain == full.plain:
        return shown
    shown.append(CUT_MARK, style=mark_style(action))
    return shown


def cut_lines(
    value: object,
    column: Column,
    empty: str,
    limit: int,
    action: str | None = None,
) -> Text:
    if value is None:
        return placeholder(empty)
    full = styled(value, column)
    lines = full.split("\n") if full.plain else [full]
    cell = Text()
    for index, line in enumerate(lines):
        if index:
            cell.append("\n")
        shown = line[:limit] if limit else line
        cell.append_text(shown)
        if shown.plain != line.plain:
            cell.append(CUT_MARK, style=mark_style(action))
    return cell


def mark_style(action: str | None) -> Style:
    meta = {"@click": action} if action else {}
    return Style.parse(MUTED) + Style(meta=meta)

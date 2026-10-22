#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
from collections.abc import Sequence

from tabsdatak.common._tabular._printer import print_view, renderable
from tabsdatak.common._tabular._theme import HELP_STYLES
from tabsdatak.common._tabular._view import (
    DEFAULT_MAX_WIDTH,
    Column,
    Display,
    Kind,
    RecordView,
    TableView,
)

__all__ = [
    "DEFAULT_MAX_WIDTH",
    "HELP_STYLES",
    "Column",
    "Display",
    "Kind",
    "RecordView",
    "TableView",
    "renderable",
    "show",
]

View = TableView | RecordView


def show(views: View | Sequence[View], display: Display = Display()) -> None:
    every = [views] if isinstance(views, (TableView, RecordView)) else list(views)
    if display.browse and sys.stdout.isatty():
        from tabsdatak.common._tabular._browser import TableBrowser

        for view in every:
            TableBrowser(view, display).run()
        return
    for index, view in enumerate(every):
        print_view(view, display, lead=index == 0)

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys

from tabsdatak.common._tabular._printer import print_view
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
    "Column",
    "Display",
    "RecordView",
    "Kind",
    "TableView",
    "show",
]


def show(view: TableView | RecordView, display: Display = Display()) -> None:
    if display.browse and sys.stdout.isatty():
        from tabsdatak.common._tabular._browser import TableBrowser

        TableBrowser(view, display).run()
        return
    print_view(view, display)

#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import sys

from rich import box
from rich.console import Console
from rich.table import Table

STATUS_STYLES = {
    "Running": "green",
    "Complete": "blue",
    "Succeeded": "blue",
    "Failed": "red",
    "Pending": "yellow",
}


def render(payload: dict) -> None:
    console = Console()
    table = Table(
        title=payload.get("title"),
        title_justify="left",
        title_style="bold magenta",
        box=box.SQUARE,
    )
    for column in payload.get("columns", []):
        table.add_column(column, no_wrap=True)
    status_column = payload.get("status_column")
    for row in payload.get("rows", []):
        cells = list(row)
        if status_column is not None and status_column < len(cells):
            value = cells[status_column]
            style = STATUS_STYLES.get(value)
            if style:
                cells[status_column] = f"[{style}]{value}[/{style}]"
        table.add_row(*cells)
    console.print(table)


def main() -> int:
    payload = json.load(sys.stdin)
    if isinstance(payload, list):
        for entry in payload:
            render(entry)
    else:
        render(payload)
    return 0


if __name__ == "__main__":
    sys.exit(main())

#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import sys

from rich import box
from rich.console import Console
from rich.table import Table

RENDER_WIDTH = 1024

COLOR_SCHEMES = {
    "status": {
        "Running": "green",
        "Complete": "blue",
        "Succeeded": "blue",
        "Failed": "red",
        "Stopped": "red",
        "Pending": "yellow",
    },
}


def render(payload: dict) -> None:
    console = Console(width=RENDER_WIDTH)
    table = Table(
        title=payload.get("title"),
        title_justify="left",
        title_style="bold magenta",
        box=box.SQUARE,
    )
    columns = payload.get("columns", [])
    for column in columns:
        table.add_column(column.get("name"), no_wrap=True)
    schemes = [COLOR_SCHEMES.get(column.get("color_scheme")) for column in columns]
    for row in payload.get("rows", []):
        cells = list(row)
        for index, cell in enumerate(cells):
            scheme = schemes[index] if index < len(schemes) else None
            if scheme:
                style = scheme.get(cell)
                if style:
                    cells[index] = f"[{style}]{cell}[/{style}]"
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

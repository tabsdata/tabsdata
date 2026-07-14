#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import json
import os
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


def render(console: Console, payload: dict) -> None:
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
        cells = [str(cell) for cell in row]
        for index, cell in enumerate(cells):
            scheme = schemes[index] if index < len(schemes) else None
            if scheme:
                style = scheme.get(cell)
                if style:
                    cells[index] = f"[{style}]{cell}[/{style}]"
        table.add_row(*cells)
    console.print(table)


def main() -> int:
    parser = argparse.ArgumentParser(description="Render a JSON payload of tables.")
    parser.add_argument(
        "--env",
        metavar="VARIABLE",
        help="environment variable holding the payload (default: read stdin)",
    )
    arguments = parser.parse_args()
    if arguments.env:
        payload = json.loads(os.environ[arguments.env])
    else:
        payload = json.load(sys.stdin)
    console = Console(width=RENDER_WIDTH)
    documents = payload if isinstance(payload, list) else [payload]
    for document in documents:
        render(console, document)
    return 0


if __name__ == "__main__":
    sys.exit(main())

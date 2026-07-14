#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import json
import os
import sys

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.text import Text

THEME = "gruvbox-dark"
BORDER_STYLE = "bold #2f7fd1"
MARGIN = 4
RENDER_WIDTH = 1024


def render(console: Console, document: dict) -> None:
    title = document.get("title", "")
    content = document.get("content", "")
    content_width = max(
        (Text(line).cell_len for line in content.splitlines()), default=0
    )
    title_width = Text(title).cell_len + MARGIN
    width = min(console.width, max(content_width, title_width) + MARGIN)
    console.print(
        Panel(
            Syntax(content, "json", theme=THEME, background_color="default"),
            title=title,
            title_align="left",
            border_style=BORDER_STYLE,
            expand=False,
            width=width,
            padding=(0, 1),
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render a JSON payload of documents with syntax highlighting."
    )
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

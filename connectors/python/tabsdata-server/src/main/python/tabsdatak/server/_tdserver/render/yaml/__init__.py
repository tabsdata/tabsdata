#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import sys

from rich.console import Console
from rich.rule import Rule
from rich.syntax import Syntax

THEME = "gruvbox-dark"
DELIMITER_STYLE = "bold #2f7fd1"
SUBTITLE_STYLE = "not bold #8a8a8a"
DOCUMENT_SEPARATION = 2


def render(console: Console, document: dict) -> None:
    title = document.get("title", "")
    subtitle = document.get("subtitle")
    content = document.get("content", "").rstrip()
    if subtitle:
        title = f"{title} [{SUBTITLE_STYLE}]({subtitle})[/{SUBTITLE_STYLE}]"
    console.print(Rule(title=title, style=DELIMITER_STYLE, align="left"))
    console.print()
    console.print(Syntax(content, "yaml", theme=THEME, background_color="default"))
    console.print(Rule(style=DELIMITER_STYLE))


def main() -> int:
    payload = json.load(sys.stdin)
    console = Console()
    documents = payload if isinstance(payload, list) else [payload]
    for position, document in enumerate(documents):
        if position:
            console.print("\n" * DOCUMENT_SEPARATION, end="")
        render(console, document)
    return 0


if __name__ == "__main__":
    sys.exit(main())

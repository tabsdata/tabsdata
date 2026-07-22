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


def render(console: Console, document: dict) -> None:
    title = document.get("title", "")
    content = document.get("content", "")
    console.print(Rule(title=title, style=DELIMITER_STYLE, align="left"))
    console.print(Syntax(content, "yaml", theme=THEME, background_color="default"))
    console.print(Rule(style=DELIMITER_STYLE))


def main() -> int:
    payload = json.load(sys.stdin)
    console = Console()
    documents = payload if isinstance(payload, list) else [payload]
    for document in documents:
        render(console, document)
    return 0


if __name__ == "__main__":
    sys.exit(main())

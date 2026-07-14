#
# Copyright 2026 Tabs Data Inc.
#

import rich_click as click
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

DEFAULT_COLOR = "dark_orange"
RENDER_WIDTH = 1024


@click.command(name="banner")
@click.argument("message")
@click.option("--color", default=DEFAULT_COLOR)
def banner(message: str, color: str):
    """Print a highly visible boxed banner."""
    console = Console(width=RENDER_WIDTH)
    text = Text(message, justify="left")
    width = min(console.width, text.cell_len + 4)
    console.print(
        Panel(
            text,
            border_style=color,
            expand=False,
            width=width,
            padding=(0, 1),
        )
    )

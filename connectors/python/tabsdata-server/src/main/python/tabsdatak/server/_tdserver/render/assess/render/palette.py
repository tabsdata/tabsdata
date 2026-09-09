#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys

from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

from tabsdatak.server._tdserver.render.assess import (
    COMMAND_STYLE,
    DELIMITER_STYLE,
    DETAIL_STYLE,
    DOCS_STYLE,
    HELP_STYLE,
    LABEL_STYLE,
    LINK_STYLE,
    PENDING_LABEL_STYLE,
    State,
)

SAMPLE_CHECK = "Definition match"

ELEMENTS = (
    ("Panel border and concern title", DELIMITER_STYLE, "Local Instance"),
    ("Timing, gutter and skip reason", DETAIL_STYLE, "0.02s"),
    ("A command to run", COMMAND_STYLE, "tdkserver init"),
    ("Help prefix", HELP_STYLE, "Help:"),
    ("Docs prefix", DOCS_STYLE, "Docs:"),
    ("Docs link", LINK_STYLE, "https://docs.tabsdata.com"),
    ("Label of a finished check", LABEL_STYLE, SAMPLE_CHECK),
    ("Label of a check not yet run", PENDING_LABEL_STYLE, SAMPLE_CHECK),
)


def label_style_of(state: State) -> str:
    weight = (
        PENDING_LABEL_STYLE if state in (State.WAIVED, State.PENDING) else LABEL_STYLE
    )
    return f"{weight} {state.style}"


def states_table() -> Table:
    table = Table(
        box=box.SQUARE,
        border_style=DELIMITER_STYLE,
        title="Check states",
        title_style="bold",
        title_justify="left",
        pad_edge=False,
    )
    table.add_column("State", no_wrap=True)
    table.add_column("Mark", justify="center", no_wrap=True)
    table.add_column("Colour", no_wrap=True)
    table.add_column("As rendered", no_wrap=True)
    table.add_column("Blocks", justify="center", no_wrap=True)
    table.add_column("Meaning", overflow="fold")
    for state in State:
        table.add_row(
            state.caption,
            Text(state.mark, style=state.style),
            Text(state.style, style=DETAIL_STYLE),
            Text(f"{state.mark} {SAMPLE_CHECK}", style=label_style_of(state)),
            Text("yes" if state.blocking else "", style=State.FAILED.style),
            Text(state.meaning, style=DETAIL_STYLE),
        )
    return table


def elements_table() -> Table:
    table = Table(
        box=box.SQUARE,
        border_style=DELIMITER_STYLE,
        title="Other elements",
        title_style="bold",
        title_justify="left",
        pad_edge=False,
    )
    table.add_column("Element", no_wrap=True)
    table.add_column("Style", no_wrap=True)
    table.add_column("As rendered", no_wrap=True)
    for element, style, sample in ELEMENTS:
        table.add_row(
            element,
            Text(style, style=DETAIL_STYLE),
            Text(sample, style=style),
        )
    return table


def main() -> int:
    console = Console()
    console.print()
    console.print(states_table())
    console.print()
    console.print(elements_table())
    console.print()
    console.print(
        Text(
            "Every colour is an explicit hex, so the palette renders the same in "
            "every terminal.",
            style=DETAIL_STYLE,
        )
    )
    console.print()
    return 0


if __name__ == "__main__":
    sys.exit(main())

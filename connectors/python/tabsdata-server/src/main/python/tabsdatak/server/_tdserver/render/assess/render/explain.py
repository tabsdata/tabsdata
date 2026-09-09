#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Sequence

from rich import box
from rich.console import Group, RenderableType
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from tabsdatak.server._tdserver.render.assess import (
    DELIMITER_STYLE,
    DETAIL_STYLE,
    LABEL_STYLE,
    titled,
)
from tabsdatak.server._tdserver.render.assess.core.provider import provider_of
from tabsdatak.server._tdserver.render.assess.core.schedule import scheduled
from tabsdatak.server._tdserver.render.assess.core.task import Target, Task

TITLE = "What each check asks"

TIER_NOTE = {
    "local": "Read on this workstation",
    "cluster": "Asked of the cluster",
    "remote": "Asked of a service the pods use",
}


def concerns_of(tasks: Sequence[Task]) -> dict[str, list[Task]]:
    grouped: dict[str, list[Task]] = {}
    for task in tasks:
        grouped.setdefault(task.concern, []).append(task)
    return grouped


def explanation(tasks: Sequence[Task], target: Target) -> Panel:
    planned = scheduled(tasks, provider_of(target.provider))
    parts: list[RenderableType] = []

    for concern, group in concerns_of(planned).items():
        if parts:
            parts.append(Text())
        heading = Text()
        heading.append(titled(concern), style=f"{LABEL_STYLE} {DELIMITER_STYLE}")
        heading.append(
            f"  {TIER_NOTE.get(group[0].tier.value, '')}", style=DETAIL_STYLE
        )
        parts.append(heading)

        table = Table.grid(padding=(0, 2))
        table.add_column(no_wrap=True, width=22)
        table.add_column(overflow="fold")
        for task in group:
            body = Text(task.asserts or "")
            particulars = task.particulars
            if particulars:
                body.append(f"\n{particulars}", style=DETAIL_STYLE)
            table.add_row(Text(task.check, style=LABEL_STYLE), body)
        parts.append(table)

    return Panel(
        Group(*parts),
        title=titled(TITLE),
        title_align="left",
        border_style=DELIMITER_STYLE,
        box=box.SQUARE,
        padding=(1, 2),
    )

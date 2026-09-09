#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from tabsdatak.server._tdserver.render.assess.core.provider import Provider
from tabsdatak.server._tdserver.render.assess.core.task import Task


def scheduled(tasks: Sequence[Task], provider: Provider | None) -> list[Task]:
    applicable = [task for task in tasks if task.applies_to(provider)]
    known = {task.key for task in applicable}

    planned: list[Task] = []
    gates: list[str] = []
    for task in applicable:
        needs: list[str] = []
        for need in (*task.needs, *gates):
            if need in known and need != task.key and need not in needs:
                needs.append(need)
        planned.append(replace(task, needs=tuple(needs)))
        if task.gate:
            gates.append(task.key)
    return planned

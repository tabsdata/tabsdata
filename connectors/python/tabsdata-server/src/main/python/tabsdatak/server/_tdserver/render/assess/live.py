#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from rich import box
from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, ProgressColumn, SpinnerColumn
from rich.table import Column
from rich.text import Text

from tabsdatak.server._tdserver.render.assess import (
    DELIMITER_STYLE,
    DETAIL_STYLE,
    LABEL_STYLE,
    PENDING_LABEL_STYLE,
    Check,
    State,
    titled,
)
from tabsdatak.server._tdserver.render.assess.task import (
    Target,
    Task,
    TaskError,
    failed,
    skipped,
)

MINIMUM_WIDTH = 60
MAXIMUM_WIDTH = 128

LABEL_WIDTH = 26
ASIDE_WIDTH = 8
FIXED_WIDTH = 1 + (2 * 4) + 4

REFRESH_PER_SECOND = 12
WAIT_SECONDS = 0.05


def console_width() -> int:
    return max(min(Console().width, MAXIMUM_WIDTH), MINIMUM_WIDTH)


def reason_width(total: int) -> int:
    return max(total - FIXED_WIDTH - LABEL_WIDTH - ASIDE_WIDTH, 12)


def label_style_of(state: State) -> str:
    weight = (
        PENDING_LABEL_STYLE if state in (State.WAIVED, State.PENDING) else LABEL_STYLE
    )
    return f"{weight} {state.style}"


class Row:
    def __init__(self, task: Task) -> None:
        self.task = task
        self.state = State.PENDING
        self.reason: str | None = None
        self.outcome: Check | None = None


class StateColumn(ProgressColumn):
    def __init__(self) -> None:
        super().__init__(table_column=Column(width=1, no_wrap=True))
        self.spinner = SpinnerColumn(style=State.RUNNING.style)

    def render(self, task) -> RenderableType:
        row: Row = task.fields["row"]
        if row.state is State.RUNNING:
            return self.spinner.render(task)
        return Text(row.state.mark, style=row.state.style)


class LabelColumn(ProgressColumn):
    def __init__(self) -> None:
        super().__init__(
            table_column=Column(width=LABEL_WIDTH, no_wrap=True, overflow="ellipsis")
        )

    def render(self, task) -> RenderableType:
        row: Row = task.fields["row"]
        return Text(row.task.check, style=label_style_of(row.state))


class ReasonColumn(ProgressColumn):
    def __init__(self, width: int) -> None:
        super().__init__(
            table_column=Column(width=width, no_wrap=True, overflow="ellipsis")
        )

    def render(self, task) -> RenderableType:
        row: Row = task.fields["row"]
        if row.state not in (State.SKIPPED, State.WAIVED):
            return Text("")
        return Text(row.reason or row.state.caption, style=row.state.style)


class AsideColumn(ProgressColumn):
    def __init__(self) -> None:
        super().__init__(
            table_column=Column(width=ASIDE_WIDTH, no_wrap=True, justify="right")
        )

    def render(self, task) -> RenderableType:
        row: Row = task.fields["row"]
        if row.state in (State.PENDING, State.SKIPPED, State.WAIVED):
            return Text("")
        elapsed = task.finished_time if task.finished else task.elapsed
        return Text(f"{elapsed:.2f}s" if elapsed else "", style=DETAIL_STYLE)


def concern_panel(concern: str, progress: Progress) -> Panel:
    return Panel(
        progress,
        title=titled(concern),
        title_align="left",
        border_style=DELIMITER_STYLE,
        box=box.SQUARE,
        padding=(0, 1),
    )


def assess(
    tasks: Sequence[Task], target: Target, console: Console
) -> tuple[list[Check], float]:
    width = console.width
    rows = {task.key: Row(task) for task in tasks}

    concerns: dict[str, list[Task]] = {}
    for task in tasks:
        concerns.setdefault(task.concern, []).append(task)

    progresses: dict[str, Progress] = {}
    identifiers: dict[str, tuple[Progress, int]] = {}
    for concern, group in concerns.items():
        progress = Progress(
            StateColumn(),
            LabelColumn(),
            ReasonColumn(reason_width(width)),
            AsideColumn(),
            console=console,
        )
        for task in group:
            identifier = progress.add_task("", start=False, total=1, row=rows[task.key])
            identifiers[task.key] = (progress, identifier)
        progresses[concern] = progress

    panels = Group(*(concern_panel(name, progresses[name]) for name in concerns))

    settled: dict[str, State] = {}
    guard = Lock()

    def run(task: Task) -> Check:
        row = rows[task.key]
        progress, identifier = identifiers[task.key]

        while True:
            with guard:
                pending = [need for need in task.needs if need not in settled]
            if not pending:
                break
            time.sleep(WAIT_SECONDS)

        with guard:
            unmet = [
                rows[need].task.check
                for need in task.needs
                if settled[need] is not State.PASSED
            ]
        if unmet:
            outcome = skipped(task, unmet)
            row.state = outcome.state
            row.reason = outcome.reason
            row.outcome = outcome
            with guard:
                settled[task.key] = row.state
            return outcome

        row.state = State.RUNNING
        progress.start_task(identifier)
        try:
            outcome = task.run(target)
        except TaskError as error:
            outcome = failed(task, str(error))
        row.state = outcome.state
        row.reason = outcome.reason
        row.outcome = outcome
        progress.update(identifier, completed=1)
        progress.stop_task(identifier)
        with guard:
            settled[task.key] = row.state
        return outcome

    started = time.monotonic()
    with Live(panels, console=console, refresh_per_second=REFRESH_PER_SECOND):
        with ThreadPoolExecutor(max_workers=max(len(tasks), 1)) as pool:
            outcomes = list(pool.map(run, tasks))
    return outcomes, time.monotonic() - started

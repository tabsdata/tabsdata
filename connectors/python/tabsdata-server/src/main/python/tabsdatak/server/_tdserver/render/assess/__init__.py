#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
from enum import Enum

from pydantic import BaseModel, Field
from rich import box
from rich.console import Console, Group, RenderableType
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

DELIMITER_STYLE = "bold #2f7fd1"
DETAIL_STYLE = "not bold #8a8a8a"
LABEL_STYLE = "bold"
PENDING_LABEL_STYLE = "not bold"
COMMAND_STYLE = "bold #d7875f"
HELP_STYLE = "bold #22a5c4"
DOCS_STYLE = "bold #4c8fd4"
LINK_STYLE = "underline #4c8fd4"

MINOR_WORDS = frozenset({
    "a",
    "an",
    "and",
    "as",
    "at",
    "but",
    "by",
    "for",
    "in",
    "nor",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
})


def titled(text: str) -> str:
    words = text.split()
    return " ".join(
        word if index and word.lower() in MINOR_WORDS else word[:1].upper() + word[1:]
        for index, word in enumerate(words)
    )


class State(Enum):
    PASSED = ("ok", "\u2714", "#2ea043", "Passed")
    FAILED = ("ko", "\u2718", "#d1495b", "Failed")
    WARNING = ("warn", "\u25b2", "#d97706", "Warning")
    SKIPPED = ("skip", "\u2298", "#d4c53a", "Skipped")
    WAIVED = ("waive", "\u25a1", "#8a8a8a", "Waived")
    RUNNING = ("running", "\u2192", "#22a5c4", "Running")
    PENDING = ("pending", "\u25cb", "#8f7fd1", "Pending")

    def __init__(self, code: str, mark: str, style: str, caption: str) -> None:
        self._value_ = code
        self.mark = mark
        self.style = style
        self.caption = caption

    @property
    def failing(self) -> bool:
        """Whether the check earns an explanation panel of its own."""
        return self in (State.FAILED, State.WARNING, State.SKIPPED)

    @property
    def blocking(self) -> bool:
        """Whether the check stands in the way of creating the instance."""
        return self in (State.FAILED, State.SKIPPED)

    @property
    def meaning(self) -> str:
        return MEANINGS[self]

    def tally(self, count: int) -> str:
        caption = self.caption.lower()
        if self is State.WARNING:
            return caption if count == 1 else f"{caption}s"
        return caption


MEANINGS: dict[State, str] = {
    State.PASSED: "The check found nothing wrong",
    State.FAILED: "The check found a problem that stops the instance",
    State.WARNING: "The check passed, with something worth reading",
    State.SKIPPED: "The check could not run, so the outcome is unknown",
    State.WAIVED: "The check does not apply to this instance",
    State.RUNNING: "The check is still running",
    State.PENDING: "The check has not started",
}


def legend() -> Text:
    line = Text()
    for state in State:
        if line:
            line.append("   ")
        line.append(state.mark, style=state.style)
        line.append(f" {state.caption}", style=f"{LABEL_STYLE} {state.style}")
    return line


class Help(BaseModel):
    text: str
    command: str | None = None


class Span(BaseModel):
    line: int
    column: int
    length: int
    label: str | None = None


class Source(BaseModel):
    label: str | None = None
    first: int = 1
    lines: list[str] = Field(default_factory=list)


class Check(BaseModel):
    concern: str
    check: str
    state: State
    seconds: float | None = None
    code: str | None = None
    title: str | None = None
    facts: list[str] = Field(default_factory=list)
    help: list[Help] = Field(default_factory=list)
    url: str | None = None
    source: Source | None = None
    span: Span | None = None
    reason: str | None = None

    @property
    def elapsed(self) -> str:
        return f"{self.seconds:.2f}s" if self.seconds is not None else ""

    @property
    def headline(self) -> str:
        return self.title or self.check

    @property
    def label_style(self) -> str:
        if self.state in (State.WAIVED, State.PENDING):
            return f"{PENDING_LABEL_STYLE} {self.state.style}"
        return f"{LABEL_STYLE} {self.state.style}"

    @property
    def aside(self) -> Text:
        if self.state is State.SKIPPED:
            return Text(self.title or "Skipped", style=self.state.style)
        return Text(self.elapsed, style=DETAIL_STYLE)


class Report(BaseModel):
    instance: str | None = None
    seconds: float | None = None
    checks: list[Check] = Field(default_factory=list)

    @property
    def concerns(self) -> dict[str, list[Check]]:
        grouped: dict[str, list[Check]] = {}
        for check in self.checks:
            grouped.setdefault(check.concern, []).append(check)
        return grouped

    @property
    def failing(self) -> list[Check]:
        return [check for check in self.checks if check.state.failing]


def concern_panel(concern: str, checks: list[Check]) -> Panel:
    table = Table.grid(padding=(0, 1), expand=True)
    table.add_column(width=1)
    table.add_column(ratio=1, overflow="fold")
    table.add_column(justify="right", no_wrap=True)
    for check in checks:
        table.add_row(
            Text(check.state.mark, style=check.state.style),
            Text(check.check, style=check.label_style),
            check.aside,
        )
    return Panel(
        table,
        title=titled(concern),
        title_align="left",
        border_style=DELIMITER_STYLE,
        box=box.SQUARE,
        padding=(0, 1),
    )


def snippet(source: Source, span: Span | None) -> Text:
    body = Text()
    if source.label:
        body.append(f"╭─[ {source.label} ]\n", style=DETAIL_STYLE)
    width = len(str(source.first + len(source.lines) - 1))
    for offset, raw in enumerate(source.lines):
        number = source.first + offset
        body.append(f"{number:>{width}} │ ", style=DETAIL_STYLE)
        body.append(f"{raw}\n")
        if span and number == span.line:
            body.append(f"{'':>{width}} │ ", style=DETAIL_STYLE)
            body.append(" " * span.column)
            body.append("─" * span.length, style=State.FAILED.style)
            if span.label:
                body.append(f" {span.label}", style=State.FAILED.style)
            body.append("\n")
    if source.label:
        body.append("╰────", style=DETAIL_STYLE)
    return body


def diagnostic(check: Check) -> Panel:
    head = Text()
    head.append(f"{check.state.mark} ", style=f"bold {check.state.style}")
    head.append(check.headline, style="bold")

    parts: list[RenderableType] = [head]

    if check.source:
        parts.extend([Text(), snippet(check.source, check.span)])

    if check.facts:
        table = Table.grid(padding=(0, 1))
        table.add_column(width=1)
        table.add_column(overflow="fold")
        for fact in check.facts:
            table.add_row(Text("•", style=check.state.style), Text(fact))
        parts.extend([Text(), table])

    if check.help:
        parts.append(Text())
        for entry in check.help:
            line = Text()
            line.append("Help: ", style=HELP_STYLE)
            line.append(entry.text)
            if entry.command:
                line.append(" ")
                line.append(entry.command, style=COMMAND_STYLE)
            parts.append(line)

    if check.url:
        line = Text()
        line.append("Docs: ", style=DOCS_STYLE)
        line.append(check.url, style=LINK_STYLE)
        parts.append(line)

    return Panel(
        Group(*parts),
        border_style=check.state.style,
        box=box.SQUARE,
        padding=(1, 2),
    )


def summary(checks: list[Check], seconds: float | None = None) -> Table:
    counts = {state: 0 for state in State}
    for check in checks:
        counts[check.state] += 1

    table = Table.grid(padding=(0, 1))
    table.add_column(justify="right", style=DETAIL_STYLE, no_wrap=True)
    table.add_column(overflow="fold")

    if sum(counts[state] for state in State if state.blocking):
        outcome = State.FAILED if counts[State.FAILED] else State.SKIPPED
        verdict = "The instance cannot be created yet"
    elif counts[State.WARNING]:
        outcome = State.WARNING
        verdict = "The instance can be created, with caveats"
    else:
        outcome = State.PASSED
        verdict = "The instance can be created"

    table.add_row("Verdict:", Text(verdict, style=f"{LABEL_STYLE} {outcome.style}"))
    if seconds is not None:
        table.add_row("Total time:", Text(f"{seconds:.2f}s", style=LABEL_STYLE))
    table.add_row("Total checks:", Text(str(len(checks)), style=LABEL_STYLE))

    tally = Text()
    for state in (
        State.PASSED,
        State.FAILED,
        State.WARNING,
        State.SKIPPED,
        State.WAIVED,
    ):
        if tally:
            tally.append("  -  ", style=DETAIL_STYLE)
        count = counts[state]
        tally.append(f"{state.caption}: ", style=DETAIL_STYLE)
        tally.append(
            str(count), style=f"{LABEL_STYLE} {state.style if count else DETAIL_STYLE}"
        )
    table.add_row("Outcomes:", tally)
    return table


def render(report: Report, console: Console | None = None) -> None:
    console = console or Console()

    console.print()
    console.print(legend())
    console.print()
    for concern, checks in report.concerns.items():
        console.print(concern_panel(concern, checks))

    for check in report.failing:
        console.print()
        console.print(diagnostic(check))

    console.print()
    console.print(summary(report.checks, report.seconds))
    console.print()


def epilogue(report: Report, console: Console) -> None:
    for check in report.failing:
        console.print()
        console.print(diagnostic(check))
    console.print()
    console.print(summary(report.checks, report.seconds))
    console.print()


def main() -> int:
    from tabsdatak.server._tdserver.render.assess.command import command

    return command()


if __name__ == "__main__":
    sys.exit(main())

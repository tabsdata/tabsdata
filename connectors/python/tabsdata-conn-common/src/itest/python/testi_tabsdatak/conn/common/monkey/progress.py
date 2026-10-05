#
# Copyright 2026 Tabsdata Inc.
#

"""Live progress of a monkey run, off unless asked for. Test output is captured,
and under xdist even ``-s`` cannot show it, so progress goes around pytest:
to the controlling terminal (``tty``), in muted colors, or appended to a file,
plain. When the target cannot be opened, as on a runner without a terminal,
progress stays off.

Every line shares one layout: wall time and time since the run started, then a
tag (or, for an operation, its position in the plan), then the message;
operations spread their fields over aligned columns. A message can carry a
concern, which colors it: good, waiting or bad."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterable
from typing import TextIO

from testi_tabsdatak.conn.common.monkey.model import ERROR, OK, REJECTED, SKIPPED
from testi_tabsdatak.conn.common.monkey.plan import (
    CANCEL_EXECUTION,
    CANCEL_TRANSACTION,
    DELETE,
    DISABLE,
    ENABLE,
    LOSE_JOB,
    LOSE_STREAMER,
    ORPHAN,
    PAUSE,
    RECOVER_EXECUTION,
    RECOVER_TRANSACTION,
    REGISTER,
    TRIGGER,
    UPDATE,
)

TTY = "tty"
TTY_DEVICE = "/dev/tty"

SETUP = "setup"
RUN = "run"
SETTLE = "settle"
OVERLAP = "overlap"
PROBLEM = "problem"
PASSED = "passed"
FAILED = "failed"

GOOD = "good"
WAITING = "waiting"
BAD = "bad"

ACTIONS = (
    REGISTER,
    DELETE,
    ENABLE,
    DISABLE,
    UPDATE,
    TRIGGER,
    CANCEL_EXECUTION,
    CANCEL_TRANSACTION,
    RECOVER_EXECUTION,
    RECOVER_TRANSACTION,
    LOSE_JOB,
    LOSE_STREAMER,
    ORPHAN,
    PAUSE,
)
ACTION_WIDTH = max(len(action) for action in ACTIONS)
RESULT_WIDTH = max(len(result) for result in (OK, REJECTED, SKIPPED, ERROR))
STATUS_WIDTH = 3
ELAPSED_WIDTH = 7
DETAIL_LENGTH = 160

RESET = "\033[0m"
GREY = "38;5;244"
DIM = "38;5;240"
STEEL = "38;5;67"
TEAL = "38;5;73"
SAND = "38;5;180"
SAGE = "38;5;108"
OCHRE = "38;5;179"
CLAY = "38;5;167"
MAUVE = "38;5;139"
SKY = "38;5;110"

TAG_COLORS = {
    SETUP: SKY,
    RUN: STEEL,
    SETTLE: MAUVE,
    OVERLAP: CLAY,
    PROBLEM: CLAY,
    PASSED: SAGE,
    FAILED: CLAY,
}
RESULT_COLORS = {OK: SAGE, REJECTED: OCHRE, SKIPPED: GREY, ERROR: CLAY}
CONCERN_COLORS = {GOOD: SAGE, WAITING: OCHRE, BAD: CLAY}


def sentence(text: str) -> str:
    return text[:1].upper() + text[1:]


def counted(number: int, noun: str) -> str:
    return f"{number} {noun}" if number == 1 else f"{number} {noun}s"


def spoken(seconds: float) -> str:
    total = round(seconds)
    hours, rest = divmod(total, 3600)
    minutes, seconds = divmod(rest, 60)
    if hours:
        return f"{hours} h {minutes:02d} min"
    if minutes:
        return f"{minutes} min {seconds:02d} s"
    return f"{seconds} s"


class Progress:
    def __init__(self, stream: TextIO | None = None, colored: bool = False):
        self._stream = stream
        self._colored = colored
        self._lock = threading.Lock()
        self._started = time.monotonic()
        self._written = False
        self._total_width = 4
        self._name_width = 8

    @staticmethod
    def open(target: str | None) -> Progress:
        if not target:
            return Progress()
        try:
            stream = open(TTY_DEVICE if target == TTY else target, "a", buffering=1)
        except OSError:
            return Progress()
        return Progress(stream, colored=target == TTY)

    def layout(self, total: int, names: Iterable[str]) -> None:
        self._total_width = len(str(total))
        self._name_width = max((len(name) for name in names), default=8)

    def elapsed(self) -> float:
        return time.monotonic() - self._started

    def __call__(
        self, message: str, tag: str = RUN, concern: str | None = None
    ) -> None:
        width = 2 * self._total_width + 3
        color = TAG_COLORS.get(tag, GREY)
        message = sentence(message)
        if concern is not None:
            body = self._paint(message, CONCERN_COLORS[concern])
        elif tag in (SETUP, RUN, SETTLE):
            body = message
        else:
            body = self._paint(message, color)
        self._write(f"{self._paint(tag.ljust(width), color)}  {body}")

    def table(
        self, header: tuple[str, ...], rows: list[tuple[str, ...]], tag: str = SETUP
    ) -> None:
        widths = [
            max(len(row[column]) for row in [header, *rows])
            for column in range(len(header))
        ]
        colors = [TEAL, None, DIM]

        def border(left: str, middle: str, right: str) -> str:
            line = middle.join("─" * (width + 2) for width in widths)
            return self._paint(f"{left}{line}{right}", DIM)

        def cell(value: str, width: int, color: str | None) -> str:
            text = value.ljust(width)
            return f" {self._paint(text, color) if color else text} "

        def cells(row: tuple[str, ...], paint: list[str | None]) -> str:
            bar = self._paint("│", DIM)
            parts = [cell(*column) for column in zip(row, widths, paint)]
            return bar + bar.join(parts) + bar

        tag_column = self._paint(tag.ljust(2 * self._total_width + 3), TAG_COLORS[tag])
        lines = [
            border("┌", "┬", "┐"),
            cells(header, [GREY] * len(header)),
            border("├", "┼", "┤"),
            *(cells(row, colors) for row in rows),
            border("└", "┴", "┘"),
        ]
        for line in lines:
            self._write(f"{tag_column}  {line}")

    def operation(
        self,
        done: int,
        total: int,
        lane: int,
        index: int,
        action: str,
        function: str | None,
        result: str,
        status: int | None,
        elapsed: float,
        detail: str,
    ) -> None:
        counter = f"[{done:>{self._total_width}}/{total:>{self._total_width}}]"
        color = RESULT_COLORS.get(result, GREY)
        columns = [
            self._paint(counter, STEEL),
            self._paint(f"lane {lane:<2}", DIM),
            self._paint(f"op {index:>{self._total_width}}", DIM),
            self._paint(action.ljust(ACTION_WIDTH), TEAL),
            self._paint((function or "-").ljust(self._name_width), SAND),
            self._paint(result.ljust(RESULT_WIDTH), color),
            self._paint(str(status or "").ljust(STATUS_WIDTH), color),
            self._paint(f"{elapsed:.2f}s".rjust(ELAPSED_WIDTH), GREY),
        ]
        if detail:
            columns.append(self._paint(sentence(detail[:DETAIL_LENGTH]), DIM))
        self._write("  ".join(columns))

    def close(self) -> None:
        if self._stream is not None:
            self._stream.close()
            self._stream = None

    def _paint(self, text: str, color: str) -> str:
        return f"\033[{color}m{text}{RESET}" if self._colored else text

    def _write(self, line: str) -> None:
        if self._stream is None:
            return
        hours, rest = divmod(int(self.elapsed()), 3600)
        minutes, seconds = divmod(rest, 60)
        stamp = self._paint(time.strftime("%H:%M:%S"), DIM)
        clock = self._paint(f"+{hours}:{minutes:02d}:{seconds:02d}", GREY)
        marker = self._paint("monkey", DIM)
        with self._lock:
            # On the terminal, start below whatever pytest left on its line.
            lead = "\n" if self._colored and not self._written else ""
            self._written = True
            try:
                self._stream.write(f"{lead}{stamp}  {clock}  {marker}  {line}\n")
            except OSError:
                self._stream = None

#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import sys
import threading
import time
from collections.abc import Generator

import pytest

PROGRESS_OPTION = "--tdx-progress"
PROGRESS_DEST = "tdx_progress"
PLUGIN_NAME = "tdx-progress-reporter"

TICK_SECONDS = 1.0
LOG_SECONDS = 30.0

ERASE_TO_END = "\x1b[K"
CLEAR_LINE = f"\r{ERASE_TO_END}"
SEPARATOR = " | "

ALWAYS_SHOWN = ("passed", "failed")
OUTCOMES = (
    "passed",
    "failed",
    "error",
    "skipped",
    "xfailed",
    "xpassed",
    "warnings",
    "deselected",
    "subtests passed",
    "subtests failed",
    "subtests skipped",
    "rerun",
)
COLORS = {
    "passed": "green",
    "failed": "red",
    "error": "red",
    "subtests passed": "green",
    "subtests failed": "red",
    "running": "cyan",
}
DEFAULT_COLOR = "yellow"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.getgroup("terminal reporting").addoption(
        PROGRESS_OPTION,
        dest=PROGRESS_DEST,
        action="store_true",
        default=False,
        help="Replace the progress dots with one live line of counters "
        "(tests, done, running and each outcome) and print each failure "
        "as it happens.",
    )


@pytest.hookimpl(trylast=True)
def pytest_configure(config: pytest.Config) -> None:
    if not config.getoption(PROGRESS_DEST) or hasattr(config, "workerinput"):
        return
    if config.get_verbosity() > 0 or config.getoption("collectonly"):
        return
    if config.pluginmanager.getplugin("terminalreporter") is None:
        return
    config.pluginmanager.register(ProgressReporter(config), PLUGIN_NAME)


def escapes_supported() -> bool:
    if sys.platform != "win32":
        return True
    # Windows Terminal and pwsh 7 interpret escape sequences only in a console
    # with virtual terminal processing; a legacy console without it would
    # print them, so the line is then logged instead of redrawn.
    import ctypes
    from ctypes import wintypes

    enable_virtual_terminal_processing = 0x0004
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    handle = kernel32.GetStdHandle(wintypes.DWORD(-11 & 0xFFFFFFFF))
    mode = wintypes.DWORD()
    if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
        return False
    if mode.value & enable_virtual_terminal_processing:
        return True
    return bool(
        kernel32.SetConsoleMode(handle, mode.value | enable_virtual_terminal_processing)
    )


def format_seconds(seconds: float) -> str:
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


class ProgressReporter:
    def __init__(self, config: pytest.Config) -> None:
        self._config = config
        self._reporter = None
        self._writer = None
        self._live = False
        self._instafail = bool(config.getoption("instafail", False))
        self._session: pytest.Session | None = None
        self._lock = threading.RLock()
        self._started = 0
        self._finished = 0
        self._begin = time.monotonic()
        self._last_log = 0.0
        self._last_draw = 0.0
        self._shown = False
        self._running = False
        self._ticker: threading.Thread | None = None
        self._stop = threading.Event()

    def pytest_sessionstart(self, session: pytest.Session) -> None:
        # Resolved here, not at configure: instafail swaps in a reporter of its
        # own from its pytest_configure.
        reporter = self._config.pluginmanager.getplugin("terminalreporter")
        reporter.showfspath = False
        reporter._show_progress_info = False
        self._reporter = reporter
        self._writer = reporter._tw
        self._live = bool(reporter.isatty()) and escapes_supported()
        self._session = session

    @pytest.hookimpl(wrapper=True)
    def pytest_report_teststatus(
        self, report: pytest.TestReport, config: pytest.Config
    ) -> Generator[None, object, object]:
        status = yield
        if not isinstance(status, tuple) or len(status) != 3:
            return status
        category, _, word = status
        return category, "", word

    @pytest.hookimpl(wrapper=True)
    def pytest_runtestloop(
        self, session: pytest.Session
    ) -> Generator[None, object, object]:
        self._begin = time.monotonic()
        self._running = True
        if self._live:
            self._ticker = threading.Thread(
                target=self._tick, name="tdx-progress", daemon=True
            )
            self._ticker.start()
        try:
            return (yield)
        finally:
            self._stop.set()
            if self._ticker is not None:
                self._ticker.join(timeout=2 * TICK_SECONDS)
            with self._lock:
                self._running = False
                self._clear()
                if self._started:
                    self._writer.write(self._line(final=True))
                    self._writer.line()

    def pytest_runtest_logstart(self, nodeid: str, location: tuple) -> None:
        with self._lock:
            self._started += 1
            self._draw()

    def pytest_runtest_logfinish(self, nodeid: str, location: tuple) -> None:
        with self._lock:
            self._finished += 1
            self._draw()

    @pytest.hookimpl(wrapper=True)
    def pytest_runtest_logreport(
        self, report: pytest.TestReport
    ) -> Generator[None, object, object]:
        failed = report.failed and not hasattr(report, "wasxfail")
        with self._lock:
            if failed:
                self._clear()
            try:
                return (yield)
            finally:
                if failed:
                    if not self._instafail:
                        self._failure(report)
                    self._draw(force=True)
                else:
                    self._draw()

    def _tick(self) -> None:
        while not self._stop.wait(TICK_SECONDS):
            with self._lock:
                self._draw()

    def _failure(self, report: pytest.TestReport) -> None:
        word = "FAILED" if report.when == "call" else "ERROR"
        line = f"{word} {report.nodeid}"
        crash = getattr(report.longrepr, "reprcrash", None)
        message = getattr(crash, "message", None)
        if message:
            line = f"{line} - {message.splitlines()[0]}"
        if self._live:
            line = line[: max(1, self._writer.fullwidth - 1)]
        self._writer.line(line, red=True)

    def _clear(self) -> None:
        if self._live and self._shown:
            self._writer.write(CLEAR_LINE, flush=True)
            self._shown = False

    def _draw(self, force: bool = False) -> None:
        if not self._running or not self._started:
            return
        now = time.monotonic()
        if self._live:
            # At most once per second, and in one write that overwrites the
            # line and then erases what is left of it, so it never blinks.
            if not force and self._shown and now - self._last_draw < TICK_SECONDS:
                return
            self._last_draw = now
            self._writer.write(f"\r{self._line()}{ERASE_TO_END}", flush=True)
            self._shown = True
            return
        if now - self._last_log >= LOG_SECONDS:
            self._last_log = now
            self._writer.write(self._line())
            self._writer.line()

    def _counts(self) -> dict[str, int]:
        counts = {}
        for key in OUTCOMES:
            reports = self._reporter.stats.get(key, [])
            counts[key] = sum(
                1
                for report in reports
                if getattr(report, "count_towards_summary", True)
            )
        return counts

    def _segments(self, final: bool) -> list[tuple[str, str | None, bool]]:
        total = self._session.testscollected if self._session else 0
        done = self._finished
        running = 0 if final else max(0, self._started - self._finished)
        elapsed = time.monotonic() - self._begin
        percent = done * 100 // total if total else 100
        segments: list[tuple[str, str | None, bool]] = [
            (f"tests {total}", None, True),
            (f"done {done} ({percent}%)", None, True),
            (f"running {running}", COLORS["running"], True),
        ]
        for key, count in self._counts().items():
            if count or key in ALWAYS_SHOWN:
                segments.append((
                    f"{key} {count}",
                    COLORS.get(key, DEFAULT_COLOR),
                    True,
                ))
        segments.append((format_seconds(elapsed), None, False))
        if done and total > done and not final:
            remaining = elapsed / done * (total - done)
            segments.append((f"eta {format_seconds(remaining)}", None, False))
        return segments

    def _line(self, final: bool = False) -> str:
        segments = self._segments(final)
        if self._live:
            width = self._writer.fullwidth - 1

            def too_wide() -> bool:
                plain = SEPARATOR.join(text for text, _, _ in segments)
                return len(plain) > width and len(segments) > 1

            while too_wide() and any(not kept for _, _, kept in segments):
                optional = max(i for i, (_, _, kept) in enumerate(segments) if not kept)
                del segments[optional]
            while too_wide():
                segments.pop()
        parts = []
        for text, color, _ in segments:
            parts.append(self._writer.markup(text, **{color: True}) if color else text)
        return SEPARATOR.join(parts)

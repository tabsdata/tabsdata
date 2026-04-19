#
# Copyright 2025 Tabs Data Inc.
#

import json
import os
import subprocess
import threading
import time
from dataclasses import dataclass, field

import rich_click as click
from rich.columns import Columns
from rich.console import Console, ConsoleRenderable, Group
from rich.live import Live
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text
from tenacity import retry, retry_if_result, stop_after_attempt, wait_fixed

from tabsdatax.pytest.plugins.harvester.internal.constants import (
    MONITOR_REFRESH_INTERVAL_SECONDS,
    PREFIX_TO_COUNTER,
    PREFIXES_FAILED,
    SUDO_REFRESH_INTERVAL_SECONDS,
    SUDO_REFRESH_TIMEOUT_SECONDS,
    TDX_HARVESTER_FOLDER_DEF,
    TDX_HARVESTER_FOLDER_ENV,
    TestFilePrefix,
)

console = Console()


@dataclass
class ScanCounts:
    collected: int | None = None
    passed: int = 0
    running: int = 0
    skipped: int = 0
    orphaned: int = 0
    error_setup: int = 0
    error_call: int = 0
    error_teardown: int = 0
    error_unknown: int = 0

    @property
    def total_failed(self) -> int:
        return (
            self.error_setup
            + self.error_call
            + self.error_teardown
            + self.error_unknown
        )

    @property
    def total_completed(self) -> int:
        return (
            self.passed
            + self.running
            + self.skipped
            + self.orphaned
            + self.total_failed
        )

    @property
    def pending(self) -> int | None:
        if self.collected is None:
            return None
        return max(0, self.collected - self.total_completed)


@dataclass
class StackFrame:
    function: str
    filename: str
    line: int

    @property
    def basename(self) -> str:
        return os.path.basename(self.filename)

    @property
    def location(self) -> str:
        return f"{self.basename}:{self.line}"


@dataclass
class SpyDump:
    frames: list[StackFrame] = field(default_factory=list)


@dataclass
class TestRecord:
    nodeid: str
    file: str
    line: int
    function: str
    status: str
    start: float
    worker: str
    pid: int
    duration_setup: float | None
    duration_call: float | None
    duration_teardown: float | None
    error: str | None
    error_type: str | None
    error_location: str | None
    json_path: str
    top_frame: StackFrame | None = None
    all_frames: list[StackFrame] = field(default_factory=list)
    stack_depth: int = 0

    @property
    def test_name(self) -> str:
        idx = self.nodeid.find("[")
        return self.nodeid[:idx] if idx != -1 else self.nodeid

    @property
    def test_params(self) -> str:
        idx = self.nodeid.find("[")
        if idx != -1 and self.nodeid.endswith("]"):
            raw = self.nodeid[idx + 1 : -1]
            return raw.replace("-", " - ")
        return ""

    @property
    def total_duration(self) -> float:
        return sum(
            duration
            for duration in (
                self.duration_setup,
                self.duration_call,
                self.duration_teardown,
            )
            if duration is not None
        )

    @property
    def elapsed(self) -> float:
        if self.status == "Running":
            return time.time() - self.start
        return self.total_duration

    @property
    def duration_breakdown(self) -> str:
        parts = []
        if self.duration_setup is not None:
            parts.append(f"s:{self.duration_setup:.1f}")
        if self.duration_call is not None:
            parts.append(f"c:{self.duration_call:.1f}")
        if self.duration_teardown is not None:
            parts.append(f"t:{self.duration_teardown:.1f}")
        return " ".join(parts)


def has_py_spy() -> bool:
    try:
        subprocess.run(
            ["py-spy", "--version"],
            capture_output=True,
            check=True,
        )
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def start_sudo_refresh(interval: int = SUDO_REFRESH_INTERVAL_SECONDS) -> None:
    def run():
        while True:
            time.sleep(interval)
            try:
                subprocess.run(
                    ["sudo", "-v"],
                    capture_output=True,
                    timeout=SUDO_REFRESH_TIMEOUT_SECONDS,
                )
            except (OSError, subprocess.TimeoutExpired):
                pass

    runner = threading.Thread(target=run, daemon=True)
    runner.start()


def pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def format_time(seconds: float | None) -> str:
    if seconds is None:
        return ""
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    return f"{s // 3600}h {(s % 3600) // 60}m {s % 60}s"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(0.3),
    retry=retry_if_result(lambda r: r is None),
    retry_error_callback=lambda _: None,
)
def run_py_spy(pid: int) -> SpyDump | None:
    try:
        result = subprocess.run(
            [
                "sudo",
                "py-spy",
                "dump",
                "--json",
                "--nonblocking",
                "--pid",
                str(pid),
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return None
        data = json.loads(result.stdout)
        thread_list = data if isinstance(data, list) else data.get("threads", [])

        def parse_frames(i_thread: dict) -> list[StackFrame]:
            return [
                StackFrame(
                    function=f.get("name", "?"),
                    filename=f.get("filename", "?"),
                    line=f.get("line", 0),
                )
                for f in i_thread.get("frames", [])
            ]

        # Priority 1: find the thread that contains a test_* frame
        for thread in thread_list:
            frames = parse_frames(thread)
            if any(f.function.startswith("test_") for f in frames):
                return SpyDump(frames=frames)

        # Priority 2: first active thread
        for thread in thread_list:
            if not thread.get("active", False):
                continue
            frames = parse_frames(thread)
            if frames:
                return SpyDump(frames=frames)

        # Priority 3: any thread with frames
        for thread in thread_list:
            frames = parse_frames(thread)
            if frames:
                return SpyDump(frames=frames)

        return SpyDump()
    except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError):
        return None


def read_record(path: str) -> TestRecord | None:
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    return TestRecord(
        nodeid=data.get("nodeid", "?"),
        file=data.get("file", "?"),
        line=data.get("line", 0),
        function=data.get("function", "?"),
        status=data.get("status", "?"),
        start=data.get("start", 0),
        worker=data.get("worker", ""),
        pid=data.get("pid", 0),
        duration_setup=data.get("duration_setup"),
        duration_call=data.get("duration_call"),
        duration_teardown=data.get("duration_teardown"),
        error=data.get("error"),
        error_type=data.get("error_type"),
        error_location=data.get("error_location"),
        json_path=path,
    )


def scan_monitor_folder(  # noqa: C901
    monitor_folder: str, use_py_spy: bool, full_trace: bool
) -> tuple[list[TestRecord], list[TestRecord], ScanCounts]:
    running: list[TestRecord] = []
    failed: list[TestRecord] = []
    counts = ScanCounts()

    if not os.path.isdir(monitor_folder):
        return running, failed, counts

    meta_path = os.path.join(monitor_folder, "_meta.json")
    try:
        with open(meta_path) as f:
            meta = json.load(f)
        counts.collected = meta.get("collected")
    except (OSError, json.JSONDecodeError):
        pass

    try:
        entries = os.listdir(monitor_folder)
    except OSError:
        return running, failed, counts

    for filename in entries:
        if not filename.endswith(".json") or filename.startswith("_"):
            continue

        if filename.startswith(TestFilePrefix.PASSED):
            counts.passed += 1
            continue

        if filename.startswith(TestFilePrefix.SKIPPED):
            counts.skipped += 1
            continue

        if filename.startswith(TestFilePrefix.RUNNING):
            path = os.path.join(monitor_folder, filename)
            record = read_record(path)
            if record and pid_exists(record.pid):
                running.append(record)
            else:
                counts.orphaned += 1
            continue

        for prefix in PREFIXES_FAILED:
            if filename.startswith(prefix):
                path = os.path.join(monitor_folder, filename)
                record = read_record(path)
                if record:
                    failed.append(record)
                counter = PREFIX_TO_COUNTER.get(prefix)
                if counter:
                    setattr(counts, counter, getattr(counts, counter) + 1)
                break

    counts.running = len(running)

    def worker_sort_key(test_record: TestRecord) -> tuple:
        test_worker = test_record.worker
        if test_worker.startswith("gw"):
            try:
                return 0, int(test_worker[2:])
            except ValueError:
                pass
        return (1, 0) if not test_worker else (0, 0)

    running.sort(key=worker_sort_key)

    if use_py_spy and running:
        for record in running:
            if record.pid <= 0:
                continue
            dump = run_py_spy(record.pid)
            if dump is None or not dump.frames:
                continue
            test_frame_idx = -1
            for i, frame in enumerate(dump.frames):
                if frame.function.startswith("test_"):
                    test_frame_idx = i
                    break
            if test_frame_idx >= 0:
                record.stack_depth = test_frame_idx
                # py-spy frames: [0]=deepest (IO), [N]=outermost (test_*)
                record.top_frame = dump.frames[test_frame_idx]
                if full_trace:
                    # From test_* down to blocking call (reversed)
                    up_to_test = dump.frames[: test_frame_idx + 1]
                    up_to_test.reverse()
                    record.all_frames = up_to_test
            elif dump.frames:
                record.top_frame = dump.frames[-1]
                record.stack_depth = len(dump.frames)
                if full_trace:
                    frames_reversed = list(reversed(dump.frames))
                    record.all_frames = frames_reversed

    failed.sort(key=lambda r: r.test_name)

    return running, failed, counts


# noinspection DuplicatedCode
def render_running_table(running: list[TestRecord], full_trace: bool) -> Table:
    table = Table(
        title=Text("Running", style="bold cyan"),
        show_header=True,
        expand=False,
        border_style="cyan",
        padding=(0, 1),
        show_lines=False,
    )
    table.add_column("#", style="dim", justify="right", no_wrap=True)
    table.add_column("Test", style="green bold", no_wrap=True)
    table.add_column("Params", style="yellow", no_wrap=True)
    table.add_column("Worker", style="dim", no_wrap=True)
    table.add_column("Elapsed", style="magenta", justify="right", no_wrap=True)
    table.add_column("Setup", style="dim", justify="right", no_wrap=True)
    table.add_column("Call", style="dim", justify="right", no_wrap=True)
    table.add_column("Teardown", style="dim", justify="right", no_wrap=True)
    table.add_column("Frame", no_wrap=True)
    table.add_column("Depth", style="dim", justify="right", no_wrap=True)

    for idx, test in enumerate(running, 1):
        frame_text = Text()
        if test.top_frame:
            frame_text.append(test.top_frame.function, style="white")
            frame_text.append(f"  {test.top_frame.location}", style="dim")
        else:
            frame_text.append("n/a", style="dim")

        # Infer current phase time from elapsed:
        # - No phases done yet → still in setup
        # - Setup done, call not → in call
        # - Setup + call done, teardown not → in teardown
        setup_display = test.duration_setup
        call_display = test.duration_call
        teardown_display = test.duration_teardown
        if setup_display is None:
            setup_display = test.elapsed
        elif call_display is None:
            inferred = test.elapsed - setup_display
            if inferred > 0:
                call_display = inferred
        elif teardown_display is None:
            inferred = test.elapsed - setup_display - call_display
            if inferred > 0:
                teardown_display = inferred

        table.add_row(
            str(idx),
            test.test_name,
            test.test_params,
            test.worker,
            format_time(test.elapsed),
            format_time(setup_display),
            format_time(call_display),
            format_time(teardown_display),
            frame_text,
            str(test.stack_depth) if test.top_frame is not None else "",
        )

        if full_trace and test.all_frames:
            trace_parts = []
            for i, frame in enumerate(test.all_frames):
                connector = (
                    "\u251c\u2500" if i < len(test.all_frames) - 1 else "\u2514\u2500"
                )
                trace_parts.append(
                    f"    {connector} {frame.function}  ({frame.location})"
                )
            table.add_row(
                "",
                Text.from_markup(f"[dim]{chr(10).join(trace_parts)}[/dim]"),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            )

    return table


# noinspection DuplicatedCode
def render_failed_table(failed: list[TestRecord], details: bool = False) -> Table:
    table = Table(
        title=Text("Failed", style="bold red"),
        show_header=True,
        expand=False,
        border_style="red",
        padding=(0, 1),
        show_lines=False,
    )
    table.add_column("#", style="dim", justify="right", no_wrap=True)
    table.add_column("Test", style="red bold", no_wrap=True)
    table.add_column("Params", style="yellow", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Where", style="dim", no_wrap=True)
    table.add_column("Error", no_wrap=True)
    table.add_column("Elapsed", style="magenta", justify="right", no_wrap=True)
    table.add_column("Setup", style="dim", justify="right", no_wrap=True)
    table.add_column("Call", style="dim", justify="right", no_wrap=True)
    table.add_column("Teardown", style="dim", justify="right", no_wrap=True)
    if details:
        table.add_column("Details", style="dim", no_wrap=True)

    for idx, test in enumerate(failed, 1):
        status_text = Text(test.status)
        if "Setup" in test.status or "Teardown" in test.status:
            status_text.stylize("yellow bold")
        else:
            status_text.stylize("red bold")

        error_summary = ""
        if test.error_type and test.error:
            error_summary = f"{test.error_type}: {test.error}"
        elif test.error:
            error_summary = test.error
        if len(error_summary) > 80:
            error_summary = error_summary[:77] + "..."

        row = [
            str(idx),
            test.test_name,
            test.test_params,
            status_text,
            test.error_location or "",
            error_summary,
            format_time(test.elapsed),
            format_time(test.duration_setup),
            format_time(test.duration_call),
            format_time(test.duration_teardown),
        ]
        if details:
            row.append(test.json_path)
        table.add_row(*row)

    return table


def collect(
    harvester_folder: str,
    use_py_spy: bool,
    full_trace: bool,
    details: bool = False,
    monitor_start: float | None = None,
) -> ConsoleRenderable:
    if not os.path.isdir(harvester_folder):
        return Text(
            f"Waiting for pytest to start (monitoring {harvester_folder}) ...",
            style="yellow",
        )

    running_tests: list[TestRecord]
    failed_tests: list[TestRecord]
    tests_counts: ScanCounts
    (
        running_tests,
        failed_tests,
        tests_counts,
    ) = scan_monitor_folder(
        harvester_folder,
        use_py_spy,
        full_trace,
    )

    if tests_counts.total_completed == 0 and tests_counts.collected is None:
        return Text(
            "No test records found. Waiting for tests ...",
            style="yellow",
        )

    pending_tests = tests_counts.pending
    fully_done = (
        tests_counts.collected is not None
        and pending_tests is not None
        and pending_tests <= 0
    )
    if fully_done:
        tests_counts.running = 0

    header = Text()
    header.append("pytest monitor", style="bold cyan")
    header.append("  [", style="dim")
    header.append(f"{tests_counts.total_completed} total", style="white")
    if tests_counts.collected is not None:
        header.append(f"/{tests_counts.collected}", style="dim")
    if tests_counts.running > 0:
        header.append(" | ", style="dim")
        header.append(f"{tests_counts.running} running", style="green")
    if pending_tests is not None and pending_tests > 0:
        header.append(" | ", style="dim")
        header.append(f"{pending_tests} pending", style="dim")
    header.append(" | ", style="dim")
    header.append(f"{tests_counts.passed} passed", style="green bold")
    if tests_counts.skipped:
        header.append(" | ", style="dim")
        header.append(f"{tests_counts.skipped} skipped", style="yellow")
    header.append(" | ", style="dim")
    header.append(f"{tests_counts.total_failed} failed", style="red bold")
    if tests_counts.orphaned:
        header.append(" | ", style="dim")
        header.append(f"{tests_counts.orphaned} orphaned", style="red")
    header.append("]", style="dim")

    if monitor_start is not None:
        header.append("  ", style="dim")
        header.append(
            f"monitoring for {format_time(time.monotonic() - monitor_start)}",
            style="dim",
        )

    parts: list[ConsoleRenderable] = [Text(""), header]

    if tests_counts.collected is not None and tests_counts.collected > 0:
        pct = tests_counts.total_completed / tests_counts.collected
        bar = ProgressBar(
            total=tests_counts.collected,
            completed=tests_counts.total_completed,
            width=40,
        )
        pct_text = Text(f" {pct:.2%}", style="bold cyan")
        progress_line = Columns(
            [Text("  "), bar, pct_text],
            padding=(0, 0),
            expand=False,
        )
        parts.append(progress_line)

    if tests_counts.total_failed > 0:
        breakdown = Text()
        breakdown.append("  failures: ", style="dim")
        items = []
        if tests_counts.error_setup:
            items.append(("setup", str(tests_counts.error_setup), "yellow"))
        if tests_counts.error_call:
            items.append(("execution", str(tests_counts.error_call), "red"))
        if tests_counts.error_teardown:
            items.append(("teardown", str(tests_counts.error_teardown), "yellow"))
        if tests_counts.error_unknown:
            items.append(("unknown", str(tests_counts.error_unknown), "dim"))
        for i, (label, count, style) in enumerate(items):
            if i > 0:
                breakdown.append(" | ", style="dim")
            breakdown.append(f"{count} {label}", style=style)
        parts.append(breakdown)

    if running_tests and not fully_done:
        parts.append(Text(""))
        parts.append(render_running_table(running_tests, full_trace))

    if failed_tests:
        parts.append(Text(""))
        parts.append(render_failed_table(failed_tests, details=details))

    if not failed_tests and (fully_done or not running_tests):
        parts.append(Text(""))
        parts.append(Text("All tests passed.", style="green bold"))

    return Group(*parts)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.version_option()
@click.option(
    "-f",
    "--folder",
    default=None,
    help="Harvester directory (default: $TDX_HARVESTER_FOLDER or "
    "~/.tabsdatax/pytest/harvester).",
)
@click.option(
    "-i",
    "--interval",
    default=2,
    type=int,
    show_default=True,
    help="Refresh interval in seconds.",
)
@click.option(
    "-t",
    "--trace",
    is_flag=True,
    default=False,
    help="Show full stack trace for running tests.",
)
@click.option(
    "-1",
    "--once",
    is_flag=True,
    default=False,
    help="Run once and exit (no continuous monitoring).",
)
@click.option(
    "--nospy",
    is_flag=True,
    default=False,
    help="Disable py-spy stack inspection for running tests.",
)
@click.option(
    "--details",
    is_flag=True,
    default=False,
    help="Show file path to JSON details for failed tests.",
)
def main(
    folder: str | None,
    interval: int,
    trace: bool,
    once: bool,
    nospy: bool,
    details: bool,
):
    """Monitor running pytest tests.

    \b
    Tips:
      Scrollable output:  tdx-monitor --no-spy --once | less -R
      Initial sudo for py-spy: sudo -v && tdx-monitor
    """

    harvester_folder = folder or os.environ.get(
        TDX_HARVESTER_FOLDER_ENV,
        TDX_HARVESTER_FOLDER_DEF,
    )
    harvester_folder = os.path.abspath(harvester_folder)

    use_py_spy = not nospy and has_py_spy()
    if not nospy and not use_py_spy:
        console.print(
            "[dim]py-spy not found — stack frames disabled. "
            "Install with: pip install py-spy[/dim]"
        )
    if use_py_spy:
        console.print("[dim]Authenticating for py-spy (sudo) ...[/dim]")
        subprocess.run(["sudo", "-v"])
        start_sudo_refresh()

    if once:
        console.print(
            collect(
                harvester_folder,
                use_py_spy,
                trace,
                details=details,
            )
        )
        console.print()
        return

    monitor_start = time.monotonic()
    with Live(
        collect(
            harvester_folder,
            use_py_spy,
            trace,
            details=details,
            monitor_start=monitor_start,
        ),
        console=console,
        refresh_per_second=MONITOR_REFRESH_INTERVAL_SECONDS,
        screen=True,
    ) as live:
        try:
            while True:
                time.sleep(interval)
                live.update(
                    collect(
                        harvester_folder,
                        use_py_spy,
                        trace,
                        details=details,
                        monitor_start=monitor_start,
                    )
                )
        except KeyboardInterrupt:
            pass

    console.print("[dim]Stopped.[/dim]")


if __name__ == "__main__":
    main.main()

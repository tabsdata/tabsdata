#
# Copyright 2026 Tabsdata Inc.
#

"""Textual-based dashboard for monitoring pytest test execution.

Replaces the previous Rich Live monitor with a scrollable, flicker-free
Textual application.  Reads ``_meta_state.json`` and the SQLite database
from the harvester folder on a polling interval.

Tables:
  1. Session Phases
  2. Fixtures (scope + lifecycle + countdown)
  3. Markers (countdown)
  4. Plugins (registration + activity)
  5. Running Tests (live, with optional py-spy frames)
  6. Failed Tests (accumulated errors)
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from dataclasses import dataclass, field

import rich_click as click
from rich.text import Text as RichText
from tenacity import retry, retry_if_result, stop_after_attempt, wait_fixed
from textual.app import App, ComposeResult
from textual.containers import Horizontal, VerticalScroll
from textual.widgets import DataTable, Footer, Static

from tabsdatax.pytesting.plugins.harvester.internal.constants import (
    FAILED_STATUSES,
    SESSION_PHASES_ORDERED,
    SUDO_REFRESH_INTERVAL_SECONDS,
    SUDO_REFRESH_TIMEOUT_SECONDS,
    TDX_HARVESTER_FOLDER_DEF,
    TDX_HARVESTER_FOLDER_ENV,
    Emoji,
    TestStatus,
)
from tabsdatax.pytesting.plugins.harvester.internal.dashboard import (
    SummaryHeader,
    TableSection,
    component_state_circle,
    fixture_scope_icon,
    fixture_scope_text,
    fixture_sort_key,
    fixture_state_circle,
    format_duration,
    format_time,
    header_for,
    marker_state_circle,
    phase_display_name,
    phase_status_circle,
    plugin_active_circle,
    plugin_sort_key,
    plugin_type_icon,
    text_for,
)
from tabsdatax.pytesting.plugins.harvester.internal.database import (
    query_component_stats,
    query_fixture_stats,
    query_outcome_counts,
    query_test_averages,
    query_test_durations,
    query_test_records,
    query_tests_per_minute,
)
from tabsdatax.pytesting.plugins.harvester.internal.schema import MetaState


# ---------------------------------------------------------------------------
# Stack frame / py-spy support
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Test record
# ---------------------------------------------------------------------------
@dataclass
class TestRecord:
    nodeid: str
    file: str
    line: int
    function: str
    status: str
    start: float
    stop: float | None
    worker: str
    pid: int
    duration_setup: float | None
    duration_call: float | None
    duration_teardown: float | None
    error: str | None
    error_type: str | None
    error_location: str | None
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
    def elapsed(self) -> float:
        if self.status == "Running":
            return time.time() - self.start
        return sum(
            d
            for d in (self.duration_setup, self.duration_call, self.duration_teardown)
            if d is not None
        )


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


def row_to_record(row) -> TestRecord:
    return TestRecord(
        nodeid=row["nodeid"] or "?",
        file=row["file"] or "?",
        line=row["line"] if row["line"] is not None else 0,
        function=row["function"] or "?",
        status=row["status"] or "?",
        start=row["start"] if row["start"] is not None else 0,
        stop=row["stop"],
        worker=row["worker"] or "",
        pid=row["pid"] if row["pid"] is not None else 0,
        duration_setup=row["duration_setup"],
        duration_call=row["duration_call"],
        duration_teardown=row["duration_teardown"],
        error=row["error"],
        error_type=row["error_type"],
        error_location=row["error_location"],
    )


def scan_test_files(
    folder: str,
    use_py_spy: bool = False,
    full_trace: bool = False,
) -> tuple[list[TestRecord], list[TestRecord], int, int, int]:
    """Scan test_records rows from the harvester database.

    Returns (running, failed, passed_count, skipped_count,
    orphaned_count).
    """
    running: list[TestRecord] = []
    failed: list[TestRecord] = []
    passed = 0
    skipped = 0
    orphaned = 0

    if not os.path.isdir(folder):
        return running, failed, passed, skipped, orphaned

    rows = query_test_records(folder)

    running_status = str(TestStatus.RUNNING)
    passed_status = str(TestStatus.PASSED)
    skipped_status = str(TestStatus.SKIPPED)
    failed_status_set = {str(status) for status in FAILED_STATUSES}

    for row in rows:
        status = row["status"]
        if status == passed_status:
            passed += 1
        elif status == skipped_status:
            skipped += 1
        elif status == running_status:
            record = row_to_record(row)
            if pid_exists(record.pid):
                running.append(record)
            else:
                orphaned += 1
        elif status in failed_status_set:
            failed.append(row_to_record(row))

    def worker_sort_key(record: TestRecord) -> tuple:
        worker = record.worker
        if worker.startswith("gw"):
            try:
                return 0, int(worker[2:])
            except ValueError:
                pass
        return (1, 0) if not worker else (0, 0)

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
                record.top_frame = dump.frames[test_frame_idx]
                if full_trace:
                    up_to_test = dump.frames[: test_frame_idx + 1]
                    up_to_test.reverse()
                    record.all_frames = up_to_test
            elif dump.frames:
                record.top_frame = dump.frames[-1]
                record.stack_depth = len(dump.frames)
                if full_trace:
                    record.all_frames = list(reversed(dump.frames))

    failed.sort(key=lambda r: r.test_name)
    return running, failed, passed, skipped, orphaned


# ---------------------------------------------------------------------------
# py-spy helpers
# ---------------------------------------------------------------------------
def has_py_spy() -> bool:
    try:
        subprocess.run(["py-spy", "--version"], capture_output=True, check=True)
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

    threading.Thread(target=run, daemon=True).start()


# ---------------------------------------------------------------------------
# Textual dashboard app
# ---------------------------------------------------------------------------
class HarvesterDashboard(App):
    """Textual app with 6 scrollable tables for monitoring pytest."""

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
        ("k", "kill_session", "Kill pytest Session"),
        ("Tip:", "noop", "Hold ⌥ Option + click to select text"),
    ]

    def action_noop(self) -> None:
        """No-op action for the tip binding."""

    def action_kill_session(self) -> None:
        """Send SIGKILL (kill -9) to the pytest session process."""
        import signal

        pid = self._meta.pid
        if pid and pid_exists(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

    CSS = """
    Screen {
        overflow: hidden;
        background: #282838;
    }

    #outer-scroll {
        height: 1fr;
        background: #282838;
    }

    .panel-pair {
        height: auto;
        margin-bottom: 1;
        border-bottom: solid #3a4a70;
    }

    .panel-pair > TableSection {
        width: 1fr;
        margin-bottom: 0;
    }

    #outer-scroll > TableSection {
        border-bottom: solid #3a4a70;
    }

    #bottom-spacer {
        height: 3;
        background: #282838;
    }

    Footer {
        background: #2e5090;
        color: #ffffff;
    }

    Footer > .footer--key {
        color: #ffffff;
        background: #243e70;
    }

    """

    def __init__(
        self,
        harvester_folder: str,
        poll_interval: float = 2.0,
        use_py_spy: bool = False,
        full_trace: bool = False,
        details: bool = False,
    ) -> None:
        super().__init__()
        self._folder = harvester_folder
        self._poll_interval = poll_interval
        self._use_py_spy = use_py_spy
        self._full_trace = full_trace
        self._details = details
        self._meta = MetaState()
        self._session_pid: int = 0  # tracks current session's PID
        self._cached_running: list = []
        self._cached_failed: list = []
        self._cached_passed: int = 0
        self._cached_skipped: int = 0
        self._cached_orphaned: int = 0
        self._cached_db_stats: list = []
        self._cached_component_db_stats: list = []
        self._cached_test_averages: dict = {}
        self._cached_test_durations: list = []
        self._cached_tests_per_minute: list = []
        self._running_keys: set[str] = set()
        self._failed_keys_ordered: tuple = ()
        self._failed_keys: set[str] = set()
        self._reporting_detected_at: float | None = None

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="outer-scroll"):
            yield SummaryHeader()
            with Horizontal(classes="panel-pair"):
                yield TableSection(
                    title="Session Phases",
                    table_id="phases",
                    table_height=len(SESSION_PHASES_ORDERED) + 1,
                    table_index=0,
                )
                yield TableSection(
                    title="Plugins",
                    table_id="plugins",
                    table_height=12,
                    table_index=3,
                )
            with Horizontal(classes="panel-pair"):
                yield TableSection(
                    title="Components",
                    table_id="components",
                    table_height=28,
                    table_index=6,
                )
                yield TableSection(
                    title="Markers",
                    table_id="markers",
                    table_height=28,
                    table_index=2,
                )
                yield TableSection(
                    title="Fixtures",
                    table_id="fixtures",
                    table_height=28,
                    table_index=1,
                )
            yield TableSection(
                title=f"Running Tests [{Emoji.CIRCLE_GREEN_STYLE}]{Emoji.CIRCLE}[/]",
                table_id="running",
                table_height=17,
                table_index=4,
            )
            yield TableSection(
                title=f"Failed Tests [{Emoji.CIRCLE_RED_STYLE}]{Emoji.CIRCLE}[/]",
                table_id="failed",
                table_height=17,
                table_index=5,
            )
            yield Static("", id="bottom-spacer")
        yield Footer()

    def on_mount(self) -> None:
        self._init_phases_table()
        self._init_components_table()
        self._init_fixtures_table()
        self._init_markers_table()
        self._init_plugins_table()
        self._init_running_table()
        self._init_failed_table()
        self.set_interval(self._poll_interval, self._poll)

    def action_refresh(self) -> None:
        self._poll()

    # ------------------------------------------------------------------
    # Table initialization
    # ------------------------------------------------------------------
    @staticmethod
    def _add_col(dt, h, label, key):
        dt.add_column(h(label), key=key)

    def _init_phases_table(self) -> None:
        t, h = text_for(0), header_for(0)
        dt = self.query_one("#phases", DataTable)

        def ac(label, key):
            self._add_col(dt, h, label, key)

        ac("Phase", "phase")
        ac("State", "status_icon")
        ac("Start", "start")
        ac("Duration", "duration")
        for phase in SESSION_PHASES_ORDERED:
            dt.add_row(
                t(phase_display_name(phase)),
                phase_status_circle("pending"),
                t(""),
                t(""),
                key=phase,
            )

    def _init_components_table(self) -> None:
        h = header_for(6)
        dt = self.query_one("#components", DataTable)

        def ac(label, key):
            self._add_col(dt, h, label, key)

        ac("Component", "component")
        ac("State", "state_icon")
        ac("Total", "total")
        ac("Passed", "passed")
        ac("Failed", "failed")
        ac("Pending", "pending")
        ac("Setup", "setup_count")
        ac("Teardown", "teardown_count")
        ac("Average", "avg_duration")

    def _init_fixtures_table(self) -> None:
        h = header_for(1)
        dt = self.query_one("#fixtures", DataTable)

        def ac(label, key):
            self._add_col(dt, h, label, key)

        ac("Fixture", "fixture")
        dt.add_column("", key="autouse_icon")
        ac("State", "state_icon")
        dt.add_column("", key="scope_icon")
        ac("Scope", "scope")
        ac("Total", "total")
        ac("Passed", "passed")
        ac("Failed", "failed")
        ac("Pending", "pending")
        ac("Setup", "setup_count")
        ac("Teardown", "teardown_count")
        ac("Average", "avg_setup")

    def _init_markers_table(self) -> None:
        h = header_for(2)
        dt = self.query_one("#markers", DataTable)

        def ac(label, key):
            self._add_col(dt, h, label, key)

        ac("Marker", "marker")
        ac("State", "state_icon")
        ac("Total", "total")
        ac("Passed", "passed")
        ac("Failed", "failed")
        ac("Skipped", "skipped")
        ac("Pending", "pending")

    def _init_plugins_table(self) -> None:
        h = header_for(3)
        dt = self.query_one("#plugins", DataTable)

        def ac(label, key):
            self._add_col(dt, h, label, key)

        ac("Plugin", "plugin")
        ac("State", "active_icon")
        ac("Type", "type")
        dt.add_column("", key="type_icon")

    def _init_running_table(self) -> None:
        h = header_for(4)
        dt = self.query_one("#running", DataTable)

        def ac(label, key):
            self._add_col(dt, h, label, key)

        ac("#", "idx")
        ac("Test", "test")
        ac("Parameters", "params")
        ac("Worker", "worker")
        ac("Start", "start")
        ac("Elapsed", "elapsed")
        ac("Setup", "setup")
        ac("Call", "call")
        ac("Teardown", "teardown")
        ac("Frame", "frame")
        ac("Depth", "depth")

    def _init_failed_table(self) -> None:
        h = header_for(5)
        dt = self.query_one("#failed", DataTable)

        def ac(label, key):
            self._add_col(dt, h, label, key)

        ac("#", "idx")
        ac("Test", "test")
        ac("Parameters", "params")
        ac("Status", "status")
        ac("Where", "where")
        ac("Error", "error")
        ac("Start", "start")
        ac("End", "end")
        ac("Elapsed", "elapsed")
        ac("Setup", "setup")
        ac("Call", "call")
        ac("Teardown", "teardown")
        if self._details:
            ac("Details", "details")

    # ------------------------------------------------------------------
    # Polling
    # ------------------------------------------------------------------
    def _poll(self) -> None:
        self.run_worker(self._poll_io, exclusive=True)

    async def _poll_io(self) -> None:
        """Run heavy I/O in a worker thread, then update UI on main thread."""
        import asyncio

        # I/O in thread
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self._collect_data)
        (
            meta,
            running,
            failed,
            passed,
            skipped,
            orphaned,
            db_stats,
            component_db_stats,
            test_averages,
            test_durations,
            tests_per_minute,
        ) = result

        # UI updates on main thread
        self._meta = meta
        if self._check_session_restart():
            return
        self._cached_running = running
        self._cached_failed = failed
        self._cached_passed = passed
        self._cached_skipped = skipped
        self._cached_orphaned = orphaned
        self._cached_db_stats = db_stats
        self._cached_component_db_stats = component_db_stats
        self._cached_test_averages = test_averages
        self._cached_test_durations = test_durations
        self._cached_tests_per_minute = tests_per_minute
        self._apply_updates()

    def _collect_data(self):
        """Heavy I/O: read meta, scan files, query DB. Called in thread."""
        # Load meta
        meta = self._meta
        meta_path = os.path.join(self._folder, "_meta_state.json")
        if os.path.exists(meta_path):
            try:
                with open(meta_path) as f:
                    data = json.load(f)
                if data.get("version", 1) >= 2:
                    meta = MetaState.from_dict(data)
            except (OSError, json.JSONDecodeError):
                pass

        # Scan test files
        running, failed, passed, skipped, orphaned = scan_test_files(
            self._folder,
            self._use_py_spy,
            self._full_trace,
        )

        # Query DB
        db_stats = query_fixture_stats(self._folder)
        component_db_stats = query_component_stats(self._folder)
        test_averages = query_test_averages(self._folder)
        test_durations = query_test_durations(self._folder)
        tests_per_minute = query_tests_per_minute(self._folder, meta.session_start_ts)

        return (
            meta,
            running,
            failed,
            passed,
            skipped,
            orphaned,
            db_stats,
            component_db_stats,
            test_averages,
            test_durations,
            tests_per_minute,
        )

    def _apply_updates(self) -> None:
        """Apply cached data to UI widgets. Called on main thread."""
        self._update_summary()
        self._update_phases()
        self._update_components()
        self._update_fixtures()
        self._update_markers()
        self._update_plugins()
        self._update_running(self._cached_running)
        self._update_failed(self._cached_failed)

    def _load_meta(self) -> None:
        meta_path = os.path.join(self._folder, "_meta_state.json")
        if not os.path.exists(meta_path):
            return
        try:
            with open(meta_path) as f:
                data = json.load(f)
            if data.get("version", 1) >= 2:
                self._meta = MetaState.from_dict(data)
        except (OSError, json.JSONDecodeError):
            pass

    def _clear_dashboard(self) -> None:
        """Clear all DataTables and summary header to remove stale data."""
        for dt in self.query(DataTable):
            dt.clear()
        self._running_keys = set()
        self._failed_keys_ordered = ()
        self._reporting_detected_at = None
        self._meta = MetaState()
        # Re-add phase rows (columns survive clear, but rows don't)
        t = text_for(0)
        dt = self.query_one("#phases", DataTable)
        for phase in SESSION_PHASES_ORDERED:
            dt.add_row(
                t(phase_display_name(phase)),
                phase_status_circle("pending"),
                t(""),
                t(""),
                key=phase,
            )
        # Reset the summary header with empty data
        header = self.query_one(SummaryHeader)
        header.update_summary(
            collected=0,
            passed=0,
            failed=0,
            skipped=0,
            running=0,
            orphaned=0,
            markers={},
            fixtures={},
            pid=0,
            session_start_ts=None,
            test_averages={},
            running_tests=[],
            test_durations=[],
            tests_per_minute=[],
        )

    def _check_session_restart(self) -> bool:
        """Detect when a new pytest session starts (PID changes) or
        the session process has died, and clear the dashboard.

        Returns True if the dashboard was cleared (caller should skip
        the rest of the poll cycle).
        """
        new_pid = self._meta.pid
        if new_pid == 0:
            return False
        if self._session_pid == 0:
            # First time seeing a PID — store it
            self._session_pid = new_pid
            return False
        if new_pid != self._session_pid:
            # New session started — clear old data and let new data populate
            self._session_pid = new_pid
            self._clear_dashboard()
            return False
        return False

    def _update_summary(self) -> None:
        passed = getattr(self, "_cached_passed", 0)
        skipped = getattr(self, "_cached_skipped", 0)
        orphaned = getattr(self, "_cached_orphaned", 0)
        failed_count = len(getattr(self, "_cached_failed", []))
        running_count = len(getattr(self, "_cached_running", []))
        collected = self._meta.collected or 0

        # Convert meta dicts for the summary widget
        markers_raw = (
            {k: v.to_dict() for k, v in self._meta.markers.items()}
            if self._meta.markers
            else {}
        )
        fixtures_raw = (
            {k: v.to_dict() for k, v in self._meta.fixtures.items()}
            if self._meta.fixtures
            else {}
        )

        # When PID is dead, freeze elapsed at last phase end time
        pid_alive_val = pid_exists(self._meta.pid) if self._meta.pid else False
        session_end_ts = None
        if self._meta.pid and not pid_alive_val:
            for ps in self._meta.phases.values():
                if ps.end and (session_end_ts is None or ps.end > session_end_ts):
                    session_end_ts = ps.end

        if (
            self._meta.pid
            and not pid_alive_val
            and collected
            and (passed + failed_count + skipped + orphaned + running_count) < collected
        ):
            db_counts = query_outcome_counts(self._folder)
            if (
                db_counts["passed"] + db_counts["failed"] + db_counts["skipped"]
                > passed + failed_count + skipped
            ):
                passed = max(passed, db_counts["passed"])
                skipped = max(skipped, db_counts["skipped"])

        header = self.query_one(SummaryHeader)
        header.update_summary(
            collected=collected,
            passed=passed,
            failed=failed_count,
            skipped=skipped,
            running=running_count,
            orphaned=orphaned,
            markers=markers_raw,
            fixtures=fixtures_raw,
            pid=self._meta.pid,
            pid_alive=pid_alive_val,
            session_start_ts=self._meta.session_start_ts,
            test_averages=self._cached_test_averages,
            running_tests=self._cached_running,
            test_durations=self._cached_test_durations,
            session_end_ts=session_end_ts,
            tests_per_minute=self._cached_tests_per_minute,
        )

    # ------------------------------------------------------------------
    # Table updates (flicker-free via update_cell)
    # ------------------------------------------------------------------
    def _update_phases(self) -> None:
        t = text_for(0)
        dt = self.query_one("#phases", DataTable)

        # Early Reporting detection: all tests done but meta still shows RUN
        collected = self._meta.collected or 0
        total_done = (
            self._cached_passed
            + len(self._cached_failed)
            + self._cached_skipped
            + self._cached_orphaned
        )
        running_count = len(self._cached_running)
        all_done = collected > 0 and running_count == 0 and total_done >= collected

        run_state = self._meta.phases.get("run")
        reporting_state = self._meta.phases.get("reporting")
        early_reporting = (
            all_done
            and run_state
            and run_state.status == "active"
            and (not reporting_state or reporting_state.status == "pending")
        )

        if early_reporting:
            if self._reporting_detected_at is None:
                self._reporting_detected_at = time.time()
        elif reporting_state and reporting_state.status != "pending":
            self._reporting_detected_at = None

        for phase in SESSION_PHASES_ORDERED:
            state = self._meta.phases.get(phase)
            if not state:
                continue

            status = state.status
            start = state.start
            duration = state.duration

            # Override RUN and REPORTING during early detection
            if early_reporting and self._reporting_detected_at:
                if phase == "run":
                    status = "done"
                    duration = (
                        self._reporting_detected_at - state.start
                        if state.start
                        else None
                    )
                elif phase == "reporting":
                    status = "active"
                    start = self._reporting_detected_at

            dt.update_cell(phase, "status_icon", phase_status_circle(status))
            dt.update_cell(phase, "start", t(format_time(start)))
            if status == "active" and start:
                duration = time.time() - start
            dt.update_cell(phase, "duration", t(format_duration(duration)))

    def _update_fixtures(self) -> None:
        t = text_for(1)
        dt = self.query_one("#fixtures", DataTable)
        fixtures = self._meta.fixtures

        # Use cached DB stats from background I/O
        db_stats = {}
        for row in getattr(self, "_cached_db_stats", []):
            db_stats[row["fixture"]] = row

        sorted_names = sorted(
            fixtures.keys(),
            key=lambda n: fixture_sort_key(n, fixtures[n].scope, fixtures[n].lifecycle),
        )
        existing_keys = {rk.value for rk in dt.rows}
        for name in sorted_names:
            fix = fixtures[name]
            circle = fixture_state_circle(
                fix.total, fix.passed, fix.failed, fix.skipped
            )

            # Merge DB stats
            stats = db_stats.get(name, {})
            setup_count = stats.get("setup_count", 0)
            teardown_count = stats.get("teardown_count", 0)
            avg_setup = stats.get("avg_setup_duration")
            avg_setup_str = format_duration(avg_setup) if avg_setup is not None else ""

            autouse_icon = (
                RichText("Ⓐ", style=Emoji.CIRCLE_PURPLE_STYLE) if fix.autouse else ""
            )

            if name in existing_keys:
                dt.update_cell(name, "autouse_icon", autouse_icon)
                dt.update_cell(name, "scope_icon", fixture_scope_icon(fix.scope))
                dt.update_cell(name, "scope", t(fixture_scope_text(fix.scope)))
                dt.update_cell(name, "state_icon", circle)
                dt.update_cell(name, "total", t(fix.total))
                dt.update_cell(name, "passed", t(fix.passed))
                dt.update_cell(name, "failed", t(fix.failed))
                dt.update_cell(name, "pending", t(fix.remaining))
                dt.update_cell(name, "setup_count", t(setup_count))
                dt.update_cell(name, "teardown_count", t(teardown_count))
                dt.update_cell(name, "avg_setup", t(avg_setup_str))
            else:
                dt.add_row(
                    t(name),
                    autouse_icon,
                    circle,
                    fixture_scope_icon(fix.scope),
                    t(fixture_scope_text(fix.scope)),
                    t(fix.total),
                    t(fix.passed),
                    t(fix.failed),
                    t(fix.remaining),
                    t(setup_count),
                    t(teardown_count),
                    t(avg_setup_str),
                    key=name,
                )

    def _update_components(self) -> None:
        t = text_for(6)
        dt = self.query_one("#components", DataTable)
        components = self._meta.components

        # Build lookup from cached DB stats
        db_stats = {}
        for row in self._cached_component_db_stats:
            db_stats[row["component"]] = row

        def component_sort_key(item):
            name = item[0]
            is_tabsdata = 0 if name.startswith("tabsdata") else 1
            return (is_tabsdata, name)

        existing_keys = {rk.value for rk in dt.rows}
        for name, state in sorted(components.items(), key=component_sort_key):
            circle = component_state_circle(
                state.total, state.passed, state.failed, state.skipped
            )

            stats = db_stats.get(name, {})
            setup_count = stats.get("setup_count", 0)
            teardown_count = stats.get("teardown_count", 0)
            avg_duration = stats.get("avg_duration")
            avg_str = format_duration(avg_duration) if avg_duration is not None else ""

            if name in existing_keys:
                dt.update_cell(name, "state_icon", circle)
                dt.update_cell(name, "total", t(state.total))
                dt.update_cell(name, "passed", t(state.passed))
                dt.update_cell(name, "failed", t(state.failed))
                dt.update_cell(name, "pending", t(state.pending))
                dt.update_cell(name, "setup_count", t(setup_count))
                dt.update_cell(name, "teardown_count", t(teardown_count))
                dt.update_cell(name, "avg_duration", t(avg_str))
            else:
                dt.add_row(
                    t(name),
                    circle,
                    t(state.total),
                    t(state.passed),
                    t(state.failed),
                    t(state.pending),
                    t(setup_count),
                    t(teardown_count),
                    t(avg_str),
                    key=name,
                )

    def _update_markers(self) -> None:
        t = text_for(2)
        dt = self.query_one("#markers", DataTable)
        markers = self._meta.markers
        existing_keys = {rk.value for rk in dt.rows}
        for name, state in sorted(markers.items()):
            circle = marker_state_circle(
                state.total, state.passed, state.failed, state.skipped
            )
            if name in existing_keys:
                dt.update_cell(name, "state_icon", circle)
                dt.update_cell(name, "total", t(state.total))
                dt.update_cell(name, "passed", t(state.passed))
                dt.update_cell(name, "failed", t(state.failed))
                dt.update_cell(name, "skipped", t(state.skipped))
                dt.update_cell(name, "pending", t(state.pending))
            else:
                dt.add_row(
                    t(name),
                    circle,
                    t(state.total),
                    t(state.passed),
                    t(state.failed),
                    t(state.skipped),
                    t(state.pending),
                    key=name,
                )

    def _update_plugins(self) -> None:
        t = text_for(3)
        dt = self.query_one("#plugins", DataTable)
        plugins = self._meta.plugins
        existing_keys = {rk.value for rk in dt.rows}
        sorted_plugins = sorted(
            plugins.items(),
            key=lambda item: plugin_sort_key(item[0], item[1].plugin_type),
        )
        for name, state in sorted_plugins:
            icon = plugin_type_icon(state.plugin_type)
            if name in existing_keys:
                dt.update_cell(name, "type", t(state.plugin_type.title()))
                dt.update_cell(name, "type_icon", icon)
                dt.update_cell(name, "active_icon", plugin_active_circle(state.active))
            else:
                dt.add_row(
                    t(name),
                    plugin_active_circle(state.active),
                    t(state.plugin_type.title()),
                    icon,
                    key=name,
                )

    def _update_test_tables(self) -> None:
        self._update_running(getattr(self, "_cached_running", []))
        self._update_failed(getattr(self, "_cached_failed", []))

    def _update_running(self, running: list[TestRecord]) -> None:
        t = text_for(4)
        dt = self.query_one("#running", DataTable)

        # Clear and rebuild to maintain sort order (by worker)
        dt.clear()
        for idx, test in enumerate(running, 1):
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
            setup_d = format_duration(setup_display)
            call_d = format_duration(call_display)
            teardown_d = format_duration(teardown_display)
            # Frame and depth from py-spy (if available on TestRecord)
            frame = ""
            depth = ""
            if test.top_frame:
                frame = f"{test.top_frame.function}  {test.top_frame.location}"
                depth = str(test.stack_depth) if test.stack_depth else ""
            dt.add_row(
                t(str(idx)),
                t(test.test_name),
                t(test.test_params),
                t(test.worker),
                t(format_time(test.start)),
                t(format_duration(test.elapsed)),
                t(setup_d),
                t(call_d),
                t(teardown_d),
                t(frame),
                t(depth),
                key=test.nodeid,
            )
            # Full stack trace as additional rows (one per frame)
            if self._full_trace and test.all_frames:
                for fi, sf in enumerate(test.all_frames):
                    connector = (
                        "\u251c\u2500"
                        if fi < len(test.all_frames) - 1
                        else "\u2514\u2500"
                    )
                    trace_line = f"  {connector} {sf.function}  ({sf.location})"
                    dt.add_row(
                        t(""),
                        t(trace_line),
                        t(""),
                        t(""),
                        t(""),
                        t(""),
                        t(""),
                        t(""),
                        t(""),
                        t(""),
                        t(""),
                        key=f"{test.nodeid}__trace_{fi}",
                    )

    def _update_failed(self, failed: list[TestRecord]) -> None:
        t = text_for(5)
        dt = self.query_one("#failed", DataTable)

        # Only rebuild if the set of failed tests changed
        new_keys = tuple(r.nodeid for r in failed)
        if new_keys == getattr(self, "_failed_keys_ordered", ()):
            return
        self._failed_keys_ordered = new_keys

        dt.clear()
        for idx, test in enumerate(failed, 1):
            error_summary = ""
            if test.error_type and test.error:
                error_summary = f"{test.error_type}: {test.error}"
            elif test.error:
                error_summary = test.error
            if len(error_summary) > 80:
                error_summary = error_summary[:77] + "..."

            where = test.error_location or ""
            setup_d = format_duration(test.duration_setup)
            call_d = format_duration(test.duration_call)
            teardown_d = format_duration(test.duration_teardown)

            start_time = format_time(test.start)
            end_time = format_time(test.stop) if test.stop else ""

            row = [
                t(str(idx)),
                t(test.test_name),
                t(test.test_params),
                t(test.status),
                t(where),
                t(error_summary),
                t(start_time),
                t(end_time),
                t(format_duration(test.elapsed)),
                t(setup_d),
                t(call_d),
                t(teardown_d),
            ]
            if self._details:
                row.append(t(test.nodeid))
            dt.add_row(*row, key=test.nodeid)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
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
    "--nospy",
    is_flag=True,
    default=False,
    help="Disable py-spy stack inspection for running tests.",
)
@click.option(
    "--details",
    is_flag=True,
    default=False,
    help="Show node id details for failed tests.",
)
def main(
    folder: str | None,
    interval: int,
    trace: bool,
    nospy: bool,
    details: bool,
):
    """Monitor running pytest tests with a scrollable Textual dashboard.

    \b
    Tips:
      Initial sudo for py-spy: sudo -v && tdx-harvester-monitor
      Scrollable output:       tdx-harvester-monitor --nospy
    """
    harvester_folder = folder or os.environ.get(
        TDX_HARVESTER_FOLDER_ENV,
        TDX_HARVESTER_FOLDER_DEF,
    )
    harvester_folder = os.path.abspath(harvester_folder)

    use_py_spy = not nospy and has_py_spy()
    if not nospy and not use_py_spy:
        print(
            "py-spy not found — stack frames disabled. Install with: pip install py-spy"
        )
    if use_py_spy:
        print("Authenticating for py-spy (sudo) ...")
        subprocess.run(["sudo", "-v"])
        start_sudo_refresh()

    app = HarvesterDashboard(
        harvester_folder=harvester_folder,
        poll_interval=float(interval),
        use_py_spy=use_py_spy,
        full_trace=trace,
        details=details,
    )
    app.run()


if __name__ == "__main__":
    main.main()

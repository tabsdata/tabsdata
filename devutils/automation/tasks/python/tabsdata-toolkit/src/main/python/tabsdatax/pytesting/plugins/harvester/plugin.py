#
#  Copyright 2026 Tabsdata Inc.
#

"""Pytest plugin that monitors test execution by persisting per-test rows in
SQLite and session-level metadata (phases, fixtures, markers, plugins) to
``_meta_state.json`` for the Textual dashboard.

This module is a thin hook dispatcher.  Each hook delegates to the
appropriate tracker in ``internal/``.

──────────────────────────────────────

**Hook lifecycle** (https://docs.pytest.org/en/stable/reference/reference.html)::

    pytest_addoption                Register configs & options.
            ↓
    pytest_configure                Initial plugin configuration & state.
            ↓
    pytest_sessionstart             Session begins.
            ↓
    pytest_collection_modifyitems   Run tasks after test collection.
            ↓
    pytest_fixture_setup            Before each fixture setup.
            ↓
    pytest_fixture_post_finalizer   After each fixture teardown.
            ↓
    ┌─── per test item ─────────────────────────────────────┐
    │                                                       │
    │   pytest_runtest_logstart     Tasks when test starts. │
    │           ↓                                           │
    │   pytest_runtest_makereport   Record test outcome.    │
    │       ↓       ↓       ↓                               │
    │     setup   call   teardown                           │
    └───────────────────────────────────────────────────────┘
            ↓
    pytest_sessionfinish            Session ends.
            ↓
    pytest_unconfigure              Final cleanup.

───────────

**Status transitions per test** (in the ``test_records.status`` column):

    logstart ─> Running
                    │
        ┌───────────┼──────────────────┬──────────────┬───────────┐
        ↓           ↓                  ↓              ↓           ↓
     Passed   Failed (Setup)    Failed (Execution)  Skipped    Unknown
        │
        ↓ (teardown fails)
    Failed (Teardown)
"""

import os
import shutil
import time

import pytest

from tabsdatax.pytesting.plugins.harvester.internal.components import ComponentTracker
from tabsdatax.pytesting.plugins.harvester.internal.constants import (
    PYTEST_OPTION_NUMPROCESSES,
    PYTEST_PLUGIN_XDIST,
    PYTEST_WORKERINPUT,
    PYTEST_XDIST_WORKER,
    TDX_HARVESTER_FOLDER_ARG,
    TDX_HARVESTER_FOLDER_DEF,
    TDX_HARVESTER_FOLDER_ENV,
    TDX_HARVESTER_FOLDER_OPT,
    SessionPhase,
    TestStatus,
)
from tabsdatax.pytesting.plugins.harvester.internal.database import (
    init_db,
    record_exists,
    record_fixture_setup_end,
    record_fixture_setup_start,
    record_fixture_teardown_end,
    record_test_completion,
    record_test_running,
    reset_db,
    upsert_test_final,
)
from tabsdatax.pytesting.plugins.harvester.internal.extensions import PluginTracker
from tabsdatax.pytesting.plugins.harvester.internal.fixtures import FixtureTracker
from tabsdatax.pytesting.plugins.harvester.internal.helpers import (
    handle_call,
    handle_setup,
    handle_teardown,
    handle_unknown,
)
from tabsdatax.pytesting.plugins.harvester.internal.markers import MarkerTracker
from tabsdatax.pytesting.plugins.harvester.internal.phases import PhaseTracker
from tabsdatax.pytesting.plugins.harvester.internal.writer import (
    MetaMessage,
    MetaWriter,
)

# ──────────────────────────────────────────────────────────────────────
# Module-level state (each process — controller and workers — gets its
# own copy, which is exactly what we need for xdist).
# ──────────────────────────────────────────────────────────────────────
g_harvester_folder: str | None = None
g_is_xdist_controller: bool = False
g_config: pytest.Config | None = None
g_collected: int = 0
g_completed: int = 0

# Tracker config attribute keys
_CFG_WRITER = "_tdx_meta_writer"
_CFG_PHASE_TRACKER = "_tdx_phase_tracker"
_CFG_FIXTURE_TRACKER = "_tdx_fixture_tracker"
_CFG_MARKER_TRACKER = "_tdx_marker_tracker"
_CFG_COMPONENT_TRACKER = "_tdx_component_tracker"
_CFG_PLUGIN_TRACKER = "_tdx_plugin_tracker"


def _get(config: pytest.Config, attr: str, default=None):
    return getattr(config, attr, default)


# ──────────────────────────────────────────────────────────────────────
# Hook implementations
# ──────────────────────────────────────────────────────────────────────


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("tdx-harvester", "Test execution monitoring")
    group.addoption(
        TDX_HARVESTER_FOLDER_OPT,
        dest=TDX_HARVESTER_FOLDER_ARG,
        default=None,
        help="Folder where the harvester database and metadata are written. "
        "Falls back to ${TDX_HARVESTER_FOLDER_ENV}, "
        "then $HOME/.tabsdatax/pytest/harvester.",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Initialize harvester folder, writer, and all trackers.

    Module globals ``g_harvester_folder`` and ``g_is_xdist_controller``
    are set here — each process (controller + workers) gets its own copy.
    """
    global g_harvester_folder, g_is_xdist_controller, g_config, g_collected, g_completed
    g_completed = 0

    harvester_folder = config.getoption(
        TDX_HARVESTER_FOLDER_ARG,
        default=None,
    ) or os.environ.get(TDX_HARVESTER_FOLDER_ENV)
    if not harvester_folder:
        harvester_folder = TDX_HARVESTER_FOLDER_DEF
    harvester_folder = os.path.abspath(harvester_folder)

    is_worker = hasattr(config, PYTEST_WORKERINPUT)
    xdist_active = (
        is_worker
        or config.pluginmanager.has_plugin(PYTEST_PLUGIN_XDIST)
        and getattr(config.option, PYTEST_OPTION_NUMPROCESSES, None) is not None
    )
    g_is_xdist_controller = xdist_active and not is_worker

    if not is_worker:
        if os.path.exists(harvester_folder):
            shutil.rmtree(harvester_folder)
    os.makedirs(harvester_folder, exist_ok=True)

    g_harvester_folder = harvester_folder
    g_config = config

    # Initialize SQLite database (controller resets, workers just init)
    if not is_worker:
        reset_db(harvester_folder)
    else:
        init_db(harvester_folder)

    # Only the controller (or non-xdist main process) manages _meta.json.
    # Workers only write per-test JSON files via the module globals above.
    if not is_worker:
        meta_path = os.path.join(harvester_folder, "_meta_state.json")
        writer = MetaWriter(meta_path, fresh=True)
        writer.start()
        setattr(config, _CFG_WRITER, writer)

        # Store controller PID and start time so the monitor can detect
        # session restarts and show elapsed time
        writer.enqueue(
            MetaMessage(
                kind="full_sync",
                payload={
                    "pid": os.getpid(),
                    "session_start_ts": time.time(),
                },
            )
        )

        phase_tracker = PhaseTracker(writer)
        phase_tracker.enter_phase(SessionPhase.CONFIGURE)
        setattr(config, _CFG_PHASE_TRACKER, phase_tracker)

        fixture_tracker = FixtureTracker(writer)
        setattr(config, _CFG_FIXTURE_TRACKER, fixture_tracker)

        marker_tracker = MarkerTracker(writer)
        setattr(config, _CFG_MARKER_TRACKER, marker_tracker)

        component_tracker = ComponentTracker(writer)
        setattr(config, _CFG_COMPONENT_TRACKER, component_tracker)

        plugin_tracker = PluginTracker(writer)
        plugin_tracker.snapshot(config.pluginmanager)
        setattr(config, _CFG_PLUGIN_TRACKER, plugin_tracker)


def pytest_sessionstart(session: pytest.Session) -> None:
    phase_tracker: PhaseTracker | None = _get(session.config, _CFG_PHASE_TRACKER)
    if phase_tracker:
        phase_tracker.transition(SessionPhase.SESSION_START)

    writer: MetaWriter | None = _get(session.config, _CFG_WRITER)
    if writer:
        writer.enqueue(
            MetaMessage(
                kind="full_sync",
                payload={"session_start_ts": time.time()},
            )
        )


# noinspection PyUnusedLocal
def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Register fixtures and markers, write collected count."""
    global g_collected

    if not g_harvester_folder or not items:
        return

    phase_tracker: PhaseTracker | None = _get(config, _CFG_PHASE_TRACKER)
    if phase_tracker:
        phase_tracker.transition(SessionPhase.COLLECTION)

    g_collected = len(items)
    writer: MetaWriter | None = _get(config, _CFG_WRITER)
    if writer:
        writer.enqueue(
            MetaMessage(
                kind="full_sync",
                payload={"collected": g_collected},
            )
        )

    fixture_tracker: FixtureTracker | None = _get(config, _CFG_FIXTURE_TRACKER)
    if fixture_tracker:
        fixture_tracker.register_all(items)

    marker_tracker: MarkerTracker | None = _get(config, _CFG_MARKER_TRACKER)
    if marker_tracker:
        marker_tracker.register_all(items)

    component_tracker: ComponentTracker | None = _get(config, _CFG_COMPONENT_TRACKER)
    if component_tracker:
        component_tracker.register_all(items)


def pytest_deselected(items: list[pytest.Item]) -> None:
    """Adjust tracker counts when tests are excluded by filters (-m, -k)."""
    global g_collected

    if not g_config:
        return

    marker_tracker: MarkerTracker | None = _get(g_config, _CFG_MARKER_TRACKER)
    if marker_tracker:
        marker_tracker.on_deselected(items)

    fixture_tracker: FixtureTracker | None = _get(g_config, _CFG_FIXTURE_TRACKER)
    if fixture_tracker:
        fixture_tracker.on_deselected(items)

    component_tracker: ComponentTracker | None = _get(g_config, _CFG_COMPONENT_TRACKER)
    if component_tracker:
        component_tracker.on_deselected(items)

    g_collected = max(0, g_collected - len(items))
    writer: MetaWriter | None = _get(g_config, _CFG_WRITER)
    if writer:
        writer.enqueue(
            MetaMessage(
                kind="full_sync",
                payload={"collected": g_collected},
            )
        )


@pytest.hookimpl(wrapper=True)
def pytest_fixture_setup(fixturedef, request):
    # Tracker updates (controller only)
    fixture_tracker: FixtureTracker | None = _get(request.config, _CFG_FIXTURE_TRACKER)
    if fixture_tracker:
        fixture_tracker.on_setup(fixturedef, request)

    # DB write (works on both controller and workers)
    fname = fixturedef.argname
    scope = str(fixturedef.scope or "function")
    worker = os.environ.get(PYTEST_XDIST_WORKER, "")
    if g_harvester_folder:
        record_fixture_setup_start(g_harvester_folder, fname, scope, worker)

    t0 = time.time()
    try:
        result = yield
    finally:
        duration = time.time() - t0
        if fixture_tracker:
            fixture_tracker.on_setup_complete(fixturedef, request)
        if g_harvester_folder:
            record_fixture_setup_end(g_harvester_folder, fname, scope, duration, worker)

    return result


_torn_down: set[str] = set()


def pytest_fixture_post_finalizer(fixturedef, request) -> None:
    # For non-function-scoped fixtures, pytest calls this hook for every test
    # that uses the fixture, not just when the fixture is actually torn down.
    # Only record the first teardown per fixture per process.
    fname = fixturedef.argname
    scope = str(fixturedef.scope or "function")
    if scope != "function":
        key = f"{fname}:{scope}"
        if key in _torn_down:
            return
        # Only count as torn down if the cached result was invalidated.
        cached = getattr(fixturedef, "cached_result", None)
        if cached is not None:
            return
        _torn_down.add(key)

    # Tracker updates (controller only)
    fixture_tracker: FixtureTracker | None = _get(request.config, _CFG_FIXTURE_TRACKER)
    if fixture_tracker:
        fixture_tracker.on_teardown(fixturedef, request)

    # DB write (works on both controller and workers)
    worker = os.environ.get(PYTEST_XDIST_WORKER, "")
    if g_harvester_folder:
        record_fixture_teardown_end(g_harvester_folder, fname, scope, worker)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logstart(nodeid: str, location: tuple) -> None:
    """Insert a ``Running`` row in test_records when a test starts.

    Skipped on the xdist controller to avoid orphan rows — under xdist this
    hook fires on the controller for every dispatched test, but only workers
    actually run them.
    """
    if g_is_xdist_controller:
        return

    if not g_harvester_folder:
        return

    worker = os.environ.get(PYTEST_XDIST_WORKER, "")
    record_test_running(
        g_harvester_folder,
        nodeid=nodeid,
        file=location[0],
        line=location[1],
        function=location[2],
        status=str(TestStatus.RUNNING),
        start=time.time(),
        worker=worker,
    )


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    """Update the test's database row after each phase (setup / call / teardown).

    Called up to three times per test item.  Each invocation records the phase
    duration, captures any error or skip information, and transitions the row's
    status column to the outcome status (Passed / Failed (Setup) / Failed
    (Execution) / Failed (Teardown) / Skipped / Unknown).
    """
    report = yield

    nodeid = item.nodeid

    if not g_harvester_folder:
        return report

    phase_tracker: PhaseTracker | None = _get(item.config, _CFG_PHASE_TRACKER)
    if phase_tracker:
        phase_tracker.ensure(SessionPhase.RUN)

    if g_is_xdist_controller:
        return report

    match call.when:
        case "setup":
            handle_setup(g_harvester_folder, nodeid, call, report)
        case "call":
            handle_call(g_harvester_folder, nodeid, call, report)
        case "teardown":
            handle_teardown(g_harvester_folder, nodeid, call, report)
        case _:
            handle_unknown(g_harvester_folder, nodeid, call, report)

    return report


def _notify_trackers_on_result(
    config: pytest.Config,
    nodeid: str,
    report: pytest.TestReport,
) -> None:
    """Notify fixture and marker trackers about a test result,
    and update cumulative duration counters."""
    if report.skipped:
        outcome = "skipped"
    elif report.failed:
        outcome = "failed"
    else:
        outcome = "passed"

    marker_tracker: MarkerTracker | None = _get(config, _CFG_MARKER_TRACKER)
    if marker_tracker:
        marker_tracker.on_test_result(nodeid, outcome)

    fixture_tracker: FixtureTracker | None = _get(config, _CFG_FIXTURE_TRACKER)
    if fixture_tracker:
        fixture_tracker.on_test_complete(nodeid, outcome)

    component_tracker: ComponentTracker | None = _get(config, _CFG_COMPONENT_TRACKER)
    if component_tracker:
        component_tracker.on_test_result(nodeid, outcome)

    # Record test completion in DB for averages and distribution.
    # Only on controller / non-xdist to avoid double-counting
    # (under xdist, workers also fire logreport).
    is_worker = bool(os.environ.get(PYTEST_XDIST_WORKER))
    if (
        g_harvester_folder
        and not is_worker
        and outcome in ("passed", "failed", "skipped")
    ):
        try:
            component = ""
            if component_tracker:
                component = component_tracker.get_component(nodeid)
            record_test_completion(
                g_harvester_folder,
                nodeid,
                outcome,
                report.duration,
                "",  # controller doesn't have a worker id
                component,
            )
        except Exception:
            pass

        status_from_outcome = {
            "passed": str(TestStatus.PASSED),
            "failed": str(TestStatus.FAILED_EXECUTION),
            "skipped": str(TestStatus.SKIPPED),
        }
        if g_is_xdist_controller and not record_exists(g_harvester_folder, nodeid):
            try:
                now = time.time()
                fields = {
                    "start": now - (report.duration or 0.0),
                    "stop": now,
                    "duration_call": report.duration or 0.0,
                }
                if outcome == "skipped" and getattr(report, "longrepr", None):
                    fields["skip_reason"] = str(report.longrepr)
                upsert_test_final(
                    g_harvester_folder,
                    nodeid,
                    status_from_outcome[outcome],
                    **fields,
                )
            except Exception:
                pass


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    """Called on every process when a test report is available.

    On the controller (xdist) this is the only hook that receives test
    results — ``pytest_runtest_makereport`` only fires on workers.
    On non-xdist this fires after ``pytest_runtest_makereport``.

    We use this as the single place to update marker/fixture counters
    and ensure the RUN phase is active.
    """
    global g_completed

    # Ensure we're in RUN phase on first test report
    if g_config is not None:
        phase_tracker: PhaseTracker | None = _get(g_config, _CFG_PHASE_TRACKER)
        if phase_tracker:
            phase_tracker.ensure(SessionPhase.RUN)

    # Count test results from the "call" phase, plus setup failures/skips
    # (setup failures/skips mean the test never reaches "call")
    should_count = report.when == "call" or (
        report.when == "setup" and (report.failed or report.skipped)
    )
    if should_count and g_config is not None:
        _notify_trackers_on_result(g_config, report.nodeid, report)
        # Transition to Reporting when all tests complete
        g_completed += 1
        if g_collected > 0 and g_completed >= g_collected:
            _pt = _get(g_config, _CFG_PHASE_TRACKER)
            if _pt:
                _pt.transition(SessionPhase.REPORTING)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    phase_tracker: PhaseTracker | None = _get(session.config, _CFG_PHASE_TRACKER)
    if phase_tracker:
        phase_tracker.transition(SessionPhase.SESSION_FINISH)


def pytest_unconfigure(config: pytest.Config) -> None:
    phase_tracker: PhaseTracker | None = _get(config, _CFG_PHASE_TRACKER)
    if phase_tracker:
        phase_tracker.transition(SessionPhase.UNCONFIGURE)
        phase_tracker.finish_all()

    writer: MetaWriter | None = _get(config, _CFG_WRITER)
    if writer:
        writer.flush()
        writer.stop()

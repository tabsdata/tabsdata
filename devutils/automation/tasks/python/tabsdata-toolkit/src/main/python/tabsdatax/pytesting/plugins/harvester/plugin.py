#
#  Copyright 2026 Tabsdata Inc.
#

"""Pytest plugin that monitors test execution by persisting per-test rows in
SQLite (``_meta_database.db``) and session-level metadata (phases, fixtures,
markers, plugins) to ``_meta_state.json`` for the Textual dashboard.

Each session writes to a folder of its own, ``<base>/<UTC stamp>-<pid>``,
whose name the session header prints; ``tdx-harvester-monitor <session>``
follows it. The controller (or the only process, without xdist) creates the
folder and hands it to the xdist workers. A pytest started from inside a
harvested session (pytester, a subprocess) records nothing, so it never
touches the session around it; neither does one on GitHub Actions
(``TD_ON_GA=true``).

This module is a thin hook dispatcher.  Each hook delegates to the
appropriate tracker in ``internal/``.

──────────────────────────────────────

**Hook lifecycle** (https://docs.pytest.org/en/stable/reference/reference.html)::

    pytest_addoption                Register configs & options.
            ↓
    pytest_configure                Session folder, trackers, writer.
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

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from tabsdatax.pytesting.plugins.harvester.internal.collection import (
    CollectedTest,
    describe,
    read_collection,
    write_collection,
)
from tabsdatax.pytesting.plugins.harvester.internal.components import ComponentTracker
from tabsdatax.pytesting.plugins.harvester.internal.constants import (
    TDX_HARVESTER_FOLDER_ARG,
    TDX_HARVESTER_FOLDER_OPT,
    TDX_HARVESTER_KEEP_ARG,
    TDX_HARVESTER_KEEP_DEF,
    TDX_HARVESTER_KEEP_OPT,
    TDX_HARVESTER_SESSION_ENV,
    TDX_HARVESTER_SESSION_INPUT,
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
from tabsdatax.pytesting.plugins.harvester.internal.sessions import (
    base_folder,
    create_session,
    prune_sessions,
)
from tabsdatax.pytesting.plugins.harvester.internal.writer import (
    MetaMessage,
    MetaWriter,
)

PLUGIN_NAME = "tdx-harvester-session"
# Nobody watches a dashboard on GitHub Actions, so its runs harvest nothing.
TD_ON_GA_ENV = "TD_ON_GA"
DESCRIBING_WORKER = "gw0"


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("tdx-harvester", "Test execution monitoring")
    group.addoption(
        TDX_HARVESTER_FOLDER_OPT,
        dest=TDX_HARVESTER_FOLDER_ARG,
        default=None,
        help="Folder holding a folder per harvested session. "
        "Falls back to $TDX_HARVESTER_FOLDER, "
        "then $HOME/.tabsdatax/pytest/harvester.",
    )
    group.addoption(
        TDX_HARVESTER_KEEP_OPT,
        dest=TDX_HARVESTER_KEEP_ARG,
        type=int,
        default=TDX_HARVESTER_KEEP_DEF,
        help="How many earlier, finished sessions to keep. "
        f"Default: {TDX_HARVESTER_KEEP_DEF}.",
    )


def on_github_actions() -> bool:
    return os.environ.get(TD_ON_GA_ENV) == "true"


def distributed(config: pytest.Config) -> bool:
    return (
        not config.getoption("collectonly")
        and config.getoption("dist", "no") != "no"
        and bool(config.getoption("tx", None))
    )


@pytest.hookimpl(trylast=True)
def pytest_configure(config: pytest.Config) -> None:
    workerinput = getattr(config, "workerinput", None)
    if workerinput is not None:
        folder = workerinput.get(TDX_HARVESTER_SESSION_INPUT)
        if folder:
            harvester = Harvester(config, Path(folder), worker=True)
            config.pluginmanager.register(harvester, PLUGIN_NAME)
        return
    if os.environ.get(TDX_HARVESTER_SESSION_ENV) or on_github_actions():
        return
    base = base_folder(config.getoption(TDX_HARVESTER_FOLDER_ARG))
    folder = create_session(base)
    prune_sessions(base, config.getoption(TDX_HARVESTER_KEEP_ARG), folder.name)
    os.environ[TDX_HARVESTER_SESSION_ENV] = folder.name
    harvester = Harvester(config, folder, worker=False, controller=distributed(config))
    config.pluginmanager.register(harvester, PLUGIN_NAME)


class Harvester:
    """The hooks of one harvested pytest process: the xdist controller or
    the only process (``worker=False``), or an xdist worker."""

    def __init__(
        self,
        config: pytest.Config,
        folder: Path,
        worker: bool,
        controller: bool = False,
    ) -> None:
        self.config = config
        self.folder = str(folder)
        self.session = folder.name
        self.worker = worker
        self.controller = controller
        self.worker_id = os.environ.get("PYTEST_XDIST_WORKER", "")
        self.collected = 0
        self.completed = 0
        self.writer: MetaWriter | None = None
        self.phases: PhaseTracker | None = None
        self.fixtures: FixtureTracker | None = None
        self.markers: MarkerTracker | None = None
        self.components: ComponentTracker | None = None
        self.plugins: PluginTracker | None = None

        init_db(self.folder)
        if worker:
            return

        self.writer = MetaWriter(
            os.path.join(self.folder, "_meta_state.json"), fresh=True
        )
        self.writer.start()
        self.writer.enqueue(
            MetaMessage(
                kind="full_sync",
                payload={"pid": os.getpid(), "session_start_ts": time.time()},
            )
        )
        self.phases = PhaseTracker(self.writer)
        self.phases.enter_phase(SessionPhase.CONFIGURE)
        self.fixtures = FixtureTracker(self.writer)
        self.markers = MarkerTracker(self.writer)
        self.components = ComponentTracker(self.writer)
        self.plugins = PluginTracker(self.writer)
        self.plugins.snapshot(config.pluginmanager)

    def _sync(self, **payload) -> None:
        if self.writer:
            self.writer.enqueue(MetaMessage(kind="full_sync", payload=payload))

    def pytest_report_header(self, config: pytest.Config) -> list[str]:
        if self.worker:
            return []
        line = f"harvester session: {self.session} (tdpytest {self.session})"
        reporter = config.pluginmanager.getplugin("terminalreporter")
        if reporter is None:
            return [line]
        return [reporter._tw.markup(line, purple=True, bold=True)]

    @pytest.hookimpl(optionalhook=True)
    def pytest_configure_node(self, node) -> None:
        node.workerinput[TDX_HARVESTER_SESSION_INPUT] = self.folder

    @pytest.hookimpl(optionalhook=True)
    def pytest_xdist_node_collection_finished(self, node, ids: list[str]) -> None:
        # Under xdist only the workers collect; the first one describes its
        # tests in the session folder before it reports them here.
        if self.collected or node.gateway.id != DESCRIBING_WORKER:
            return
        tests = read_collection(self.folder)
        if tests is None:
            return
        self._register(tests)

    def pytest_sessionstart(self, session: pytest.Session) -> None:
        if self.phases:
            self.phases.transition(SessionPhase.SESSION_START)
        self._sync(session_start_ts=time.time())

    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(
        self,
        config: pytest.Config,
        items: list[pytest.Item],
    ) -> None:
        """Register the tests left once -m and -k have deselected theirs."""
        if self.worker:
            if self.worker_id == DESCRIBING_WORKER:
                write_collection(self.folder, describe(items))
            return
        if not self.controller:
            self._register(describe(items))

    def _register(self, tests: list[CollectedTest]) -> None:
        if self.phases:
            self.phases.transition(SessionPhase.COLLECTION)
        self.collected = len(tests)
        self._sync(collected=self.collected)
        if self.fixtures:
            self.fixtures.register_all(tests)
        if self.markers:
            self.markers.register_all(tests)
        if self.components:
            self.components.register_all(tests)

    @pytest.hookimpl(wrapper=True)
    def pytest_fixture_setup(self, fixturedef, request):
        if self.fixtures:
            self.fixtures.on_setup(fixturedef, request)
        name = fixturedef.argname
        scope = str(fixturedef.scope or "function")
        record_fixture_setup_start(self.folder, name, scope, self.worker_id)
        started = time.time()
        try:
            return (yield)
        finally:
            if self.fixtures:
                self.fixtures.on_setup_complete(fixturedef, request)
            record_fixture_setup_end(
                self.folder, name, scope, time.time() - started, self.worker_id
            )

    def pytest_fixture_post_finalizer(self, fixturedef, request) -> None:
        # pytest calls this once per actual teardown of the fixture, whatever
        # its scope.
        if self.fixtures:
            self.fixtures.on_teardown(fixturedef, request)
        record_fixture_teardown_end(
            self.folder,
            fixturedef.argname,
            str(fixturedef.scope or "function"),
            self.worker_id,
        )

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_logstart(self, nodeid: str, location: tuple) -> None:
        """Insert a ``Running`` row in test_records when a test starts.

        Skipped on the xdist controller, which gets this hook for every test
        a worker starts: the worker records it.
        """
        if self.controller:
            return
        record_test_running(
            self.folder,
            nodeid=nodeid,
            file=location[0],
            line=location[1],
            function=location[2],
            status=str(TestStatus.RUNNING),
            start=time.time(),
            worker=self.worker_id,
        )

    @pytest.hookimpl(wrapper=True)
    def pytest_runtest_makereport(self, item: pytest.Item, call: pytest.CallInfo):
        """Update the test's database row after each phase (setup / call /
        teardown): its duration, any error or skip, and its status."""
        report = yield
        if self.phases:
            self.phases.ensure(SessionPhase.RUN)
        if self.controller:
            return report
        match call.when:
            case "setup":
                handle_setup(self.folder, item.nodeid, call, report)
            case "call":
                handle_call(self.folder, item.nodeid, call, report)
            case "teardown":
                handle_teardown(self.folder, item.nodeid, call, report)
            case _:
                handle_unknown(self.folder, item.nodeid, call, report)
        return report

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        """Update the marker, fixture and component counters, on the
        controller or the only process: under xdist the controller gets
        every result here, while makereport runs only on the workers."""
        if self.worker:
            return
        if self.phases:
            self.phases.ensure(SessionPhase.RUN)
        counts = report.when == "call" or (
            report.when == "setup" and (report.failed or report.skipped)
        )
        if not counts:
            return
        self._record_result(report)
        self.completed += 1
        if self.phases and 0 < self.collected <= self.completed:
            self.phases.transition(SessionPhase.REPORTING)

    def _record_result(self, report: pytest.TestReport) -> None:
        nodeid = report.nodeid
        if report.skipped:
            outcome = "skipped"
        elif report.failed:
            outcome = "failed"
        else:
            outcome = "passed"
        if self.markers:
            self.markers.on_test_result(nodeid, outcome)
        if self.fixtures:
            self.fixtures.on_test_complete(nodeid, outcome)
        component = ""
        if self.components:
            self.components.on_test_result(nodeid, outcome)
            component = self.components.get_component(nodeid)
        try:
            record_test_completion(
                self.folder, nodeid, outcome, report.duration, "", component
            )
        except Exception:
            pass
        if not self.controller or record_exists(self.folder, nodeid):
            return
        status = {
            "passed": TestStatus.PASSED,
            "failed": TestStatus.FAILED_EXECUTION,
            "skipped": TestStatus.SKIPPED,
        }[outcome]
        now = time.time()
        fields = {
            "start": now - (report.duration or 0.0),
            "stop": now,
            "duration_call": report.duration or 0.0,
        }
        if outcome == "skipped" and getattr(report, "longrepr", None):
            fields["skip_reason"] = str(report.longrepr)
        try:
            upsert_test_final(self.folder, nodeid, str(status), **fields)
        except Exception:
            pass

    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        if self.phases:
            self.phases.transition(SessionPhase.SESSION_FINISH)

    def pytest_unconfigure(self, config: pytest.Config) -> None:
        if self.phases:
            self.phases.transition(SessionPhase.UNCONFIGURE)
            self.phases.finish_all()
        if self.writer:
            self.writer.flush()
            self.writer.stop()
        if (
            not self.worker
            and os.environ.get(TDX_HARVESTER_SESSION_ENV) == self.session
        ):
            del os.environ[TDX_HARVESTER_SESSION_ENV]

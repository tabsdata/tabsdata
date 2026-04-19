#
#  Copyright 2026 Tabs Data Inc.
#

"""Pytest plugin that monitors tests execution by writing per-test JSON files.

Each test produces a file that is created on start and renamed on completion
with a prefix indicating the outcome.

File prefix convention is defined in the
**tabsdatax.pytest.plugins.harvester.internal.constants.TestFilePrefix** enum.

Filenames use the full test nodeid (including parameters) sanitized for
filesystem safety. All files are placed flat in a single directory.

Accepts a single setting — the harvester folder — resolved from the first
available source in the following order:

- ``--tdx-harvester-folder`` command-line option
- ``TDX_HARVESTER_FOLDER`` environment variable
- ``$HOME/.tabsdatax/pytest/harvester`` (default)

The harvester folder is cleaned at session start so that only results from the
current run are present.

──────────────────────────────────────

**Hook lifecycle** (https://docs.pytest.org/en/stable/reference/reference.html)::

    pytest_addoption                Register configs & options.
            ↓
    pytest_configure                Initial plugin configuration & state.
            ↓
    pytest_collection_modifyitems   Run tasks after test collection.
            ↓
    ┌─── per test item ─────────────────────────────────────┐
    │                                                       │
    │   pytest_runtest_logstart     Tasks when test starts. │
    │           ↓                                           │
    │   pytest_runtest_makereport   Record test outcome.    │
    │       ↓       ↓       ↓                               │
    │     setup   call   teardown                           │
    └───────────────────────────────────────────────────────┘

───────────

**File transitions per test**::

    logstart ─> 1.<id>.json (RUNNING)
                    │
        ┌───────────┼─────────────┬───────────┬───────────┐
        ↓           ↓             ↓           ↓           ↓
    0.0.<id>    0.1.<id>      0.2.<id>      2.<id>    0.4.<id>
    (PASSED)  (ERROR_SETUP) (ERROR_CALL)   (SKIPPED)  (UNKNOWN)
        │
        ↓ (teardown fails)
    0.3.<id>
    (ERROR_TEARDOWN)
"""

import json
import os
import shutil
import time

import pytest

from tabsdatax.pytest.plugins.harvester.internal.constants import (
    PHASE_DURATION_FIELD,
    PYTEST_OPTION_NUMPROCESSES,
    PYTEST_PLUGIN_XDIST,
    PYTEST_WORKERINPUT,
    PYTEST_XDIST_WORKER,
    TDX_HARVESTER_FOLDER_ARG,
    TDX_HARVESTER_FOLDER_DEF,
    TDX_HARVESTER_FOLDER_ENV,
    TDX_HARVESTER_FOLDER_OPT,
    TestField,
    TestFilePrefix,
    TestPhase,
    TestStatus,
)
from tabsdatax.pytest.plugins.harvester.internal.helpers import (
    filepath,
    find_current_path,
    handle_call,
    handle_setup,
    handle_teardown,
    handle_unknown,
    write,
)

g_harvester_folder: str | None = None
g_is_xdist_controller: bool = False


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("tdx-harvester", "Test execution monitoring")
    group.addoption(
        TDX_HARVESTER_FOLDER_OPT,
        dest=TDX_HARVESTER_FOLDER_ARG,
        default=None,
        help="Folder where per-test harvester files are written. "
        "Falls back to ${TDX_HARVESTER_FOLDER_ENV}, "
        "then $HOME/.tabsdatax/pytest/harvester.",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Sets the module-level ``harvester_folder`` and ``is_xdist_controller``
    state used by subsequent hooks. On the controller (or in non-xdist mode)
    the directory is wiped and recreated; workers only ensure it exists.
    """
    global g_harvester_folder, g_is_xdist_controller

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
    os.makedirs(
        harvester_folder,
        exist_ok=True,
    )

    g_harvester_folder = harvester_folder
    config._tdx_harvester_folder = harvester_folder


# noinspection PyUnusedLocal
def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Write total collected test count to the harvester directory."""
    if not g_harvester_folder or not items:
        return
    meta_path = os.path.join(g_harvester_folder, "_meta.json")
    write(meta_path, {"collected": len(items)})


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logstart(nodeid: str, location: tuple) -> None:
    """Create a ``RUNNING`` JSON file when a test is about to execute.
    Skipped on the xdist controller to avoid orphan files — under xdist this
    hook fires on the controller for every dispatched test, but only workers
    actually run them.
    """
    if g_is_xdist_controller:
        return

    if not g_harvester_folder:
        return

    path = filepath(g_harvester_folder, TestFilePrefix.RUNNING, nodeid)
    worker = os.environ.get(PYTEST_XDIST_WORKER, "")
    write(
        path,
        {
            TestField.NODEID: nodeid,
            TestField.FILE: location[0],
            TestField.LINE: location[1],
            TestField.FUNCTION: location[2],
            TestField.STATUS: TestStatus.RUNNING,
            TestField.START: time.time(),
            TestField.WORKER: worker,
            TestField.PID: os.getpid(),
        },
        must_be_new=True,
    )


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    """Update the test's JSON file after each phase (setup / call / teardown).
    Called up to three times per test item. Each invocation records the phase
    duration, captures any error or skip information, and renames the file
    from its current prefix to the outcome prefix (see **TestFilePrefix**).
    """
    report = yield

    nodeid = item.nodeid

    if not g_harvester_folder:
        return report

    data_path = find_current_path(g_harvester_folder, nodeid)
    if not data_path:
        return report

    try:
        with open(data_path) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        data = {
            TestField.NODEID: nodeid,
            TestField.STATUS: TestStatus.RUNNING,
            TestField.START: call.start,
        }

    data[PHASE_DURATION_FIELD[call.when]] = call.duration

    match call.when:
        case TestPhase.SETUP:
            handle_setup(
                g_harvester_folder,
                data_path,
                data,
                nodeid,
                call,
                report,
            )
        case TestPhase.CALL:
            handle_call(
                g_harvester_folder,
                data_path,
                data,
                nodeid,
                call,
                report,
            )
        case TestPhase.TEARDOWN:
            handle_teardown(
                g_harvester_folder,
                data_path,
                data,
                nodeid,
                call,
                report,
            )
        case _:
            handle_unknown(
                g_harvester_folder,
                data_path,
                data,
                nodeid,
                call,
                report,
            )

    return report

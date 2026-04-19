#
#  Copyright 2026 Tabs Data Inc.
#

import os
from enum import StrEnum

PYTEST_WORKERINPUT = "workerinput"
PYTEST_PLUGIN_XDIST = "xdist"
PYTEST_OPTION_NUMPROCESSES = "numprocesses"
PYTEST_ATTRIBUTE_REPRCRASH = "reprcrash"

TDX_HARVESTER_FOLDER_OPT = "--tdx-harvester-folder"
TDX_HARVESTER_FOLDER_ARG = "tdx_harvester_folder"
TDX_HARVESTER_FOLDER_ENV = "TDX_HARVESTER_FOLDER"
TDX_HARVESTER_FOLDER_DEF = os.path.join(
    os.path.expanduser("~"),
    ".tabsdatax",
    "pytest",
    "harvester",
)


PYTEST_XDIST_WORKER = "PYTEST_XDIST_WORKER"

SUDO_REFRESH_INTERVAL_SECONDS = 4 * 60
SUDO_REFRESH_TIMEOUT_SECONDS = 5

MONITOR_REFRESH_INTERVAL_SECONDS = 1


class TestFilePrefix(StrEnum):
    RUNNING = "1."
    PASSED = "0.0."
    ERROR_SETUP = "0.1."
    ERROR_CALL = "0.2."
    ERROR_TEARDOWN = "0.3."
    UNKNOWN = "0.4."
    SKIPPED = "2."


class TestField(StrEnum):
    NODEID = "nodeid"
    FILE = "file"
    LINE = "line"
    FUNCTION = "function"
    STATUS = "status"
    START = "start"
    STOP = "stop"
    WORKER = "worker"
    PID = "pid"
    PHASE = "phase"
    ERROR = "error"
    ERROR_TYPE = "error_type"
    TRACEBACK = "traceback"
    ERROR_LOCATION = "error_location"
    SKIP_REASON = "skip_reason"
    DURATION_SETUP = "duration_setup"
    DURATION_CALL = "duration_call"
    DURATION_TEARDOWN = "duration_teardown"
    TEARDOWN_ERROR = "teardown_error"
    TEARDOWN_ERROR_TYPE = "teardown_error_type"
    TEARDOWN_TRACEBACK = "teardown_traceback"
    TEARDOWN_ERROR_LOCATION = "teardown_error_location"


class TestStatus(StrEnum):
    RUNNING = "Running"
    PASSED = "Passed"
    SKIPPED = "Skipped"
    FAILED_SETUP = "Failed (Setup)"
    FAILED_EXECUTION = "Failed (Execution)"
    FAILED_TEARDOWN = "Failed (Teardown)"
    UNKNOWN = "Unknown"


class TestPhase(StrEnum):
    SETUP = "setup"
    CALL = "call"
    TEARDOWN = "teardown"


PHASE_DURATION_FIELD = {
    TestPhase.SETUP: TestField.DURATION_SETUP,
    TestPhase.CALL: TestField.DURATION_CALL,
    TestPhase.TEARDOWN: TestField.DURATION_TEARDOWN,
}

PREFIXES_FAILED = (
    TestFilePrefix.ERROR_SETUP,
    TestFilePrefix.ERROR_CALL,
    TestFilePrefix.ERROR_TEARDOWN,
    TestFilePrefix.UNKNOWN,
)

PREFIX_TO_COUNTER = {
    TestFilePrefix.ERROR_SETUP: "error_setup",
    TestFilePrefix.ERROR_CALL: "error_call",
    TestFilePrefix.ERROR_TEARDOWN: "error_teardown",
    TestFilePrefix.UNKNOWN: "error_unknown",
}

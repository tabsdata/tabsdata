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

META_DEBOUNCE_MS = 100


# ---------------------------------------------------------------------------
# Session phases (sequential)
# ---------------------------------------------------------------------------
class SessionPhase(StrEnum):
    CONFIGURE = "configure"
    SESSION_START = "session_start"
    COLLECTION = "collection"
    RUN = "run"
    SESSION_FINISH = "session_finish"
    UNCONFIGURE = "unconfigure"


SESSION_PHASES_ORDERED = list(SessionPhase)


# ---------------------------------------------------------------------------
# Fixture lifecycle
# ---------------------------------------------------------------------------
class FixtureLifecycle(StrEnum):
    PENDING = "pending"
    SETUP = "setup"
    ACTIVE = "active"
    TEARDOWN = "teardown"
    DONE = "done"


FIXTURE_SCOPE_ORDER = {
    "session": 0,
    "package": 1,
    "module": 2,
    "class": 3,
    "function": 4,
}

# ---------------------------------------------------------------------------
# Duration distribution buckets (log-scale, 50 bins from 1ms to 24h)
# ---------------------------------------------------------------------------
import math

NUM_DURATION_BUCKETS = 50
_LOG_MIN = math.log10(0.001)     # 1ms
_LOG_MAX = math.log10(86400.0)   # 24 hours
_LOG_RANGE = _LOG_MAX - _LOG_MIN


def duration_to_bucket(seconds: float) -> int:
    """Map a duration to a bucket index (0..NUM_DURATION_BUCKETS-1)."""
    if seconds <= 0.001:
        return 0
    if seconds >= 86400.0:
        return NUM_DURATION_BUCKETS - 1
    log_val = math.log10(seconds)
    idx = int((log_val - _LOG_MIN) / _LOG_RANGE * NUM_DURATION_BUCKETS)
    return max(0, min(NUM_DURATION_BUCKETS - 1, idx))


# ---------------------------------------------------------------------------
# Brand colors (from tabsdata-ui base.css)
# ---------------------------------------------------------------------------
class Color:
    # Background
    BG = "#282838"

    # Section title bar — blue accent
    TITLE_BG = "#2e5090"
    TITLE_FG = "#e0e8f0"

    # Status colors
    STATUS_DONE = "#80cc80"    # soft green
    STATUS_RUNNING = "#80b8e8" # soft blue
    STATUS_ERROR = "#e88080"   # soft red
    STATUS_PENDING = "#d0c070" # soft gold

    # Text colors
    CYAN = "#80b8e8"
    GREEN = "#80cc80"
    YELLOW = "#d0c070"
    RED = "#e88080"
    MAGENTA = "#c090d0"
    WHITE = "#e0e0e0"
    DIM = "#808080"

    # Per-table text colors — each table has its own hue
    TABLE_PHASES = "#c0a8e0"       # soft purple
    TABLE_FIXTURES = "#f8d088"     # brighter warm orange
    TABLE_MARKERS = "#e0d870"      # yellow
    TABLE_PLUGINS = "#90c8f8"      # blue
    TABLE_RUNNING = "#80cc80"      # green (was Phases)
    TABLE_FAILED = "#f0a0a0"       # soft red

    # Per-table header colors — same hue but more saturated/vivid
    HDR_PHASES = "#8060c0"         # vivid saturated purple
    HDR_FIXTURES = "#f0a830"       # vivid deep orange
    HDR_MARKERS = "#fff090"        # bright yellow
    HDR_PLUGINS = "#60a0e0"        # vivid saturated blue
    HDR_RUNNING = "#50b050"        # vivid green
    HDR_FAILED = "#ff8080"         # vivid saturated red

    # Progress bar colors — intensified version of corresponding table text
    BAR_TESTS = "#60e060"          # bright green (intensified Running/Tests)
    BAR_MARKERS = "#f0e040"        # bright yellow (intensified Markers)
    BAR_FIXTURES = "#f0b040"       # bright orange (intensified Fixtures)


# ---------------------------------------------------------------------------
# Status emojis
# ---------------------------------------------------------------------------
class Emoji:
    PENDING = "\u23f3"       # ⏳
    ACTIVE = "\U0001f504"    # 🔄
    DONE = "\u2705"          # ✅
    SETUP = "\U0001f527"     # 🔧
    TEARDOWN = "\U0001f9f9"  # 🧹
    FAILED = "\u274c"        # ❌
    SKIPPED = "\u23ed\ufe0f" # ⏭️
    SESSION = "\U0001f3e0"   # 🏠 (house — session scope)
    MODULE = "\U0001f9e9"    # 🧩 (puzzle — module scope)
    FUNCTION = "\u26a1"      # ⚡ (zap — function scope)
    PASSED = "\u2705"        # ✅
    RUNNING = "\U0001f3c3"   # 🏃

    # State indicator: single character ⬤ (U+2B24) colored via Rich styles.
    # Same character everywhere = guaranteed consistent width.
    CIRCLE = "\u2b24"             # ⬤ BLACK LARGE CIRCLE
    CIRCLE_BLUE_STYLE = "#6088cc"
    CIRCLE_ORANGE_STYLE = "#e0a030"
    CIRCLE_GREEN_STYLE = "#50b850"
    CIRCLE_RED_STYLE = "#e05050"
    CIRCLE_PURPLE_STYLE = "#9060d0"


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

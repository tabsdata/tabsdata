#
#  Copyright 2026 Tabs Data Inc.
#

import hashlib
import json
import os
import traceback
import warnings

import pytest
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed

from tabsdatax.pytest.plugins.harvester.internal.constants import (
    PYTEST_ATTRIBUTE_REPRCRASH,
    TestField,
    TestFilePrefix,
    TestPhase,
    TestStatus,
)

fs_retry = retry(
    retry=retry_if_exception_type(OSError),
    stop=stop_after_delay(8),
    wait=wait_fixed(0.5),
    reraise=True,
)


def sanitize(nodeid: str) -> str:
    base = nodeid.replace("/", "_").replace("::", "__").replace(" ", "_")
    # Append a short hash to avoid collisions on case-insensitive filesystems
    # (macOS APFS). Tests like [False-False] and [false-False] would otherwise
    # map to the same file.
    digest = hashlib.sha1(nodeid.encode()).hexdigest()[:8]
    return f"{base}.{digest}"


def filepath(
    harvester_folder: str,
    prefix: str,
    nodeid: str,
) -> str:
    return os.path.join(harvester_folder, prefix + sanitize(nodeid) + ".json")


@fs_retry
def atomic_write(tmp: str, path: str, data: dict) -> None:
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def write(
    path: str,
    data: dict,
    *,
    must_be_new: bool = False,
) -> None:
    if must_be_new and os.path.exists(path):
        warnings.warn(
            f"Harvester: file already exists (possible collision): {path}",
            stacklevel=2,
        )
    try:
        atomic_write(path + ".tmp", path, data)
    except OSError as e:
        warnings.warn(
            f"Harvester: failed to write {path}: {e}",
            stacklevel=2,
        )


@fs_retry
def atomic_rename(old_path: str, new_path: str) -> None:
    os.replace(old_path, new_path)


def rename(
    harvester_folder: str,
    old_prefix: str,
    new_prefix: str,
    nodeid: str,
) -> None:
    old_path = filepath(harvester_folder, old_prefix, nodeid)
    new_path = filepath(harvester_folder, new_prefix, nodeid)
    if not os.path.exists(old_path):
        warnings.warn(
            f"Harvester: source file missing for rename: {old_path}",
            stacklevel=2,
        )
        return
    if os.path.exists(new_path):
        warnings.warn(
            f"Harvester: destination already exists (possible collision): {new_path}",
            stacklevel=2,
        )
    try:
        atomic_rename(old_path, new_path)
    except OSError as e:
        warnings.warn(
            f"Harvester: failed to rename {old_path} -> {new_path}: {e}",
            stacklevel=2,
        )


def write_and_rename(
    harvester_folder: str,
    data_path: str,
    data: dict,
    nodeid: str,
    old_prefix: TestFilePrefix,
    new_prefix: TestFilePrefix,
) -> None:
    write(data_path, data)
    rename(harvester_folder, old_prefix, new_prefix, nodeid)


def find_current_path(harvester_folder: str, nodeid: str) -> str | None:
    for prefix in TestFilePrefix:
        path = filepath(harvester_folder, prefix, nodeid)
        if os.path.exists(path):
            return path
    return None


def format_traceback(excinfo) -> str:
    # noinspection PyBroadException
    try:
        return "".join(
            traceback.format_exception(
                type(excinfo.value),
                excinfo.value,
                excinfo.tb,
            )
        )
    except Exception:
        return str(excinfo.value)


def error_location(report: pytest.TestReport) -> str | None:
    longrepr = report.longrepr
    if longrepr is None:
        return None
    reprcrash = getattr(longrepr, PYTEST_ATTRIBUTE_REPRCRASH, None)
    if reprcrash is not None:
        # noinspection PyUnresolvedReferences
        return f"{reprcrash.path}:{reprcrash.lineno}"
    return None


def record_skip(
    data: dict,
    phase: TestPhase,
    call: pytest.CallInfo,
) -> None:
    data[TestField.STATUS] = TestStatus.SKIPPED
    data[TestField.PHASE] = phase
    data[TestField.STOP] = call.stop
    if call.excinfo is not None:
        data[TestField.SKIP_REASON] = str(call.excinfo.value)


def record_error(
    data: dict,
    status: TestStatus,
    phase: TestPhase,
    stop: float,
    excinfo: pytest.ExceptionInfo[BaseException],
    report: pytest.TestReport,
) -> None:
    data[TestField.STATUS] = status
    data[TestField.PHASE] = phase
    data[TestField.STOP] = stop
    data[TestField.ERROR] = str(excinfo.value)
    data[TestField.ERROR_TYPE] = excinfo.typename
    data[TestField.TRACEBACK] = format_traceback(excinfo)
    data[TestField.ERROR_LOCATION] = error_location(report)


def handle_setup(
    harvester_folder: str,
    data_path: str,
    data: dict,
    nodeid: str,
    call: pytest.CallInfo,
    report: pytest.TestReport,
) -> None:
    if report.skipped:
        record_skip(data, TestPhase.SETUP, call)
        write_and_rename(
            harvester_folder,
            data_path,
            data,
            nodeid,
            TestFilePrefix.RUNNING,
            TestFilePrefix.SKIPPED,
        )
    elif call.excinfo is not None:
        record_error(
            data,
            TestStatus.FAILED_SETUP,
            TestPhase.SETUP,
            call.stop,
            call.excinfo,
            report,
        )
        write_and_rename(
            harvester_folder,
            data_path,
            data,
            nodeid,
            TestFilePrefix.RUNNING,
            TestFilePrefix.ERROR_SETUP,
        )
    else:
        write(data_path, data)


def handle_call(
    harvester_folder: str,
    data_path: str,
    data: dict,
    nodeid: str,
    call: pytest.CallInfo,
    report: pytest.TestReport,
) -> None:
    data[TestField.STOP] = call.stop
    if report.skipped:
        record_skip(data, TestPhase.CALL, call)
        write_and_rename(
            harvester_folder,
            data_path,
            data,
            nodeid,
            TestFilePrefix.RUNNING,
            TestFilePrefix.SKIPPED,
        )
    elif call.excinfo is None:
        data[TestField.STATUS] = TestStatus.PASSED
        write_and_rename(
            harvester_folder,
            data_path,
            data,
            nodeid,
            TestFilePrefix.RUNNING,
            TestFilePrefix.PASSED,
        )
    else:
        record_error(
            data,
            TestStatus.FAILED_EXECUTION,
            TestPhase.CALL,
            call.stop,
            call.excinfo,
            report,
        )
        write_and_rename(
            harvester_folder,
            data_path,
            data,
            nodeid,
            TestFilePrefix.RUNNING,
            TestFilePrefix.ERROR_CALL,
        )


def handle_teardown(
    harvester_folder: str,
    data_path: str,
    data: dict,
    nodeid: str,
    call: pytest.CallInfo,
    report: pytest.TestReport,
) -> None:
    data.setdefault(TestField.STOP, call.stop)
    if call.excinfo is not None:
        data[TestField.TEARDOWN_ERROR] = str(call.excinfo.value)
        data[TestField.TEARDOWN_ERROR_TYPE] = call.excinfo.typename
        data[TestField.TEARDOWN_TRACEBACK] = format_traceback(call.excinfo)
        data[TestField.TEARDOWN_ERROR_LOCATION] = error_location(report)
        if data.get(TestField.STATUS) == TestStatus.PASSED:
            data[TestField.STATUS] = TestStatus.FAILED_TEARDOWN
            data[TestField.PHASE] = TestPhase.TEARDOWN
            write_and_rename(
                harvester_folder,
                data_path,
                data,
                nodeid,
                TestFilePrefix.PASSED,
                TestFilePrefix.ERROR_TEARDOWN,
            )
        else:
            write(data_path, data)
    else:
        write(data_path, data)


def handle_unknown(
    harvester_folder: str,
    data_path: str,
    data: dict,
    nodeid: str,
    call: pytest.CallInfo,
    report: pytest.TestReport,
) -> None:
    data.setdefault(TestField.STOP, call.stop)
    data[TestField.STATUS] = TestStatus.UNKNOWN
    data[TestField.PHASE] = call.when
    write_and_rename(
        harvester_folder,
        data_path,
        data,
        nodeid,
        TestFilePrefix.RUNNING,
        TestFilePrefix.UNKNOWN,
    )

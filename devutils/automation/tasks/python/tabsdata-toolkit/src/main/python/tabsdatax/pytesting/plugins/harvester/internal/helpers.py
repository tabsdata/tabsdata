#
#  Copyright 2026 Tabsdata Inc.
#

import json
import os
import traceback

import pytest
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed

from tabsdatax.pytesting.plugins.harvester.internal.constants import (
    PYTEST_ATTRIBUTE_REPRCRASH,
    TestPhase,
    TestStatus,
)
from tabsdatax.pytesting.plugins.harvester.internal.database import (
    promote_passed_to_teardown_failure,
    record_teardown_error_fields,
    update_test_record,
    upsert_test_final,
)

fs_retry = retry(
    retry=retry_if_exception_type(OSError),
    stop=stop_after_delay(8),
    wait=wait_fixed(0.5),
    reraise=True,
)


def normalize_nodeid(nodeid: str) -> str:
    if nodeid.rfind("@") > nodeid.rfind("]"):
        return nodeid.rsplit("@", 1)[0]
    return nodeid


@fs_retry
def atomic_write(tmp: str, path: str, data: dict) -> None:
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


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


def skip_fields(phase: TestPhase, call: pytest.CallInfo) -> dict:
    fields = {
        "phase": str(phase),
        "stop": call.stop,
    }
    if call.excinfo is not None:
        fields["skip_reason"] = str(call.excinfo.value)
    return fields


def error_fields(
    phase: TestPhase,
    stop: float,
    excinfo: pytest.ExceptionInfo[BaseException],
    report: pytest.TestReport,
) -> dict:
    return {
        "phase": str(phase),
        "stop": stop,
        "error": str(excinfo.value),
        "error_type": excinfo.typename,
        "traceback": format_traceback(excinfo),
        "error_location": error_location(report),
    }


def handle_setup(
    harvester_folder: str,
    nodeid: str,
    call: pytest.CallInfo,
    report: pytest.TestReport,
) -> None:
    duration_setup = call.duration
    if report.skipped:
        fields = skip_fields(TestPhase.SETUP, call)
        fields["duration_setup"] = duration_setup
        upsert_test_final(
            harvester_folder,
            nodeid,
            str(TestStatus.SKIPPED),
            **fields,
        )
    elif call.excinfo is not None:
        fields = error_fields(TestPhase.SETUP, call.stop, call.excinfo, report)
        fields["duration_setup"] = duration_setup
        upsert_test_final(
            harvester_folder,
            nodeid,
            str(TestStatus.FAILED_SETUP),
            **fields,
        )
    else:
        update_test_record(
            harvester_folder,
            nodeid,
            duration_setup=duration_setup,
        )


def handle_call(
    harvester_folder: str,
    nodeid: str,
    call: pytest.CallInfo,
    report: pytest.TestReport,
) -> None:
    duration_call = call.duration
    if report.skipped:
        fields = skip_fields(TestPhase.CALL, call)
        fields["duration_call"] = duration_call
        upsert_test_final(
            harvester_folder,
            nodeid,
            str(TestStatus.SKIPPED),
            **fields,
        )
    elif call.excinfo is None:
        upsert_test_final(
            harvester_folder,
            nodeid,
            str(TestStatus.PASSED),
            stop=call.stop,
            duration_call=duration_call,
        )
    else:
        fields = error_fields(TestPhase.CALL, call.stop, call.excinfo, report)
        fields["duration_call"] = duration_call
        upsert_test_final(
            harvester_folder,
            nodeid,
            str(TestStatus.FAILED_EXECUTION),
            **fields,
        )


def handle_teardown(
    harvester_folder: str,
    nodeid: str,
    call: pytest.CallInfo,
    report: pytest.TestReport,
) -> None:
    duration_teardown = call.duration
    if call.excinfo is None:
        update_test_record(
            harvester_folder,
            nodeid,
            stop=call.stop,
            duration_teardown=duration_teardown,
        )
        return

    teardown_error = str(call.excinfo.value)
    teardown_error_type = call.excinfo.typename
    teardown_traceback = format_traceback(call.excinfo)
    teardown_error_location = error_location(report)

    promoted = promote_passed_to_teardown_failure(
        harvester_folder,
        nodeid,
        teardown_error=teardown_error,
        teardown_error_type=teardown_error_type,
        teardown_traceback=teardown_traceback,
        teardown_error_location=teardown_error_location,
        failed_teardown_status=str(TestStatus.FAILED_TEARDOWN),
        teardown_phase=str(TestPhase.TEARDOWN),
        stop=call.stop,
        duration_teardown=duration_teardown,
    )
    if not promoted:
        record_teardown_error_fields(
            harvester_folder,
            nodeid,
            teardown_error=teardown_error,
            teardown_error_type=teardown_error_type,
            teardown_traceback=teardown_traceback,
            teardown_error_location=teardown_error_location,
            stop=call.stop,
            duration_teardown=duration_teardown,
        )


def handle_unknown(
    harvester_folder: str,
    nodeid: str,
    call: pytest.CallInfo,
    report: pytest.TestReport,
) -> None:
    upsert_test_final(
        harvester_folder,
        nodeid,
        str(TestStatus.UNKNOWN),
        phase=str(call.when),
        stop=call.stop,
    )

#
# Copyright 2026 Tabsdata Inc.
#

from enum import Enum


class FunctionRunStatus(Enum):
    COMMITTED = "C"
    DONE = "D"
    ERROR = "E"
    FAILED = "F"
    ON_HOLD = "H"
    RUNNING = "R"
    RUN_REQUESTED = "RR"
    RESCHEDULED = "RS"
    SCHEDULED = "S"
    CANCELED = "X"
    YANKED = "Y"


FUNCTION_RUN_STATUS_MAPPING = {
    FunctionRunStatus.COMMITTED.value: "Committed",
    FunctionRunStatus.DONE.value: "Done",
    FunctionRunStatus.ERROR.value: "Error",
    FunctionRunStatus.FAILED.value: "Failed",
    FunctionRunStatus.ON_HOLD.value: "On Hold",
    FunctionRunStatus.RUNNING.value: "Running",
    FunctionRunStatus.RUN_REQUESTED.value: "Run Requested",
    FunctionRunStatus.RESCHEDULED.value: "Rescheduled",
    FunctionRunStatus.SCHEDULED.value: "Scheduled",
    FunctionRunStatus.CANCELED.value: "Canceled",
    FunctionRunStatus.YANKED.value: "Yanked",
}


class PhysicalFunctionRunStatus(Enum):
    REQUESTED = "RR"
    DISPATCHED = "DP"
    RUNNING = "R"
    DONE = "D"
    ERROR = "E"
    FAILED = "F"


PHYSICAL_FUNCTION_RUN_ENDED_STATUSES = frozenset({
    PhysicalFunctionRunStatus.DONE.value,
    PhysicalFunctionRunStatus.ERROR.value,
    PhysicalFunctionRunStatus.FAILED.value,
})


def function_run_status_to_mapping(status: str) -> str:
    """
    Function to convert a status to a mapping. While currently it
    only accesses the dictionary and returns the corresponding value, it could get
    more difficult in the future.
    """
    return FUNCTION_RUN_STATUS_MAPPING.get(status, status)


FUNCTION_RUN_VALID_USER_PROVIDED_STATUSES = ", ".join([
    f"{long}/{short}" for short, long in FUNCTION_RUN_STATUS_MAPPING.items()
])


def user_function_run_status_to_api(
    user_provided_status: str | None,
) -> str | None:
    """
    Convert a user-provided status string to the API status string.
    :param user_provided_status: The user-provided status string.
    :return: The API status string.
    """
    if not user_provided_status:
        return None

    user_provided_status = user_provided_status.lower()
    for key, value in FUNCTION_RUN_STATUS_MAPPING.items():
        if user_provided_status == key.lower() or user_provided_status == value.lower():
            return key

    raise ValueError(
        f"Invalid status: '{user_provided_status}'. "
        "Valid statuses are: "
        f"{FUNCTION_RUN_VALID_USER_PROVIDED_STATUSES}. Statuses are case-insensitive."
    )
